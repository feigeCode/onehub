use std::any::Any;

use anyhow::Error;
use gpui::{div, px, AnyElement, App, AppContext, Context, ElementId, Entity, FontWeight, InteractiveElement, IntoElement, ParentElement, Render, SharedString, StatefulInteractiveElement, Styled, Window};
use gpui::prelude::FluentBuilder;
use gpui_component::{button::{Button, ButtonVariants as _}, h_flex, input::{Input, InputEvent, InputState}, menu::PopupMenuItem, v_flex, ActiveTheme, Disableable, Icon, IconName, InteractiveElementExt, Sizable, Size, ThemeMode, WindowExt};

use one_core::storage::{ConnectionRepository, ConnectionType, DatabaseType, DbConnectionConfig, GlobalStorageState, StoredConnection, Workspace, WorkspaceRepository};
use one_core::storage::traits::Repository;
use one_core::tab_container::{TabContainer, TabContent, TabContentType, TabItem};
use one_core::themes::SwitchThemeMode;
use db_view::ai_chat_panel::AiChatPanel;
use db_view::database_tab::DatabaseTabContent;
use db_view::db_connection_form::{DbConnectionForm, DbConnectionFormEvent, DbFormConfig};
use gpui_component::button::{ButtonCustomVariant, ButtonVariant};
use gpui_component::menu::DropdownMenu;
use one_core::gpui_tokio::Tokio;

use crate::setting_tab::SettingsTabContent;



// HomePage Entity - 管理 home 页面的所有状态
pub struct HomePage {
    selected_filter: ConnectionType,
    workspaces: Vec<Workspace>,
    connections: Vec<StoredConnection>,
    _selected_workspace_id: Option<i64>,
    tab_container: Entity<TabContainer>,
    search_input: Entity<InputState>,
    search_query: Entity<String>,
    editing_connection_id: Option<i64>,
    selected_connection_id: Option<i64>,
    editing_workspace_id: Option<i64>,
    ai_chat_panel: Entity<AiChatPanel>,
    ai_panel_visible: bool,
}

impl HomePage {
    pub fn new(tab_container: Entity<TabContainer>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_query = cx.new(|_| String::new());
        let search_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder("搜索连接...")
        });

        // 订阅搜索输入变化
        let query_clone = search_query.clone();
        cx.subscribe_in(&search_input, window, move |_this, _input, event, _window, cx| {
            if let InputEvent::Change = event {
                query_clone.update(cx, |q, cx| {
                    *q = _input.read(cx).text().to_string();
                    cx.notify();
                });
                cx.notify();
            }
        })
        .detach();

        // 创建 AI 聊天面板
        let ai_chat_panel = cx.new(|cx| AiChatPanel::new(window, cx));

        let mut page = Self {
            selected_filter: ConnectionType::All,
            workspaces: Vec::new(),
            connections: Vec::new(),
            _selected_workspace_id: None,
            tab_container,
            search_input,
            search_query,
            editing_connection_id: None,
            selected_connection_id: None,
            editing_workspace_id: None,
            ai_chat_panel,
            ai_panel_visible: false,
        };

        // 异步加载工作区和连接列表
        page.load_workspaces(cx);
        page.load_connections(cx);
        page
    }

    fn toggle_ai_panel(&mut self, cx: &mut Context<Self>) {
        self.ai_panel_visible = !self.ai_panel_visible;
        cx.notify();
    }

    fn load_workspaces(&mut self, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();

        let task = Tokio::spawn(cx, async move {
            let repo = storage.get::<WorkspaceRepository>().await
                .ok_or_else(|| anyhow::anyhow!("WorkspaceRepository not found"))?;
            let pool = storage.get_pool().await?;
            let result: anyhow::Result<Vec<Workspace>> = repo.list(&pool).await;
            result
        });

        cx.spawn(async move |this, cx| {
            let task_result = task.await;
            match task_result {
                Ok(result) => match result {
                    Ok(workspaces) => {
                        _ = this.update(cx, |this, cx| {
                            this.workspaces = workspaces;
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to load workspaces: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    fn load_connections(&mut self, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();

        let task = Tokio::spawn(cx, async move {
            let repo = storage.get::<ConnectionRepository>().await
                .ok_or_else(|| anyhow::anyhow!("ConnectionRepository not found"))?;
            let pool = storage.get_pool().await?;
            let result: anyhow::Result<Vec<StoredConnection>> = repo.list(&pool).await;
            result
        });

        cx.spawn(async move |this, cx| {
            let task_result = task.await;
            match task_result {
                Ok(result) => match result {
                    Ok(connections) => {
                        _ = this.update(cx, |this, cx| {
                            this.connections = connections;
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to load connections: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    fn show_workspace_form(&mut self, workspace_id: Option<i64>, window: &mut Window, cx: &mut Context<Self>) {
        let workspace_data = workspace_id.and_then(|id| {
            self.workspaces.iter().find(|w| w.id == Some(id)).cloned()
        });
        self.editing_workspace_id = workspace_id;
        let view = cx.entity().clone();
        let is_editing = workspace_id.is_some();
        let form = cx.new(|cx| {
            let mut input_state = InputState::new(window, cx).placeholder("工作区名称");
            if let Some(ref workspace) = workspace_data {
                input_state.set_value(workspace.name.clone(), window, cx);
            }
            input_state
        });


        window.open_dialog(cx, move |dialog, _window, _cx| {
            let form_clone = form.clone();
            let view_clone = view.clone();
            let view_clone2 = view.clone();
            dialog
                .title(if is_editing { "编辑工作区" } else { "新建工作区" })
                .w(px(400.0))
                .child(
                    Input::new(&form).size_full()
                )
                .confirm()
                .on_ok(move |_, _window, cx| {
                    let name = form_clone.read(cx).text().to_string();
                    if !name.is_empty() {
                        let _ = view_clone.update(cx, |this, cx| {
                            this.handle_save_workspace(name, cx);
                        });
                        true
                    } else {
                        false
                    }
                })
                .on_cancel(move |_, _, cx| {
                    let _ = view_clone2.update(cx, |this, _| {
                        this.editing_workspace_id = None;
                    });
                    true
                })
        });
    }
    
    fn handle_save_workspace(&mut self, name: String, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();
        let editing_id = self.editing_workspace_id;
        
        let mut workspace = if let Some(id) = editing_id {
            // 编辑模式：从现有工作区更新
            let mut ws = self.workspaces.iter()
                .find(|w| w.id == Some(id))
                .cloned()
                .unwrap_or_else(|| Workspace::new(name.clone()));
            ws.name = name;
            ws
        } else {
            // 新建模式
            Workspace::new(name)
        };
        
        let task = Tokio::spawn(cx, async move {
            let repo = storage.get::<WorkspaceRepository>().await
                .ok_or_else(|| anyhow::anyhow!("WorkspaceRepository not found"))?;
            let pool = storage.get_pool().await?;
            
            if editing_id.is_some() {
                repo.update(&pool, &mut workspace).await?;
            } else {
                repo.insert(&pool, &mut workspace).await?;
            }
            
            let result: anyhow::Result<Workspace> = Ok(workspace);
            result
        });
        
        cx.spawn(async move |this, cx| {
            let task_result = task.await;
            match task_result {
                Ok(result) => match result {
                    Ok(workspace) => {
                        _ = this.update(cx, |this, cx| {
                            if let Some(editing_id) = editing_id {
                                if let Some(pos) = this.workspaces.iter().position(|w| w.id == Some(editing_id)) {
                                    this.workspaces[pos] = workspace;
                                }
                            } else {
                                this.workspaces.push(workspace);
                            }
                            this.editing_workspace_id = None;
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to save workspace: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    fn show_connection_form(&mut self, db_type: DatabaseType, window: &mut Window, cx: &mut Context<Self>) {
        let config = match db_type {
            DatabaseType::MySQL => DbFormConfig::mysql(),
            DatabaseType::PostgreSQL => DbFormConfig::postgres(),
            _ => {
                panic!("Unsupported database type");
            }
        };

        let form = cx.new(|cx| {
            DbConnectionForm::new(config, window, cx)
        });

        // 设置工作区列表
        form.update(cx, |f, cx| {
            f.set_workspaces(self.workspaces.clone(), window, cx);
        });

        // 如果是编辑模式，加载现有连接数据
        if let Some(editing_id) = self.editing_connection_id {
            if let Some(conn) = self.connections.iter().find(|c| c.id == Some(editing_id)) {
                form.update(cx, |f, cx| {
                    f.load_connection(conn, window, cx);
                });
            }
        }

        let is_editing = self.editing_connection_id.is_some();
        let title = if is_editing {
            format!("编辑 {} 连接", db_type.as_str())
        } else {
            format!("新建 {} 连接", db_type.as_str())
        };
        
        let form_clone = form.clone();
        let view = cx.entity().clone();

        // 订阅表单事件
        cx.subscribe_in(&form, window, move |this, form, event, window, cx| {
            match event {
                DbConnectionFormEvent::TestConnection(db_type, config) => {
                    this.handle_test_connection(form.clone(), *db_type, config.clone(), window, cx);
                }
                DbConnectionFormEvent::Save(db_type, config) => {
                    this.handle_save_connection(*db_type, config.clone(), window, cx);
                    window.close_dialog(cx);
                }
                DbConnectionFormEvent::Cancel => {
                    this.editing_connection_id = None;
                    window.close_dialog(cx);
                    cx.notify();
                }
            }
        }).detach();
        
        let title_shared: SharedString = title.into();
        let view_clone = view.clone();
        let form_for_footer = form.clone();
        let form_for_test = form.clone();
        let form_for_save = form.clone();
        
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let view_for_cancel = view_clone.clone();
            let form_footer = form_for_footer.clone();
            let form_test = form_for_test.clone();
            let form_save = form_for_save.clone();
            
            dialog
                .title(title_shared.clone())
                .w(px(600.0))
                .h(px(550.0))
                .child(form_clone.clone())
                .close_button(true)
                .footer(move |_ok_btn, cancel_btn, window, cx| {
                    let is_testing = form_footer.read(cx).is_testing(cx);
                    let form_t = form_test.clone();
                    let form_s = form_save.clone();
                    
                    vec![
                        cancel_btn(window, cx),
                        Button::new("test")
                            .outline()
                            .label(if is_testing { "测试中..." } else { "测试连接" })
                            .disabled(is_testing)
                            .on_click(window.listener_for(&form_t, |form, _, _, cx| {
                                form.trigger_test_connection(cx);
                            }))
                            .into_any_element(),
                        Button::new("save")
                            .primary()
                            .label("好")
                            .disabled(is_testing)
                            .on_click(window.listener_for(&form_s, |form, _, _, cx| {
                                form.trigger_save(cx);
                            }))
                            .into_any_element(),
                    ]
                })
                .on_cancel(move |_, _, cx| {
                    let _ = view_for_cancel.update(cx, |this, cx| {
                        this.editing_connection_id = None;
                        cx.notify();
                    });
                    true
                })
        });
    }

    fn handle_test_connection(
        &mut self,
        form: Entity<DbConnectionForm>,
        db_type: DatabaseType,
        config: DbConnectionConfig,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let global_state = cx.global::<db::GlobalDbState>().clone();
        cx.spawn(async move |_, cx| {
            let manager = global_state.db_manager;

            // Test connection and collect result
            let test_result = async {
                let db_plugin = manager.get_plugin(&db_type)?;
                let conn = db_plugin.create_connection(config).await?;
                conn.ping().await?;
                Ok::<bool, Error>(true)
            }.await;

            match test_result {
                Ok(_) => {
                    form.update(cx, |form, cx1| {
                        form.set_test_result(Ok(true), cx1)
                    })
                }
                Err(_) => {
                    form.update(cx, |form, cx1| {
                        form.set_test_result(Err("测试连接失败".to_string()), cx1)
                    })
                }
            }
        }).detach();
    }

    fn handle_save_connection(
        &mut self,
        _db_type: DatabaseType,
        config: DbConnectionConfig,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let editing_id = self.editing_connection_id;
        let mut stored = if let Some(id) = editing_id {
            // 编辑模式：从现有连接更新
            let mut conn = self.connections.iter()
                .find(|c| c.id == Some(id))
                .cloned()
                .unwrap_or_else(|| StoredConnection::from_db_connection(config.clone()));
            conn.name = config.name.clone();
            conn.workspace_id = config.workspace_id;
            conn.params = serde_json::to_string(&one_core::storage::DatabaseParams {
                db_type: config.database_type,
                host: config.host.clone(),
                port: config.port,
                username: config.username.clone(),
                password: config.password.clone(),
                database: config.database.clone(),
            }).unwrap();
            conn
        } else {
            // 新建模式
            StoredConnection::from_db_connection(config.clone())
        };

        let storage = cx.global::<GlobalStorageState>().storage.clone();

        let task = Tokio::spawn(cx, async move {
            let repo = storage.get::<ConnectionRepository>().await
                .ok_or_else(|| anyhow::anyhow!("ConnectionRepository not found"))?;
            let pool = storage.get_pool().await?;
            
            if editing_id.is_some() {
                repo.update(&pool, &mut stored).await?;
            } else {
                repo.insert(&pool, &mut stored).await?;
            }
            
            let result: anyhow::Result<StoredConnection> = Ok(stored);
            result
        });

        cx.spawn(async move |this, cx| {
            let task_result = task.await;
            match task_result {
                Ok(result) => match result {
                    Ok(saved_conn) => {
                        _ = this.update(cx, |this, cx| {
                            if let Some(editing_id) = editing_id {
                                if let Some(pos) = this.connections.iter().position(|c| c.id == Some(editing_id)) {
                                    this.connections[pos] = saved_conn;
                                }
                            } else {
                                this.connections.push(saved_conn);
                            }
                            this.editing_connection_id = None;
                            cx.notify();
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to save connection: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    pub fn add_settings_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.tab_container.update(cx, |tc, cx| {
            tc.activate_or_add_tab_lazy("settings", |win, cx| {
                TabItem::new("settings", SettingsTabContent::new(win, cx))
            }, window, cx);
        });
    }

    fn add_item_to_tab(&mut self, conn: &StoredConnection, window: &mut Window, cx: &mut Context<Self>) {
        self.tab_container.update(cx, |tc, cx| {
            let tab_id = format!("database-{}", conn.name);
            tc.activate_or_add_tab_lazy(
                tab_id.clone(),
                {
                    let conn = conn.clone();
                    move |window, cx| {
                        let db_content = DatabaseTabContent::new(vec![conn], window, cx);
                        TabItem::new(tab_id.clone(), db_content)
                    }
                },
                window,
                cx
            )
        });
    }

    fn render_toolbar(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let view = cx.entity();
        let has_selection = self.selected_connection_id.is_some();
        
        h_flex()
            .p_4()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .justify_between()
            .items_center()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("new-connect-button")
                            .icon(IconName::Plus)
                            .label("新建连接")
                            .text_color(cx.theme().primary_foreground)
                            .bg(cx.theme().chart_2)
                            .with_size(Size::Large)
                            .with_variant(ButtonVariant::Custom(ButtonCustomVariant::new(cx).hover(cx.theme().primary)))
                            .dropdown_menu(move |menu, window, _cx| {
                                menu
                                    .item(
                                    PopupMenuItem::new("工作区")
                                                .icon(IconName::Apps)
                                                .on_click(window.listener_for(&view, move |this, _, window, cx| {
                                                    this.show_workspace_form(None, window, cx);
                                                }))
                                ).item(
                                    PopupMenuItem::new("MySQL")
                                        .icon(Icon::from(IconName::MySQLLineColor))
                                        .on_click(window.listener_for(&view, move |this, _, window, cx| {
                                            this.editing_connection_id = None;
                                            this.show_connection_form(DatabaseType::MySQL, window, cx);
                                        }))
                                ).item(
                                    PopupMenuItem::new("PostgreSQL")
                                        .icon(Icon::from(IconName::MySQLLineColor))
                                        .on_click(window.listener_for(&view, move |this, _, window, cx| {
                                            this.editing_connection_id = None;
                                            this.show_connection_form(DatabaseType::PostgreSQL, window, cx);
                                        }))
                                )
                            })
                    )
                    .when(has_selection, |this| {
                        this.child(
                            Button::new("edit-selected")
                                .icon(IconName::Settings)
                                .tooltip("编辑连接")
                                .with_size(Size::Medium)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    if let Some(conn_id) = this.selected_connection_id {
                                        if let Some(conn) = this.connections.iter().find(|c| c.id == Some(conn_id)) {
                                            this.editing_connection_id = Some(conn_id);
                                            if let Ok(params) = conn.to_database_params() {
                                                this.show_connection_form(params.db_type, window, cx);
                                            }
                                        }
                                    }
                                }))
                        )
                    })
            )
            .child(
                h_flex()
                    .gap_2()
                    .w(px(300.0))
                    .child(Input::new(&self.search_input).w_full()
                    .bg(cx.theme().input)
                )
            )
    }

    fn render_sidebar(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let filter_types = vec![
            ConnectionType::All,
            ConnectionType::Database,
            ConnectionType::SshSftp,
            ConnectionType::Redis,
            ConnectionType::MongoDB,
        ];

        v_flex()
            .w(px(200.0))
            .h_full()
            .bg(cx.theme().background)
            .child(
                // 侧边栏过滤选项
                v_flex()
                    .flex_1()
                    .w_full()
                    .p_2()
                    .gap_2()
                    .children(
                        filter_types.into_iter().map(|filter_type| {
                            let is_selected = self.selected_filter == filter_type;
                            let filter_type_clone = filter_type.clone();

                            div()
                                .id(filter_type.label())
                                .flex()
                                .items_center()
                                .gap_2()
                                .w_full()
                                .px_3()
                                .py_2()
                                .cursor_pointer()
                                .when(is_selected, |this| {
                                    this.bg(cx.theme().primary)
                                })
                                .when(!is_selected, |this| {
                                    this.bg(cx.theme().background)
                                        .hover(|style| style.bg(cx.theme().accent))
                                })
                                .on_click(cx.listener(move |this: &mut HomePage, _, _, cx| {
                                    this.selected_filter = filter_type_clone.clone();
                                    cx.notify();
                                }))
                                .child(
                                    Icon::new(filter_type.icon())
                                        .color()
                                        .when(filter_type == ConnectionType::SshSftp  && !is_selected, |this| {
                                            this.text_color(gpui::rgb(0x8b5cf6)).mono()
                                        })
                                        .with_size(Size::Large)
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .when(is_selected, |this| this.text_color(gpui::white()))
                                        .child(filter_type.label())
                                )
                        })
                    )
            )
            .child(
                // 底部区域：主题切换和用户头像
                v_flex()
                    .w_full()
                    .p_4()
                    .gap_3()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        Button::new("theme_toggle")
                            .icon(IconName::Palette)
                            .label("切换主题")
                            .w_full()
                            .justify_start()
                            .on_click(cx.listener(|_this: &mut HomePage, _, window, cx| {
                                // 切换主题模式
                                let current_mode = cx.theme().mode;
                                let new_mode = match current_mode {
                                    ThemeMode::Light => ThemeMode::Dark,
                                    ThemeMode::Dark => ThemeMode::Light,
                                };
                                window.dispatch_action(Box::new(SwitchThemeMode(new_mode)), cx);
                            }))
                    )
                    .child(
                        Button::new("open_settings")
                            .icon(IconName::Settings)
                            .label("设置")
                            .w_full()
                            .justify_start()
                            .on_click(cx.listener(|this: &mut HomePage, _, window, cx| {
                                this.add_settings_tab(window, cx);
                            }))
                    )
            )
    }

    fn match_connection(&self, conn: &StoredConnection, query: &str) -> bool {
        if query.is_empty() {
            return true;
        }
        
        // 匹配连接名称
        if conn.name.to_lowercase().contains(query) {
            return true;
        }
        
        // 匹配连接参数（主机/IP、端口、用户名、数据库名）
        if let Ok(params) = conn.to_database_params() {
            // 主机名或 IP 地址
            if params.host.to_lowercase().contains(query) {
                return true;
            }
            
            // 端口号（转为字符串匹配）
            if params.port.to_string().contains(query) {
                return true;
            }
            
            // 用户名
            if params.username.to_lowercase().contains(query) {
                return true;
            }
            
            // 数据库名
            if params.database.as_ref().map_or(false, |db| db.to_lowercase().contains(query)) {
                return true;
            }
            
            // 完整连接字符串匹配（如 "root@localhost:3306"）
            let conn_str = format!("{}@{}:{}", params.username, params.host, params.port);
            if conn_str.to_lowercase().contains(query) {
                return true;
            }
        }
        
        false
    }

    fn render_content_area(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let search_query = self.search_query.read(cx).to_lowercase();
        
        // 分组：工作区和未分配连接
        let workspaces_with_connections: Vec<_> = self.workspaces.iter()
            .map(|ws| {
                let conn_list: Vec<_> = self.connections.iter()
                    .filter(|conn| conn.workspace_id == ws.id)
                    .filter(|conn| self.match_connection(conn, &search_query))
                    .cloned()
                    .collect();
                (ws.clone(), conn_list)
            })
            .collect();

        let unassigned_connections: Vec<_> = self.connections.iter()
            .filter(|conn| conn.workspace_id.is_none())
            .filter(|conn| self.match_connection(conn, &search_query))
            .cloned()
            .collect();

        let selected_id = self.selected_connection_id;
        div()
            .id("home-content")
            .size_full()
            .overflow_y_scroll()
            .p_6()
            .child({
                let mut container = v_flex()
                    .gap_6()
                    .w_full();
                
                // 工作区列表
                for (workspace, connections) in workspaces_with_connections {
                    container = container.child(
                        self.render_workspace_section(workspace, connections, selected_id, cx)
                    );
                }
                
                // 未分配连接
                if !unassigned_connections.is_empty() {
                    container = container.child(
                        self.render_unassigned_section(unassigned_connections, selected_id, cx)
                    );
                }
                
                container
            })
    }

    fn open_workspace_tab(&mut self, workspace_id: i64, name: String, window: &mut Window, cx: &mut Context<Self>) {
        let connections: Vec<StoredConnection> = self.connections.iter()
            .cloned()
            .filter(|conn| conn.workspace_id == Some(workspace_id))
            .collect();
        self.tab_container.update(cx, |tc, cx| {
            let tab_id = format!("workspace-{}", workspace_id);
            tc.activate_or_add_tab_lazy(
                tab_id.clone(),
                {
                    move |window, cx| {
                        let ws_content = DatabaseTabContent::new_with_name(
                            Some(name),
                            connections,
                            window,
                            cx
                        );
                        TabItem::new(tab_id.clone(), ws_content)
                    }
                },
                window,
                cx
            )
        });
    }

    fn render_workspace_section(
        &self,
        workspace: Workspace,
        connections: Vec<StoredConnection>,
        selected_id: Option<i64>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let workspace_id = workspace.id;
        let workspace_name = workspace.name.clone();
        v_flex()
            .gap_2()
            .child(
                h_flex()
                    .id(ElementId::Name(SharedString::from(format!("workspace-name-{}", workspace_id.unwrap()))))
                    .items_center()
                    .gap_2()
                    .px_2()
                    .py_1()
                    .rounded(px(6.0))
                    .bg(cx.theme().muted)
                    .cursor_pointer()
                    .hover(|style| style.bg(cx.theme().accent.opacity(0.1)))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if let Some(ws_id) = workspace_id {
                            this.open_workspace_tab(ws_id, workspace_name.clone(), window, cx);
                        }
                    }))
                    .child(
                        Icon::new(IconName::Workspace).color()
                    )
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(cx.theme().foreground)
                            .child(workspace.name.clone())
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("({} 个连接)", connections.len()))
                    )
                    .child(
                        div().flex_1()
                    )
                    .child(
                        Button::new(SharedString::from(format!("edit-workspace-{}", workspace_id.unwrap_or(0))))
                            .icon(IconName::Settings)
                            .with_size(Size::Small)
                            .tooltip("编辑工作区")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                cx.stop_propagation();
                                this.show_workspace_form(workspace_id, window, cx);
                            }))
                    )
                    .child(
                        Button::new(SharedString::from(format!("open-workspace-{}", workspace_id.unwrap_or(0))))
                            .icon(IconName::ExternalLink)
                            .with_size(Size::Small)
                            .tooltip("打开工作区")
                    )
            )
            .when(!connections.is_empty(), |this| {
                // 使用 flex 布局实现响应式卡片网格
                let mut container = div()
                    .flex()
                    .flex_wrap()
                    .w_full()
                    .gap_3();
                
                for conn in connections {
                    container = container.child(
                        div()
                            .w(px(320.0))  // 固定宽度，不增长
                            .flex_shrink_0() // 不收缩
                            .child(self.render_connection_card(conn, selected_id, cx))
                    );
                }
                
                this.child(container)
            })
    }

    fn render_unassigned_section(
        &self,
        connections: Vec<StoredConnection>,
        selected_id: Option<i64>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        v_flex()
            .gap_2()
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .px_2()
                    .py_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(cx.theme().foreground)
                            .child("未分配工作区")
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("({} 个连接)", connections.len()))
                    )
            )
            .child({
                // 使用 flex 布局实现响应式卡片网格
                let mut container = div()
                    .flex()
                    .flex_wrap()
                    .w_full()
                    .gap_3();
                
                for conn in connections {
                    container = container.child(
                        div()
                            .w(px(320.0))  // 固定宽度，不增长
                            .flex_shrink_0() // 不收缩
                            .child(self.render_connection_card(conn, selected_id, cx))
                    );
                }
                container
            })
    }

    fn render_connection_card(
        &self,
        conn: StoredConnection,
        selected_id: Option<i64>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let conn_id = conn.id;
        let clone_conn = conn.clone();
        let is_selected = selected_id == conn.id;

        div()
            .id(SharedString::from(format!("conn-card-{}", conn.id.unwrap_or(0))))
            .w_full()
            .rounded(px(8.0))
            .bg(cx.theme().background)
            .p_2()
            .border_1()
            .rounded_lg()
            .when(is_selected, |this| {
                this.border_color(cx.theme().primary)
                    .bg(cx.theme().primary_foreground)
            })
            .when(!is_selected, |this| {
                this.border_color(cx.theme().border)
            })
            .cursor_pointer()
            .hover(|style| {
                style
                    // .bg(cx.theme().primary_foreground)
                    .border_color(cx.theme().primary)
            })
            .on_double_click(cx.listener(move |this, _, w, cx| {
                this.add_item_to_tab(&clone_conn, w, cx);
                cx.notify()
            }))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.selected_connection_id = conn_id;
                cx.notify();
            }))
            .child(
                v_flex()
                    .w_full()
                    .child(
                        h_flex()
                            .items_center()
                            .justify_between()
                            .p_3()
                            .child(
                                h_flex()
                                    .items_center()
                                    .gap_2()
                                    .flex_1()
                                    .overflow_hidden()
                                    .child(
                                        div()
                                            // .w(px(48.0))
                                            .h(px(48.0))
                                            .rounded(px(8.0))
                                            // .bg(icon_bg)
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .child(
                                                match conn.connection_type {
                                                    ConnectionType::Database => {
                                                        conn.to_db_connection().unwrap().database_type.as_icon()
                                                            .with_size(px(40.0))
                                                            .text_color(gpui::white())
                                                    },
                                                    _ => {
                                                        IconName::Redis.color()
                                                            .with_size(px(40.0))
                                                            .text_color(gpui::white())
                                                    },
                                                }
                                            )
                                    )
                                    .child(
                                        v_flex()
                                            .flex_1()
                                            .gap_0p5()
                                            .overflow_hidden()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::SEMIBOLD)
                                                    .text_color(cx.theme().foreground)
                                                    .child(conn.name.clone())
                                            )
                                            .when_some(conn.to_database_params().ok(), |this, params| {
                                                this.child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child(format!("{}@{}:{}", params.username, params.host, params.port))
                                                )
                                            })
                                    )
                            )
                    )
                    .child(
                        div()
                            .w_full()
                            .h(px(1.0))
                            .bg(cx.theme().border)
                    )
                    .child(
                        div()
                            .px_3()
                            .py_2()
                            .when_some(conn.to_database_params().ok().and_then(|p| p.database), |this, db| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(format!("数据库: {}", db))
                                )
                            })
                    )
            )
    }
}


impl Render for HomePage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .relative()
            .child(
                h_flex()
                    .size_full()
                    .child(self.render_sidebar(window, cx))
                    .child(
                        v_flex()
                            .flex_1()
                            .h_full()
                            .bg(cx.theme().background)
                            .child(self.render_toolbar(window, cx))
                            .child(
                                div()
                                    .flex_1()
                                    .w_full()
                                    .overflow_hidden()
                                    .bg(cx.theme().muted)
                                    .child(self.render_content_area(cx))
                            )
                    )
                    .when(self.ai_panel_visible, |this| {
                        this.child(
                            div()
                                .w(px(400.0))
                                .h_full()
                                .border_l_1()
                                .border_color(cx.theme().border)
                                .bg(cx.theme().background)
                                .child(self.ai_chat_panel.clone())
                        )
                    })
            )
            // 悬浮的 AI 按钮
            .child(
                div()
                    .absolute()
                    .right_4()
                    .bottom_4()
                    .child(
                        Button::new("toggle-ai-chat")
                            .icon(IconName::Bot)
                            .tooltip("AI Assistant")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_ai_panel(cx);
                            }))
                    )
            )
    }
}

// HomeTabContent - TabContent 的薄包装层
pub struct HomeTabContent {
    home_page: Entity<HomePage>,
}

impl HomeTabContent {
    pub fn new(tab_container: Entity<TabContainer>, window: &mut Window, cx: &mut App) -> Self {
        let home_page = cx.new(|cx| HomePage::new(tab_container, window, cx));
        Self {
            home_page,
        }
    }
}

impl TabContent for HomeTabContent {
    fn title(&self) -> SharedString {
        "首页".into()
    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::LayoutDashboard)
    }

    fn closeable(&self) -> bool {
        false // 首页不可关闭
    }

    fn render_content(&self, _window: &mut Window, _cx: &mut App) -> AnyElement {
        self.home_page.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::Custom("home".to_string())
    }

    fn width_size(&self) -> Option<Size> {
        Some(Size::Small)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
