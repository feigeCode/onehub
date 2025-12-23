use std::any::Any;

use anyhow::Error;
use gpui::{div, px, AnyElement, App, AppContext, AsyncApp, Context, ElementId, Entity, FontWeight, InteractiveElement, IntoElement, ParentElement, Render, SharedString, StatefulInteractiveElement, Styled, Window};
use gpui::prelude::FluentBuilder;
use gpui_component::{button::{Button, ButtonVariants as _}, h_flex, input::{Input, InputEvent, InputState}, menu::PopupMenuItem, v_flex, ActiveTheme, Disableable, Icon, IconName, InteractiveElementExt, Sizable, Size, ThemeMode, WindowExt};

use one_core::storage::{ConnectionRepository, ConnectionType, DatabaseType, GlobalStorageState, StoredConnection, Workspace, WorkspaceRepository};
use one_core::storage::traits::Repository;
use one_core::tab_container::{TabContainer, TabContent, TabContentType, TabItem};
use one_core::themes::SwitchThemeMode;
use db_view::database_tab::DatabaseTabContent;
use db_view::database_view_plugin::DatabaseViewPluginRegistry;
use gpui_component::button::{ButtonCustomVariant, ButtonVariant};
use gpui_component::dialog::DialogButtonProps;
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
        };

        // 异步加载工作区和连接列表
        page.load_workspaces(cx);
        page.load_connections(cx);
        page
    }

    fn load_workspaces(&mut self, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();
        cx.spawn(async move |this, cx: &mut AsyncApp| {

            let task_result = async {
                Tokio::spawn_result(cx, async move {
                    let repo = storage.get::<WorkspaceRepository>().await
                        .ok_or_else(|| anyhow::anyhow!("WorkspaceRepository not found"))?;
                    let result: anyhow::Result<Vec<Workspace>> = repo.list().await;
                    result
                })?.await
            }.await;

            match task_result {
                Ok(workspaces) =>  {
                    _ = this.update(cx, |this, cx| {
                        this.workspaces = workspaces;
                        cx.notify();
                    });
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    fn load_connections(&mut self, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();
        cx.spawn(async move |this, cx: &mut AsyncApp| {
            let task_result = async {
                Tokio::spawn_result(cx, async move {
                    let repo = storage.get::<ConnectionRepository>().await
                        .ok_or_else(|| anyhow::anyhow!("ConnectionRepository not found"))?;
                    let result: anyhow::Result<Vec<StoredConnection>> = repo.list().await;
                    result
                })?.await
            }.await;
            match task_result {
                Ok(connections) => {
                    _ = this.update(cx, |this, cx| {
                        this.connections = connections;
                        cx.notify();
                    });
                }
                Err(e) => {
                    tracing::error!("Task join error: {}", e);
                }
            }
        }).detach();
    }

    fn delete_connection(&mut self, conn_id: i64, cx: &mut Context<Self>) {
        let storage = cx.global::<GlobalStorageState>().storage.clone();
        cx.spawn(async move |this, cx: &mut AsyncApp| {
            let delete_result = async {
                Tokio::spawn_result(cx, async move {
                    let repo = storage.get::<ConnectionRepository>().await
                        .ok_or_else(|| anyhow::anyhow!("ConnectionRepository not found"))?;
                    repo.delete(conn_id).await
                })?.await
            }.await;
            match delete_result {
                Ok(_) => {
                    _ = this.update(cx, |this, cx| {
                        this.connections.retain(|c| c.id != Some(conn_id));
                        if this.selected_connection_id == Some(conn_id) {
                            this.selected_connection_id = None;
                        }
                        cx.notify();
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to delete connection: {}", e);
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
                .content_center()
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

            if editing_id.is_some() {
                repo.update(&mut workspace).await?;
            } else {
                repo.insert(&mut workspace).await?;
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
        let plugin_registry = cx.global::<DatabaseViewPluginRegistry>();
        let plugin = match plugin_registry.get(&db_type) {
            Some(p) => p,
            None => {
                tracing::error!("No plugin found for database type: {:?}", db_type);
                return;
            }
        };

        let form = plugin.create_connection_form(window, cx);

        form.update(cx, |f, cx| {
            f.set_workspaces(self.workspaces.clone(), window, cx);
        });

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
        let title_shared: SharedString = title.into();
        let view_clone = view.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let view_for_cancel = view_clone.clone();
            let view_for_ok = view_clone.clone();
            let form_footer = form_clone.clone();
            let form_test = form_clone.clone();
            let form_save = form_clone.clone();
            let form_cancel = form_clone.clone();
            dialog
                .title(title_shared.clone())
                .w(px(600.0))
                .h(px(550.0))
                .child(form_clone.clone())
                .close_button(true)
                .overlay(false)
                .content_center()
                .button_props(DialogButtonProps::default().ok_text("好"))
                .footer(move |ok_btn, cancel_btn, window, cx| {
                    let is_testing = form_footer.read(cx).is_testing(cx);
                    let form_t = form_test.clone();
                    vec![
                        cancel_btn(window, cx),
                        Button::new("test")
                            .outline()
                            .label(if is_testing { "测试中..." } else { "测试连接" })
                            .disabled(is_testing)
                            .on_click(window.listener_for(&form_t, |form, _, _, cx| {
                                form.trigger_test_connection(cx);
                                cx.notify();
                            }))
                            .into_any_element(),
                        ok_btn(window, cx)
                    ]
                })
                .on_ok(move |_, _window, cx| {
                    let (stored, is_update) = match form_save.read(cx).build_stored_connection(cx) {
                        Ok(data) => data,
                        Err(e) => {
                            form_save.update(cx, |f, cx| {
                                f.set_save_error(e, cx);
                            });
                            return false;
                        }
                    };

                    let storage = cx.global::<GlobalStorageState>().storage.clone();
                    let view = view_for_ok.clone();
                    let form = form_save.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let task_result = Tokio::spawn(cx, async move {
                            let repo = storage.get::<ConnectionRepository>().await
                                .ok_or_else(|| anyhow::anyhow!("ConnectionRepository not found"))?;

                            let mut stored = stored;
                            if is_update {
                                repo.update(&stored).await?;
                            } else {
                                repo.insert(&mut stored).await?;
                            }

                            Ok::<StoredConnection, Error>(stored)
                        });

                        let task = match task_result {
                            Ok(t) => t,
                            Err(e) => {
                                let _ = form.update(cx, |f, cx| {
                                    f.set_save_error(format!("启动保存任务失败: {}", e), cx);
                                });
                                return;
                            }
                        };

                        match task.await {
                            Ok(_saved_conn) => {
                                if let Some(window_id) = cx.update(|cx| cx.active_window()).ok().flatten() {
                                    let _ = cx.update_window(window_id, |_entity, window, cx| {
                                        window.close_dialog(cx);
                                        view.update(cx, |v, cx| {
                                            v.editing_connection_id = None;
                                            v.load_connections(cx);
                                            cx.notify();
                                        });
                                    });
                                }
                            }
                            Err(e) => {
                                let _ = form.update(cx, |f, cx| {
                                    f.set_save_error(format!("保存连接失败: {}", e), cx);
                                });
                            }
                        }
                    }).detach();

                    false
                })
                .on_cancel(move |_, _, cx| {
                    let _ = view_for_cancel.update(cx, |this, cx| {
                        this.editing_connection_id = None;
                        cx.notify();
                    });
                    form_cancel.update(cx, |this, cx| {
                        this.trigger_cancel(cx);
                    });
                    true
                })
        });
    }

    pub fn add_settings_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.tab_container.update(cx, |tc, cx| {
            tc.activate_or_add_tab_lazy("settings", |win, cx| {
                TabItem::new("settings", SettingsTabContent::new(win, cx))
            }, window, cx);
        });
    }

    fn add_item_to_tab(&mut self, conn: &StoredConnection, workspace: Option<Workspace>, window: &mut Window, cx: &mut Context<Self>) {
        self.tab_container.update(cx, |tc, cx| {
            let w = workspace.clone();
            let mut tab_id = format!("database-tab-{}", conn.id.unwrap_or(0));
            if let Some (w) = w {
                tab_id = format!("workspace-database-tab-{}", w.id.unwrap_or(0));
            }

            tc.activate_or_add_tab_lazy(
                tab_id.clone(),
                {
                    let workspace_id = workspace.clone().and_then(|w| w.id);
                    let mut connections = vec![conn.clone()];
                    if workspace_id.is_some() {
                        connections = self.connections.iter().filter(|&conn| conn.workspace_id == workspace_id).cloned()
                            .collect();

                    }
                    move |window, cx| {
                        let db_content = DatabaseTabContent::new_with_active_conn(workspace, connections, conn.id, window, cx);
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
                            .bg(cx.theme().primary)
                            .with_size(Size::Large)
                            .with_variant(ButtonVariant::Custom(ButtonCustomVariant::new(cx).hover(cx.theme().primary)))
                            .dropdown_menu(move |menu, window, _cx| {
                                let mut menu = menu
                                    .large()
                                    .item(
                                    PopupMenuItem::new("工作区")
                                                .icon(IconName::AppsColor.color().with_size(Size::Medium))
                                                .on_click(window.listener_for(&view, move |this, _, window, cx| {
                                                    this.show_workspace_form(None, window, cx);
                                                }))
                                );

                                for db_type in DatabaseType::all() {
                                    let db_type = *db_type;
                                    let label: SharedString = db_type.as_str().to_string().into();
                                    menu = menu.item(
                                        PopupMenuItem::new(label)
                                            .icon(db_type.as_node_icon().with_size(Size::Medium))
                                            .on_click(window.listener_for(&view, move |this, _, window, cx| {
                                                this.editing_connection_id = None;
                                                this.show_connection_form(db_type, window, cx);
                                            }))
                                    );
                                }

                                menu
                            })
                    )
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
                            let filter_type_clone = filter_type;

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
                                        .rounded_lg()
                                        .shadow_md()
                                })
                                .when(!is_selected, |this| {
                                    this.bg(cx.theme().background)
                                        .hover(|style| style.bg(cx.theme().accent).rounded_lg())
                                })
                                .on_click(cx.listener(move |this: &mut HomePage, _, _, cx| {
                                    this.selected_filter = filter_type_clone;
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
        if let Ok(params) = conn.to_db_connection() {
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

    fn render_workspace_section(
        &self,
        workspace: Workspace,
        connections: Vec<StoredConnection>,
        selected_id: Option<i64>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let workspace_id = workspace.id;
        v_flex()
            .gap_2()
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .px_2()
                    .py_1()
                    .rounded(px(6.0))
                    .bg(cx.theme().muted)
                    .text_color(cx.theme().chart_2)
                    .hover(|style| {
                        style
                            .bg(cx.theme().accent.opacity(0.1))
                            .text_color(cx.theme().primary)
                    })
                    .child(
                        Icon::new(IconName::AppsColor).color().with_size(Size::Medium)
                    )
                    .child(
                        div()
                            .id(ElementId::Name(SharedString::from(format!("workspace-name-{}", workspace_id.unwrap_or(0)))))
                            .text_base()
                            .font_weight(FontWeight::SEMIBOLD)
                            .hover(|style| {
                                style.text_color(cx.theme().primary)
                            })
                            .child(workspace.name.clone())
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .hover(|style| {style.text_color(cx.theme().primary)})
                            .child(format!("({} 个连接)", connections.len()))
                    )
                    .child(
                        div().flex_1()
                    )
                    .child(
                        Button::new(SharedString::from(format!("edit-workspace-{}", workspace_id.unwrap_or(0))))
                            .icon(IconName::Edit)
                            .with_size(Size::Small)
                            .tooltip("编辑工作区")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                cx.stop_propagation();
                                this.show_workspace_form(workspace_id, window, cx);
                            }))
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
                            .child(self.render_connection_card(conn,workspace_id, selected_id, cx))
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
                            .child(self.render_connection_card(conn, None, selected_id, cx))
                    );
                }
                container
            })
    }

    fn render_connection_card(
        &self,
        conn: StoredConnection,
        workspace_id: Option<i64>,
        selected_id: Option<i64>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let conn_id = conn.id;
        let clone_conn = conn.clone();
        let edit_conn = conn.clone();
        let delete_conn_id = conn.id;
        let delete_conn_name = conn.name.clone();
        let is_selected = selected_id == conn.id;
        let workspace = workspace_id.and_then(|id| {
            self.workspaces.iter().find(|w| w.id == Some(id)).cloned()
        });

        div()
            .id(SharedString::from(format!("conn-card-{}", conn.id.unwrap_or(0))))
            .w_full()
            .rounded(px(8.0))
            .bg(cx.theme().background)
            .p_3()
            .border_1()
            .rounded_lg()
            .relative()
            .group("")
            .when(is_selected, |this| {
                this.border_color(cx.theme().primary)
                    .shadow_md()
            })
            .when(!is_selected, |this| {
                this.border_color(cx.theme().border)
            })
            .cursor_pointer()
            .hover(|style| {
                style
                    .shadow_md()
                    .border_color(cx.theme().primary)
            })
            .on_double_click(cx.listener(move |this, _, w, cx| {
                this.add_item_to_tab(&clone_conn, workspace.clone(), w, cx);
                cx.notify()
            }))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.selected_connection_id = conn_id;
                cx.notify();
            }))
            .child(
                // hover时显示的编辑和删除按钮
                h_flex()
                    .absolute()
                    .top_2()
                    .right_2()
                    .gap_1()
                    .group_hover("", |style| style.opacity(1.0))
                    .opacity(0.0)
                    .child(
                        Button::new(SharedString::from(format!("edit-conn-{}", conn.id.unwrap_or(0))))
                            .icon(IconName::Edit)
                            .with_size(Size::Small)
                            .tooltip("编辑连接")
                            .on_click(cx.listener(move |this, _, window, cx| {
                                cx.stop_propagation();
                                if let Some(conn_id) = edit_conn.id {
                                    this.editing_connection_id = Some(conn_id);
                                    if let Ok(params) = edit_conn.to_db_connection() {
                                        this.show_connection_form(params.database_type, window, cx);
                                    }
                                }
                            }))
                    )
                    .child(
                        Button::new(SharedString::from(format!("delete-conn-{}", conn.id.unwrap_or(0))))
                            .icon(IconName::Remove)
                            .with_size(Size::Small)
                            .tooltip("删除连接")
                            .on_click(cx.listener(move |_this, _, window, cx| {
                                cx.stop_propagation();
                                if let Some(conn_id) = delete_conn_id {
                                    let view = cx.entity().clone();
                                    let conn_name = delete_conn_name.clone();
                                    window.open_dialog(cx, move |dialog, _window, _cx| {
                                        let view_clone = view.clone();
                                        dialog
                                            .title("确认删除")
                                            .child(format!("确定要删除连接 \"{}\" 吗？", conn_name))
                                            .confirm()
                                            .on_ok(move |_, _, cx| {
                                                let _ = view_clone.update(cx, |this, cx| {
                                                    this.delete_connection(conn_id, cx);
                                                });
                                                true
                                            })
                                    });
                                }
                            }))
                    )
            )
            .child(
                v_flex()
                    .w_full()
                    .child(
                        h_flex()
                            .items_center()
                            .justify_between()
                            .pb_2()
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
                                                        let icon = conn.to_db_connection()
                                                            .map(|c| c.database_type.as_icon())
                                                            .unwrap_or_else(|_| IconName::Database.color());
                                                        icon.with_size(px(40.0))
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
                                            .when_some(conn.to_db_connection().ok(), |this, params| {
                                                let conn_info = if params.database_type == DatabaseType::SQLite {
                                                    params.host.clone()
                                                } else {
                                                    format!("{}@{}:{}", params.username, params.host, params.port)
                                                };
                                                this.child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child(conn_info)
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
                            .when_some(conn.to_db_connection().ok().and_then(|p| p.database), |this, db| {
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

    fn icon(&self) -> Option<Icon> {
        Some(IconName::Workspace.color().with_size(Size::Medium))
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
