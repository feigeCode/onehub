use crate::db_tree_view::{get_icon_for_node_type, DbTreeViewEvent};
use anyhow::anyhow;
use db::{DbNode, DbNodeType, GlobalDbState, ObjectView};
use gpui::{div, AnyElement, App, AppContext, AsyncApp, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, Subscription, Window};
use gpui_component::button::Button;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::label::Label;
use gpui_component::notification::Notification;
use gpui_component::WindowExt;
use gpui_component::{h_flex, table::{Column, Table, TableDelegate, TableEvent, TableState}, v_flex, ActiveTheme, Icon, IconName, Sizable, Size};
use one_core::gpui_tokio::Tokio;
use one_core::storage::{ConnectionRepository, DatabaseType, DbConnectionConfig, GlobalStorageState, Workspace};
use one_core::tab_container::{TabContent, TabContentType};
use one_core::utils::debouncer::Debouncer;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

fn format_timestamp(ts: i64) -> String {
    use chrono::{DateTime, Local};
    if let Some(dt) = DateTime::from_timestamp_millis(ts) {
        let local: DateTime<Local> = dt.into();
        local.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        "".to_string()
    }
}

/// 数据库对象面板事件 - 统一的表格交互事件
#[derive(Clone, Debug)]
pub enum DatabaseObjectsEvent {
    /// 表格行被选中（支持 ObjectView 和 ConnectionList）
    TableRowSelected { row: usize },
    /// 表格第一列（名称/图标）被点击
    TableFirstColumnClicked { row: usize },
}

pub struct DatabaseObjects {
    loaded_data: Entity<ObjectView>,
    table_state: Entity<TableState<ResultsDelegate>>,
    focus_handle: FocusHandle,
    workspace: Option<Workspace>,
    search_input: Entity<InputState>,
    search_query: String,
    search_seq: u64,
    search_debouncer: Arc<Debouncer>,
    current_node: Option<DbNode>,
    selected_row: Option<usize>,
    _subscriptions: Vec<Subscription>,
}

impl DatabaseObjects {
    pub fn new(workspace: Option<Workspace>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let loaded_data = cx.new(|_| ObjectView::default());
        let delegate = ResultsDelegate::new(vec![], vec![]);
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));
        let focus_handle = cx.focus_handle();
        let search_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder("搜索...").clean_on_escape()
        });
        let search_debouncer = Arc::new(Debouncer::new(Duration::from_millis(250)));

        let search_sub = cx.subscribe_in(&search_input, window, |this: &mut Self, input: &Entity<InputState>, event: &InputEvent, _window, cx: &mut Context<Self>| {
            if let InputEvent::Change = event {
                let query = input.read(cx).text().to_string();

                this.search_seq += 1;
                let current_seq = this.search_seq;
                let debouncer = Arc::clone(&this.search_debouncer);
                let query_for_task = query.clone();
                let table_state = this.table_state.clone();

                cx.spawn(async move |view, cx| {
                    if debouncer.debounce().await {
                        _ = view.update(cx, |this, cx| {
                            if this.search_seq == current_seq {
                                this.search_query = query_for_task.clone();
                                table_state.update(cx, |state, cx| {
                                    state.delegate_mut().set_search_query(query_for_task);
                                    state.refresh(cx);
                                });
                            }
                        });
                    }
                }).detach();
            }
        });

        let table_sub = cx.subscribe_in(&table_state, window, |this: &mut Self, _table: &Entity<TableState<ResultsDelegate>>, event: &TableEvent, _window, cx: &mut Context<Self>| {
            match event {
                TableEvent::DoubleClickedCell(row, col) => {
                    this.handle_row_double_click(*row, *col, cx);
                }
                TableEvent::SelectRow(row) => {
                    this.selected_row = Some(*row);
                }
                _ => {}
            }
        });

        Self {
            loaded_data,
            table_state,
            focus_handle,
            workspace,
            search_input,
            search_query: "".to_string(),
            search_seq: 0,
            search_debouncer,
            current_node: None,
            selected_row: None,
            _subscriptions: vec![search_sub, table_sub],
        }
    }


    fn handle_row_double_click(&self, row: usize, col: usize, cx: &mut Context<Self>) {
        // 只有双击第一列才触发打开操作
        if col != 0 {
            return;
        }

        let Some(current_node) = &self.current_node else {
            return;
        };

        let loaded_data = self.loaded_data.read(cx);
        let db_node_type = loaded_data.db_node_type.clone();

        let filtered_row = self.table_state.read(cx).delegate().filtered_rows.get(row).copied();
        let Some(original_row) = filtered_row else {
            return;
        };

        let row_data = self.table_state.read(cx).delegate().rows.get(original_row).cloned();
        let Some(row_values) = row_data else {
            return;
        };

        // 获取双击的第一列值（名称）
        let cell_value = row_values.first().cloned().unwrap_or_default();
        if cell_value.is_empty() {
            return;
        }

        let connection_id = &current_node.connection_id;
        let database = current_node.metadata.as_ref()
            .and_then(|m| m.get("database"))
            .cloned()
            .unwrap_or_default();

        let node_id = match db_node_type {
            DbNodeType::Database | DbNodeType::TablesFolder => {
                format!("{}:{}:tables:{}", connection_id, database, cell_value)
            }
            DbNodeType::ViewsFolder => {
                format!("{}:{}:views:{}", connection_id, database, cell_value)
            }
            DbNodeType::QueriesFolder => {
                format!("{}:queries:{}", connection_id, cell_value)
            }
            _ => return,
        };

        let event = match db_node_type {
            DbNodeType::Database | DbNodeType::TablesFolder => {
                Some(DbTreeViewEvent::OpenTableData { node_id })
            }
            DbNodeType::ViewsFolder => {
                Some(DbTreeViewEvent::OpenViewData { node_id })
            }
            DbNodeType::QueriesFolder => {
                Some(DbTreeViewEvent::OpenNamedQuery { node_id })
            }
            _ => None,
        };

        if let Some(evt) = event {
            cx.emit(evt);
        }
    }

    pub fn handle_node_selected(&mut self, node: DbNode, config: DbConnectionConfig, cx: &mut App) {
        self.current_node = Some(node.clone());
        self.selected_row = None;
        let loaded_data = self.loaded_data.clone();
        let table_state = self.table_state.clone();
        let node_clone = node.clone();
        let config_clone = config.clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();
        let global_state = cx.global::<GlobalDbState>().clone();
        let workspace = self.workspace.clone();
        cx.spawn(async move |cx: &mut AsyncApp| {

            let load_task = Tokio::spawn_result(cx, async move {
                let db_type = config_clone.database_type;
                let plugin = global_state
                    .db_manager
                    .get_plugin(&db_type)
                    .map_err(|err| anyhow!("Unsupported database type {:?}: {}", db_type, err))?;

                let conn_arc = global_state
                    .connection_pool
                    .get_connection(config_clone, &global_state.db_manager)
                    .await
                    .map_err(|e| anyhow!("Failed to get connection: {:?}", e))?;

                let conn = conn_arc.read().await;

                // 获取当前连接的信息
                let conn_repo_arc = storage_manager.get::<ConnectionRepository>().await
                    .ok_or_else(|| anyhow!("获取连接仓库失败"))?;

                let view = match node_clone.node_type {
                    DbNodeType::Connection => {
                        if node_clone.children_loaded {
                            plugin.list_databases_view(&**conn).await.ok()
                        } else {
                            if let Some(w) = workspace {
                                let connections = conn_repo_arc.list_by_workspace(w.id).await.ok();
                                let rows = connections
                                    .map(|conns| {
                                        conns.iter().map(|stored_conn| {
                                            let created = stored_conn.created_at
                                                .map(|ts| format_timestamp(ts))
                                                .unwrap_or_default();
                                            let updated = stored_conn.updated_at
                                                .map(|ts| format_timestamp(ts))
                                                .unwrap_or_default();
                                            let remark = stored_conn.remark.clone().unwrap_or_default();
                                            let mut db_type = DatabaseType::MySQL;
                                            if let Some(db_config) = stored_conn.to_db_connection().ok() {
                                               db_type = db_config.database_type;
                                            }
                                            vec![
                                                stored_conn.name.clone(),
                                                db_type.as_str().into(),
                                                created,
                                                updated,
                                                remark,
                                            ]
                                        }).collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default();

                                Some(ObjectView {
                                    db_node_type: DbNodeType::Connection,
                                    columns: vec![
                                        Column::new("name", "连接名称").width(200.0),
                                        Column::new("type", "连接类型"),
                                        Column::new("created_at", "创建日期").width(200.0),
                                        Column::new("updated_at", "访问日期").width(200.0),
                                        Column::new("remark", "备注").width(200.0),
                                    ],
                                    rows,
                                    title: "连接列表".to_string()
                                })
                            } else {
                                None
                            }
                        }
                    }
                    DbNodeType::Database => {
                        let database = &node_clone.name;
                        if plugin.supports_schema() {
                            plugin.list_schemas_view(&**conn, database).await.ok()
                        } else {
                            plugin.list_tables_view(&**conn, database).await.ok()
                        }
                    }
                    DbNodeType::TablesFolder => {
                        let mut database = &node_clone.name;
                        if let Some(metadata) = node_clone.metadata.as_ref() {
                            if let Some(value) = metadata.get("database") {
                                database = value;
                            }
                        }
                        plugin.list_tables_view(&**conn, database).await.ok()
                    }
                    DbNodeType::Schema => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_tables_view(&**conn, database).await.ok()
                    }
                    DbNodeType::Table | DbNodeType::ColumnsFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = match metadata.get("database") {
                            Some(value) => value,
                            None => return Ok(None),
                        };
                        let schema = metadata.get("schema").map(|s| s.as_str());
                        let table = metadata.get("table").map(|s| s.as_str()).unwrap_or(&node_clone.name);
                        plugin.list_columns_view(&**conn, database, schema, table).await.ok()
                    }
                    DbNodeType::ViewsFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_views_view(&**conn, database).await.ok()
                    }
                    DbNodeType::FunctionsFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_functions_view(&**conn, database).await.ok()
                    }
                    DbNodeType::ProceduresFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_procedures_view(&**conn, database).await.ok()
                    }
                    DbNodeType::TriggersFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_triggers_view(&**conn, database).await.ok()
                    }
                    DbNodeType::SequencesFolder => {
                        let metadata = match node_clone.metadata.as_ref() {
                            Some(meta) => meta,
                            None => return Ok(None),
                        };
                        let database = metadata.get("database").unwrap_or(&node_clone.name);
                        plugin.list_sequences_view(&**conn, database).await.ok()
                    }
                    _ => None,
                };

                Ok(view)
            });

            let result = match load_task {
                Ok(task) => task.await.ok().flatten(),
                Err(e) => {
                    eprintln!("Failed to schedule database object view load: {}", e);
                    None
                }
            };

            if let Some(view) = result {
                let columns = view.columns.clone();
                let rows = view.rows.clone();
                let db_node_type = view.db_node_type.clone();

                cx.update(|cx| {
                    loaded_data.update(cx, |data, _cx| {
                        *data = view;
                    });
                })
                    .ok();

                cx.update(|cx| {
                    table_state.update(cx, |state, _cx| {
                        state.delegate_mut().update_data(columns, rows, db_node_type);
                        state.refresh(_cx);
                    });
                })
                    .ok();
            }
            Some(())
        })
            .detach();
    }

    fn get_selected_row_name(&self, cx: &Context<Self>) -> Option<String> {
        let row_ix = self.selected_row?;
        let table_state = self.table_state.read(cx);
        let delegate = table_state.delegate();
        let original_row = delegate.filtered_rows.get(row_ix).copied()?;
        let row_data = delegate.rows.get(original_row)?;
        row_data.first().cloned()
    }

    fn build_node_id_for_selected_row(&self, cx: &Context<Self>) -> Option<String> {
        let name = self.get_selected_row_name(cx)?;
        let current_node = self.current_node.as_ref()?;
        let loaded_data = self.loaded_data.read(cx);
        let db_node_type = &loaded_data.db_node_type;
        let connection_id = &current_node.connection_id;
        let database = current_node.metadata.as_ref()
            .and_then(|m| m.get("database"))
            .cloned()
            .unwrap_or_default();

        match db_node_type {
            DbNodeType::Connection => {
                if current_node.children_loaded {
                    Some(format!("{}:{}", connection_id, name))
                } else {
                    Some(name)
                }
            }
            DbNodeType::Database | DbNodeType::TablesFolder => {
                let db = if database.is_empty() { &current_node.name } else { &database };
                Some(format!("{}:{}:table_folder:{}", connection_id, db, name))
            }
            DbNodeType::ViewsFolder => {
                let db = if database.is_empty() { &current_node.name } else { &database };
                Some(format!("{}:{}:views_folder:{}", connection_id, db, name))
            }
            DbNodeType::QueriesFolder => {
                Some(format!("{}:queries:{}", connection_id, name))
            }
            _ => None,
        }
    }

    fn render_toolbar_buttons(&self, node_type: DbNodeType, window: &mut Window, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let mut buttons: Vec<AnyElement> = vec![];
        let current_node = self.current_node.clone();
        let selected_node_id = self.build_node_id_for_selected_row(cx);

        let create_button = |id: &'static str, icon: IconName, tooltip: &'static str, event_fn: fn(&DbNode) -> Option<DbTreeViewEvent>, window: &Window, cx: &Context<Self>| -> AnyElement {
            let node = current_node.clone();
            Button::new(id)
                .with_size(Size::Medium)
                .icon(icon)
                .tooltip(tooltip)
                .on_click(window.listener_for(&cx.entity(), move |_this, _, _, cx| {
                    if let Some(ref node) = node {
                        if let Some(event) = event_fn(node) {
                            cx.emit(event);
                        }
                    }
                }))
                .into_any_element()
        };

        let create_row_button = |id: &'static str, icon: IconName, tooltip: &'static str, event_fn: fn(String) -> DbTreeViewEvent, win: &Window, cx: &Context<Self>| -> AnyElement {
            let node_id = selected_node_id.clone();
            Button::new(id)
                .with_size(Size::Medium)
                .icon(icon)
                .tooltip(tooltip)
                .on_click(win.listener_for(&cx.entity(), move |_this, _, window, cx| {
                    if let Some(ref nid) = node_id {
                        let event = event_fn(nid.clone());
                        cx.emit(event);
                    } else {
                        window.push_notification(Notification::warning("请先选择一行数据"), cx);
                    }
                }))
                .into_any_element()
        };

        buttons.push(create_button(
            "refresh-data",
            IconName::Refresh,
            "刷新",
            |node| Some(DbTreeViewEvent::NodeSelected { node_id: node.id.clone() }),
            window,
            cx,
        ));

        match node_type {
            DbNodeType::Connection => {
                buttons.push(create_button(
                    "create-database",
                    IconName::Plus,
                    "新建数据库",
                    |node| Some(DbTreeViewEvent::CreateDatabase { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "delete-connection",
                    IconName::Minus,
                    "删除连接",
                    |node_id| DbTreeViewEvent::DeleteConnection { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "close-connection",
                    IconName::CircleX,
                    "关闭连接",
                    |node_id| DbTreeViewEvent::CloseConnection { node_id },
                    window,
                    cx,
                ));
            }
            DbNodeType::Database | DbNodeType::TablesFolder => {
                buttons.push(create_button(
                    "create-table",
                    IconName::Plus,
                    "新建表",
                    |node| Some(DbTreeViewEvent::DesignTable { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "open-table",
                    IconName::Eye,
                    "查看表数据",
                    |node_id| DbTreeViewEvent::OpenTableData { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "design-table",
                    IconName::Edit,
                    "设计表",
                    |node_id| DbTreeViewEvent::DesignTable { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "drop-table",
                    IconName::Minus,
                    "删除表",
                    |node_id| DbTreeViewEvent::DeleteTable { node_id },
                    window,
                    cx,
                ));
            }
            DbNodeType::Table | DbNodeType::ColumnsFolder | DbNodeType::Column => {
                buttons.push(create_button(
                    "open-table",
                    IconName::Eye,
                    "查看表数据",
                    |node| {
                        let table_node_id = node.parent_context.clone().unwrap_or_else(|| node.id.clone());
                        Some(DbTreeViewEvent::OpenTableData { node_id: table_node_id })
                    },
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "design-table",
                    IconName::Edit,
                    "设计表",
                    |node| {
                        let table_node_id = node.parent_context.clone().unwrap_or_else(|| node.id.clone());
                        Some(DbTreeViewEvent::DesignTable { node_id: table_node_id })
                    },
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "drop-table",
                    IconName::Minus,
                    "删除表",
                    |node| {
                        let table_node_id = node.parent_context.clone().unwrap_or_else(|| node.id.clone());
                        Some(DbTreeViewEvent::DeleteTable { node_id: table_node_id })
                    },
                    window,
                    cx,
                ));
            }
            DbNodeType::ViewsFolder => {
                buttons.push(create_row_button(
                    "open-view",
                    IconName::Eye,
                    "查看视图数据",
                    |node_id| DbTreeViewEvent::OpenViewData { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "drop-view",
                    IconName::Minus,
                    "删除视图",
                    |node_id| DbTreeViewEvent::DeleteView { node_id },
                    window,
                    cx,
                ));
            }
            DbNodeType::View => {
                buttons.push(create_button(
                    "open-view",
                    IconName::Eye,
                    "查看视图数据",
                    |node| Some(DbTreeViewEvent::OpenViewData { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "drop-view",
                    IconName::Minus,
                    "删除视图",
                    |node| Some(DbTreeViewEvent::DeleteView { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
            }
            DbNodeType::FunctionsFolder | DbNodeType::Function |
            DbNodeType::ProceduresFolder | DbNodeType::Procedure |
            DbNodeType::TriggersFolder | DbNodeType::Trigger |
            DbNodeType::IndexesFolder | DbNodeType::Index |
            DbNodeType::ForeignKeysFolder | DbNodeType::ForeignKey |
            DbNodeType::SequencesFolder | DbNodeType::Sequence |
            DbNodeType::ChecksFolder | DbNodeType::Check => {
            }
            DbNodeType::QueriesFolder => {
                buttons.push(create_button(
                    "create-query",
                    IconName::Plus,
                    "新建查询",
                    |node| Some(DbTreeViewEvent::CreateNewQuery { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "open-query",
                    IconName::Eye,
                    "打开查询",
                    |node_id| DbTreeViewEvent::OpenNamedQuery { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "rename-query",
                    IconName::Edit,
                    "重命名查询",
                    |node_id| DbTreeViewEvent::RenameQuery { node_id },
                    window,
                    cx,
                ));
                buttons.push(create_row_button(
                    "delete-query",
                    IconName::Minus,
                    "删除查询",
                    |node_id| DbTreeViewEvent::DeleteQuery { node_id },
                    window,
                    cx,
                ));
            }
            DbNodeType::NamedQuery => {
                buttons.push(create_button(
                    "open-query",
                    IconName::Eye,
                    "打开查询",
                    |node| Some(DbTreeViewEvent::OpenNamedQuery { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "rename-query",
                    IconName::Edit,
                    "重命名查询",
                    |node| Some(DbTreeViewEvent::RenameQuery { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "delete-query",
                    IconName::Minus,
                    "删除查询",
                    |node| Some(DbTreeViewEvent::DeleteQuery { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
            }
            DbNodeType::Schema => {
                buttons.push(create_button(
                    "create-query",
                    IconName::Plus,
                    "新建查询",
                    |node| Some(DbTreeViewEvent::CreateNewQuery { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
                buttons.push(create_button(
                    "delete-schema",
                    IconName::Minus,
                    "删除模式",
                    |node| Some(DbTreeViewEvent::DeleteSchema { node_id: node.id.clone() }),
                    window,
                    cx,
                ));
            }
        }

        buttons
    }
}

impl Render for DatabaseObjects {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let loaded_data = self.loaded_data.read(cx);
        let title = loaded_data.title.clone();
        let node_type = loaded_data.db_node_type.clone();
        let toolbar_buttons = self.render_toolbar_buttons(node_type, window, cx);

        v_flex()
            .size_full()
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .px_2()
                    .py_1()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .children(toolbar_buttons)
                    .child(div().flex_1())
                    .child({
                        div()
                            .flex_1()
                            .child(Input::new(&self.search_input)
                                .prefix(
                                    Icon::new(IconName::Search)
                                        .text_color(cx.theme().muted_foreground),
                                )
                                .cleanable(true)
                                .small()
                                .w_full())
                    })
                    .into_any_element()
            )
            .child(
                v_flex()
                    .size_full()
                    .gap_2()
                    .child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .child(Table::new(&self.table_state)
                                .stripe(false)
                                .bordered(true)),
                    ))
            .child(div().p_2().text_sm().child(title))
    }
}


impl Clone for DatabaseObjects {
    fn clone(&self) -> Self {
        Self {
            loaded_data: self.loaded_data.clone(),
            table_state: self.table_state.clone(),
            focus_handle: self.focus_handle.clone(),
            workspace: self.workspace.clone(),
            search_input: self.search_input.clone(),
            search_seq: self.search_seq,
            search_query: self.search_query.clone(),
            search_debouncer: self.search_debouncer.clone(),
            current_node: self.current_node.clone(),
            selected_row: self.selected_row,
            _subscriptions: vec![],
        }
    }
}

impl EventEmitter<DbTreeViewEvent> for DatabaseObjects {}

impl Focusable for DatabaseObjects {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}


pub struct DatabaseObjectsPanel {
    database_objects: Entity<DatabaseObjects>,
}

impl DatabaseObjectsPanel {
    pub fn new(workspace: Option<Workspace>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let database_objects = cx.new(|cx| DatabaseObjects::new(workspace, window, cx));

        Self {
            database_objects,
        }
    }

    pub fn database_objects(&self) -> &Entity<DatabaseObjects> {
        &self.database_objects
    }

    pub fn handle_node_selected(&self, node: DbNode, config: DbConnectionConfig, cx: &mut App) {
        self.database_objects.update(cx, |database_objects, cx| {
            database_objects.handle_node_selected(node, config, cx);
        })
    }
}

impl TabContent for DatabaseObjectsPanel {
    fn title(&self) -> SharedString {
        SharedString::from("对象")
    }

    fn closeable(&self) -> bool {
        false
    }
    fn render_content(&self, _window: &mut Window, _cx: &mut App) -> AnyElement {
            self.database_objects.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::TableData("Object".to_string())
    }

    fn width_size(&self) -> Option<Size> {
        Some(Size::XSmall)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for DatabaseObjectsPanel {
    fn clone(&self) -> Self {
        Self {
            database_objects: self.database_objects.clone(),
        }
    }
}

impl EventEmitter<DatabaseObjectsEvent> for DatabaseObjectsPanel {}



pub struct ResultsDelegate {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
    pub filtered_rows: Vec<usize>,
    pub search_query: String,
    pub db_node_type: DbNodeType,
}

impl Clone for ResultsDelegate {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            rows: self.rows.clone(),
            filtered_rows: self.filtered_rows.clone(),
            search_query: self.search_query.clone(),
            db_node_type: self.db_node_type.clone(),
        }
    }
}

impl ResultsDelegate {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Vec<String>>) -> Self {
        let filtered_rows = (0..rows.len()).collect();
        Self {
            columns,
            rows,
            filtered_rows,
            search_query: String::new(),
            db_node_type: DbNodeType::default(),
        }
    }

    pub(crate) fn update_data(&mut self, columns: Vec<Column>, rows: Vec<Vec<String>>, db_node_type: DbNodeType) {
        self.columns = columns;
        self.filtered_rows = (0..rows.len()).collect();
        self.rows = rows;
        self.search_query.clear();
        self.db_node_type = db_node_type;
    }

    pub(crate) fn set_search_query(&mut self, query: String) {
        self.search_query = query.to_lowercase();
        self.apply_filter();
    }

    fn apply_filter(&mut self) {
        if self.search_query.is_empty() {
            self.filtered_rows = (0..self.rows.len()).collect();
        } else {
            self.filtered_rows = self
                .rows
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    row.iter()
                        .any(|cell| cell.to_lowercase().contains(&self.search_query))
                })
                .map(|(idx, _)| idx)
                .collect();
        }
    }
}

impl TableDelegate for ResultsDelegate {
    fn row_number_enabled(&self, _cx: &App) -> bool {
        true
    }
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }
    fn rows_count(&self, _cx: &App) -> usize {
        self.filtered_rows.len()
    }
    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns.get(col_ix).cloned().unwrap_or_else(|| Column::new("", ""))
    }
    fn render_td(
        &mut self,
        row: usize,
        col: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let cell_value = self.filtered_rows
            .get(row)
            .and_then(|&original_row| self.rows.get(original_row))
            .and_then(|r| r.get(col))
            .cloned()
            .unwrap_or_default();

        if col == 0 {
            let icon = get_icon_for_node_type(&self.db_node_type, cx.theme()).color();
            let label = if self.search_query.is_empty() {
                Label::new(cell_value)
            } else {
                Label::new(cell_value).highlights(self.search_query.clone())
            };
            h_flex()
                .gap_2()
                .items_center()
                .child(icon)
                .child(label)
                .into_any_element()
        } else {
            div().child(cell_value).into_any_element()
        }
    }
}
