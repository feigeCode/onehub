use std::any::Any;

use anyhow::anyhow;
use db::{DbNode, DbNodeType, GlobalDbState, ObjectView};
use gpui::{div, AnyElement, App, AppContext, AsyncApp, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, SharedString, Styled, Window};
use gpui_component::{
    table::{Column, Table, TableDelegate, TableState},
    v_flex, Size,
};
use one_core::gpui_tokio::Tokio;
use one_core::storage::{ConnectionRepository, DbConnectionConfig, GlobalStorageState, Workspace};
use one_core::tab_container::{TabContent, TabContentType};

fn format_timestamp(ts: i64) -> String {
    let secs = ts / 1000;
    format!("{}", ts)
}

/// 数据库对象面板事件 - 统一的表格交互事件
#[derive(Clone, Debug)]
pub enum DatabaseObjectsEvent {
    /// 表格行被选中（支持 ObjectView 和 ConnectionList）
    TableRowSelected { row: usize },
    /// 表格第一列（名称/图标）被点击
    TableFirstColumnClicked { row: usize },
}

pub struct DatabaseObjectsPanel {
    loaded_data: Entity<ObjectView>,
    table_state: Entity<TableState<ResultsDelegate>>,
    focus_handle: FocusHandle,
    workspace: Option<Workspace>
}

impl DatabaseObjectsPanel {
    pub fn new(workspace: Option<Workspace>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let loaded_data = cx.new(|_| ObjectView::default());
        let delegate = ResultsDelegate::new(vec![], vec![]);
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));
        let focus_handle = cx.focus_handle();

        Self {
            loaded_data,
            table_state,
            focus_handle,
            workspace
        }
    }

    pub fn handle_node_selected(&self, node: DbNode, config: DbConnectionConfig, cx: &mut App) {
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

                let pool = storage_manager.get_pool().await.map_err(|e| anyhow!("获取连接池失败: {}", e))?;

                // 获取当前连接的信息
                let conn_repo_arc = storage_manager.get::<ConnectionRepository>().await
                    .ok_or_else(|| anyhow!("获取连接仓库失败"))?;

                let view = match node_clone.node_type {
                    DbNodeType::Connection => {
                        if node_clone.children_loaded {
                            plugin.list_databases_view(&**conn).await.ok()
                        } else {
                            if let Some(w) = workspace {
                                let connections = conn_repo_arc.list_by_workspace(&pool, w.id).await.ok();
                                let rows = connections
                                    .map(|conns| {
                                        conns.iter().map(|stored_conn| {
                                            let created = stored_conn.created_at
                                                .map(|ts| format_timestamp(ts))
                                                .unwrap_or_default();
                                            let updated = stored_conn.updated_at
                                                .map(|ts| format_timestamp(ts))
                                                .unwrap_or_default();
                                            vec![
                                                stored_conn.name.clone(),
                                                format!("{:?}", stored_conn.connection_type),
                                                created,
                                                updated,
                                            ]
                                        }).collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default();

                                Some(ObjectView {
                                    db_node_type: DbNodeType::Connection,
                                    columns: vec![
                                        Column::new("name", "连接名称"),
                                        Column::new("type", "连接类型"),
                                        Column::new("created_at", "创建日期"),
                                        Column::new("updated_at", "访问日期"),
                                    ],
                                    rows,
                                    title: "连接列表".to_string()
                                })
                            } else {
                                None
                            }
                        }
                    }
                    DbNodeType::Database | DbNodeType::TablesFolder => {
                        let mut database = &node_clone.name;
                        if let Some(metadata) = node_clone.metadata.as_ref() {
                            if let Some(value) = metadata.get("database") {
                                database = value;
                            }
                        }
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
                        let table = metadata.get("table").map(|s| s.as_str()).unwrap_or(&node_clone.name);
                        plugin.list_columns_view(&**conn, database, table).await.ok()
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

                cx.update(|cx| {
                    loaded_data.update(cx, |data, _cx| {
                        *data = view;
                    });
                })
                    .ok();

                cx.update(|cx| {
                    table_state.update(cx, |state, _cx| {
                        state.delegate_mut().update_data(columns, rows);
                        state.refresh(_cx);
                    });
                })
                    .ok();
            }
            Some(())
        })
            .detach();
    }
}


impl TabContent for DatabaseObjectsPanel {
    fn title(&self) -> SharedString {
        SharedString::from("对象")
    }

    fn closeable(&self) -> bool {
        false
    }
    fn render_content(&self, _window: &mut Window, cx: &mut App) -> AnyElement {
        let loaded_data = self.loaded_data.read(cx);
        let title = loaded_data.title.clone();
        // TODO 支持搜索表格，安装第一列过滤
        div()
            .size_full()
            .child( 
                v_flex()
                .size_full()
                .gap_2()
                .child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .child(Table::new(&self.table_state)
                            .stripe(true)
                            .bordered(true)),
                ))
            .child(div().p_2().text_sm().child(title))
            .into_any_element()
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

impl Focusable for DatabaseObjectsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for DatabaseObjectsPanel {
    fn clone(&self) -> Self {
        Self {
            loaded_data: self.loaded_data.clone(),
            table_state: self.table_state.clone(),
            focus_handle: self.focus_handle.clone(),
            workspace: self.workspace.clone()
        }
    }
}

impl EventEmitter<DatabaseObjectsEvent> for DatabaseObjectsPanel {}



pub struct ResultsDelegate {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}

impl Clone for ResultsDelegate {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            rows: self.rows.clone(),
        }
    }
}

impl ResultsDelegate {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Vec<String>>) -> Self {
        Self {
            columns,
            rows,
        }
    }

    pub(crate) fn update_data(&mut self, columns: Vec<Column>, rows: Vec<Vec<String>>) {
        self.columns = columns;
        self.rows = rows;
    }
}

impl TableDelegate for ResultsDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }
    fn rows_count(&self, _cx: &App) -> usize {
        self.rows.len()
    }
    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns[col_ix].clone()
    }
    fn render_td(
        &mut self,
        row: usize,
        col: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        self.rows
            .get(row)
            .and_then(|r| r.get(col))
            .cloned()
            .unwrap_or_default()
    }
}
