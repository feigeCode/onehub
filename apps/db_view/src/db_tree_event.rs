// 1. 标准库导入
// (无需标准库导入)

// 2. 外部 crate 导入（按字母顺序）
use db::{DbNode, DbNodeType, GlobalDbState, SqlResult};
use gpui::{div, px, App, AppContext, AsyncApp, Context, Entity, ParentElement, Styled, Subscription, Window};
use tracing::log::{error, warn};
use gpui_component::{
    h_flex, v_flex, WindowExt,
    notification::Notification,
};
use one_core::{
    gpui_tokio::Tokio,
    tab_container::{TabContainer, TabItem},
};
use uuid::Uuid;
use gpui_component::dialog::DialogButtonProps;
use one_core::storage::query_model::Query;
// 3. 当前 crate 导入（按模块分组）
use crate::{
    database_objects_tab::DatabaseObjectsPanel,
    database_view_plugin::DatabaseViewPluginRegistry,
    db_tree_view::{DbTreeView, DbTreeViewEvent},
    sql_editor_view::SqlEditorTabContent,
    table_designer::{TableDesignerConfig, TableDesignerTabContent},
};

// Event handler for database tree view events
pub struct DatabaseEventHandler {
    _tree_subscription: Subscription,
    _objects_subscription: Subscription,
}

impl DatabaseEventHandler {
    /// 显示错误通知
    fn show_error(window: &mut Window, message: impl Into<String>, cx: &mut App) {
        window.push_notification(
            Notification::error(message.into()).autohide(true),
            cx
        );
    }


    /// 显示警告通知
    fn show_warning(window: &mut Window, message: impl Into<String>, cx: &mut App) {
        window.push_notification(
            Notification::warning(message.into()).autohide(true),
            cx
        );
    }

    /// 在异步上下文中显示错误通知
    fn show_error_async(cx: &mut App, message: impl Into<String>) {
        if let Some(window) = cx.active_window() {
            _ = window.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::error(message.into()).autohide(true),
                    cx
                );
            });
        }
    }

    /// 在异步上下文中显示成功通知
    fn show_success_async(cx: &mut App, message: impl Into<String>) {
        if let Some(window) = cx.active_window() {
            _ = window.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::success(message.into()).autohide(true),
                    cx
                );
            });
        }
    }

    /// 从节点获取数据库名的辅助方法
    fn get_database_from_node(node: &DbNode) -> String {
        if node.node_type == DbNodeType::Database {
            node.name.clone()
        } else if let Some(metadata) = &node.metadata {
            metadata.get("database").cloned().unwrap_or_else(|| {
                node.parent_context.as_ref()
                    .and_then(|p| p.split(':').nth(1))
                    .unwrap_or("")
                    .to_string()
            })
        } else {
            node.parent_context.as_ref()
                .and_then(|p| p.split(':').nth(1))
                .unwrap_or("")
                .to_string()
        }
    }

    /// 在异步上下文中执行窗口操作的辅助方法
    async fn with_window<F>(cx: &mut AsyncApp, f: F)
    where
        F: FnOnce(&mut Window, &mut App) + Send + 'static,
    {
        let _ = cx.update(|cx| {
            if let Some(window_id) = cx.active_window() {
                let _ = cx.update_window(window_id, |_, window, cx| {
                    f(window, cx);
                });
            }
        });
    }
}

impl DatabaseEventHandler {
    pub(crate) fn new(
        db_tree_view: &Entity<DbTreeView>,
        tab_container: Entity<TabContainer>,
        objects_panel: Entity<DatabaseObjectsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {

        let tab_container_clone = tab_container.clone();
        let objects_panel_clone = objects_panel.clone();
        let global_state = cx.global::<GlobalDbState>().clone();
        let tree_view_clone = db_tree_view.clone();

        let tree_subscription = cx.subscribe_in(db_tree_view, window, move |_handler, _tree, event, window, cx| {
            let global_state = global_state.clone();
            let tab_container = tab_container_clone.clone();
            let objects_panel = objects_panel_clone.clone();
            let tree_view = tree_view_clone.clone();

            let get_node = |node_id: &str, cx: &mut Context<Self>| -> Option<DbNode> {
                let node = tree_view.read(cx).get_node(node_id).cloned();
                if node.is_none() {
                    warn!("not found node {}", node_id);
                }
                node
            };

            match event {
                DbTreeViewEvent::NodeSelected { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        if node.children_loaded {
                            Self::handle_node_selected(node, global_state, objects_panel, cx);
                        }
                    }
                }
                DbTreeViewEvent::CreateNewQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_new_query(node, tab_container,window, cx);
                    }
                }
                DbTreeViewEvent::OpenTableData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_table_data(node, global_state, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::OpenViewData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_view_data(node, global_state, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::OpenTableStructure { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_table_structure(node, global_state, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::DesignTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_design_table(node, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::ImportData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_import_data(node, global_state, window, cx);
                    }
                }
                DbTreeViewEvent::ExportData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_export_data(node, global_state, window, cx);
                    }
                }
                DbTreeViewEvent::CloseConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_connection(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_connection(node, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CreateDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::EditDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_edit_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CloseDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CreateSchema { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_schema(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteSchema { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_schema(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_table(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::RenameTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_rename_table(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::TruncateTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_truncate_table(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteView { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_view(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::OpenNamedQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_named_query(node, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::RenameQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_rename_query(node, tree_view, global_state, window, cx);
                    }
                }
                DbTreeViewEvent::DeleteQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_query(node, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::RunSqlFile { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_run_sql_file(node, global_state, window, cx);
                    }
                }
                DbTreeViewEvent::DumpSqlFile { node_id, mode } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_dump_sql_file(node, *mode, global_state, window, cx);
                    }
                }
            }
        });

        let tab_container_for_objects = tab_container.clone();
        let global_state_for_objects = cx.global::<GlobalDbState>().clone();
        let tree_view_for_objects = db_tree_view.clone();
        let objects_panel_clone = objects_panel.clone();

        let database_objects = objects_panel.read(cx).database_objects().clone();
        let objects_subscription = cx.subscribe_in(&database_objects, window, move |_handler, _db_objects, event, window, cx| {
            let global_state = global_state_for_objects.clone();
            let tab_container = tab_container_for_objects.clone();
            let tree_view = tree_view_for_objects.clone();
            let objects_panel = objects_panel_clone.clone();

            let get_node = |node_id: &str, cx: &mut Context<Self>| -> Option<DbNode> {
                let node = tree_view.read(cx).get_node(node_id).cloned();
                if node.is_none() {
                    warn!("not found node {} from objects panel", node_id);
                }
                node
            };

            match event {
                DbTreeViewEvent::NodeSelected { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_node_selected(node, global_state, objects_panel, cx);
                    }
                }
                DbTreeViewEvent::OpenTableData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_table_data(node, global_state, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::OpenViewData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_view_data(node, global_state, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::OpenNamedQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_named_query(node, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::CreateNewQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_new_query(node, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::DesignTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_design_table(node, tab_container, window, cx);
                    }
                }
                DbTreeViewEvent::DeleteTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_table(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteView { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_view(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::RenameQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_rename_query(node, tree_view.clone(), global_state, window, cx);
                    }
                }
                DbTreeViewEvent::DeleteQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_query(node, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CreateDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CloseDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_database(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::CloseConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_connection(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_connection(node, tree_view.clone(), window, cx);
                    }
                }
                DbTreeViewEvent::DeleteSchema { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_schema(node, global_state, tree_view.clone(), window, cx);
                    }
                }
                _ => {}
            }
        });

        Self {
            _tree_subscription: tree_subscription,
            _objects_subscription: objects_subscription,
        }
    }

    /// 处理节点选中事件
    fn handle_node_selected(
        node: DbNode,
        global_state: GlobalDbState,
        objects_panel: Entity<DatabaseObjectsPanel>,
        cx: &mut App,
    ) {

        let connection_id = node.connection_id.clone();
        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;
            if let Some(config) = config {
                _ = objects_panel.update(cx, |panel, cx| {
                    panel.handle_node_selected(node, config, cx);
                });
            } else {
                let _ = cx.update(|cx| {
                    Self::show_error_async(cx, "获取连接配置失败");
                });
            }
        }).detach();

    }

    /// 处理创建新查询事件
    fn handle_create_new_query(
        node: DbNode,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_editor_view::SqlEditorTabContent;

        let connection_id = node.connection_id.clone();
        let database = Self::get_database_from_node(&node);

        let sql_editor = SqlEditorTabContent::new_with_config(
            format!("{} - Query", if database.is_empty() { "New Query" } else { &database }),
            connection_id,
            None,
            if database.is_empty() { None } else { Some(database.clone()) },
            window,
            cx,
        );

        tab_container.update(cx, |container, cx| {
            let tab_id = format!("query-{}-{}", if database.is_empty() { "new" } else { &database }, Uuid::new_v4());
            let tab = TabItem::new(tab_id, sql_editor);
            container.add_and_activate_tab(tab, cx);
        });
    }

    /// 处理打开表数据事件
    fn handle_open_table_data(
        node: DbNode,
        global_state: GlobalDbState,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::table_data_tab::TableDataTabContent;

        let connection_id = node.connection_id.clone();
        let table = node.name.clone();

        let Some(ref metadata) = node.metadata else {
            Self::show_error(window, "无效的节点数据", cx);
            return;
        };
        let Some(database) = metadata.get("database") else {
            Self::show_error(window, "无法获取数据库名称", cx);
            return;
        };

        let tab_id = format!("table-data-{}.{}", database, table);

        let connection_id_for_error = connection_id.clone();
        let tab_container_clone = tab_container.clone();
        let database_string = database.clone();
        let table_string = table.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;

            if let Some(config) = config {
                let config_id = config.id.clone();
                let database_type = config.database_type;
                let tab_id_for_lazy = tab_id.clone();
                let database_for_lazy = database_string.clone();
                let table_for_lazy = table_string.clone();

                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            tab_container_clone.update(cx, |container, cx| {
                                let tab_id_clone = tab_id_for_lazy.clone();
                                let database_clone = database_for_lazy.clone();
                                let table_clone = table_for_lazy.clone();
                                let config_id_clone = config_id.clone();
                                container.activate_or_add_tab_lazy(
                                    tab_id_for_lazy.clone(),
                                    move |window, cx| {
                                        let table_data = TableDataTabContent::new(
                                            database_clone.clone(),
                                            table_clone.clone(),
                                            config_id_clone.clone(),
                                            database_type,
                                            window,
                                            cx,
                                        );
                                        TabItem::new(tab_id_clone.clone(), table_data)
                                    },
                                    window,
                                    cx,
                                );
                            });
                        });
                    }
                });
            } else {
                let connection_id_for_error = connection_id_for_error.clone();
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            Self::show_error(window, format!("打开表数据失败：无法获取连接配置 {}", connection_id_for_error), cx);
                        });
                    }
                });
            }
        }).detach();
    }

    /// 处理打开视图数据事件
    fn handle_open_view_data(
        node: DbNode,
        global_state: GlobalDbState,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::table_data_tab::TableDataTabContent;

        let connection_id = node.connection_id.clone();
        let view = node.name.clone();

        let Some(ref metadata) = node.metadata else {
            Self::show_error(window, "无效的节点数据", cx);
            return;
        };
        let Some(database) = metadata.get("database") else {
            Self::show_error(window, "无法获取数据库名称", cx);
            return;
        };

        let tab_id = format!("view-data-{}.{}", database, view);

        let connection_id_for_error = connection_id.clone();
        let tab_container_clone = tab_container.clone();
        let database_string = database.clone();
        let view_string = view.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;

            if let Some(config) = config {
                let config_id = config.id.clone();
                let database_type = config.database_type;
                let tab_id_for_lazy = tab_id.clone();
                let database_for_lazy = database_string.clone();
                let view_for_lazy = view_string.clone();

                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            tab_container_clone.update(cx, |container, cx| {
                                let tab_id_clone = tab_id_for_lazy.clone();
                                let database_clone = database_for_lazy.clone();
                                let view_clone = view_for_lazy.clone();
                                let config_id_clone = config_id.clone();
                                container.activate_or_add_tab_lazy(
                                    tab_id_for_lazy.clone(),
                                    move |window, cx| {
                                        let view_data = TableDataTabContent::new(
                                            database_clone.clone(),
                                            view_clone.clone(),
                                            config_id_clone.clone(),
                                            database_type,
                                            window,
                                            cx,
                                        );
                                        TabItem::new(tab_id_clone.clone(), view_data)
                                    },
                                    window,
                                    cx,
                                );
                            });
                        });
                    }
                });
            } else {
                let connection_id_for_error = connection_id_for_error.clone();
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            Self::show_error(window, format!("打开视图数据失败：无法获取连接配置 {}", connection_id_for_error), cx);
                        });
                    }
                });
            }
        }).detach();
    }

    /// 处理打开表结构事件
    fn handle_open_table_structure(
        node: DbNode,
        global_state: GlobalDbState,
        _tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {

        let connection_id = node.connection_id.clone();
        let table = node.name.clone();

        let Some(ref metadata) = node.metadata else {
            Self::show_error(window, "无效的节点数据", cx);
            return;
        };
        let Some(database) = metadata.get("database") else {
            Self::show_error(window, "无法获取数据库名称", cx);
            return;
        };

        let _tab_id = format!("table-designer-{}.{}", database, table);

        let connection_id_for_error = connection_id.clone();
        let database_string = database.clone();
        let table_string = table.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;

            if config.is_some() {
                let database_string = database_string.clone();
                let table_string = table_string.clone();
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            let _ = (&database_string, &table_string);
                            Self::show_warning(window, "表结构设计器功能尚未实现", cx);
                        });
                    }
                });
            } else {
                let connection_id_for_error = connection_id_for_error.clone();
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            Self::show_error(window, format!("打开表结构失败：无法获取连接配置 {}", connection_id_for_error), cx);
                        });
                    }
                });
            }
        }).detach();
    }

    /// 处理设计表事件（新建或编辑表结构）
    fn handle_design_table(
        node: DbNode,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let database_type = node.database_type;

        let (database_name, table_name) = match node.node_type {
            DbNodeType::TablesFolder => {
                let database = Self::get_database_from_node(&node);
                (database, None)
            }
            DbNodeType::Table => {
                let database = node.metadata
                    .as_ref()
                    .and_then(|m| m.get("database"))
                    .cloned()
                    .unwrap_or_else(|| Self::get_database_from_node(&node));
                (database, Some(node.name.clone()))
            }
            _ => return,
        };

        let tab_id = if let Some(ref table) = table_name {
            format!("table-designer-{}-{}", database_name, table)
        } else {
            format!("table-designer-{}-new-{}", database_name, Uuid::new_v4())
        };

        let tab_title = if let Some(ref table) = table_name {
            format!("设计表: {}", table)
        } else {
            "新建表".to_string()
        };

        let mut config = TableDesignerConfig::new(connection_id, database_name, database_type);
        if let Some(table) = table_name {
            config = config.with_table_name(table);
        }

        let tab_content = TableDesignerTabContent::new(tab_title, config, window, cx);

        tab_container.update(cx, |container, cx| {
            let tab = TabItem::new(tab_id, tab_content);
            container.add_and_activate_tab(tab, cx);
        });
    }

    /// 处理导入数据事件
    fn handle_import_data(
        node: DbNode,
        global_state: GlobalDbState,
        _window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();

        // 根据节点类型选择不同的导入视图
        if node.node_type == DbNodeType::Table {
            // 表节点：使用表导入视图（支持 TXT/CSV/JSON）
            use crate::import_export::table_import_view::TableImportView;

            let db = node.metadata.as_ref()
                .and_then(|m| m.get("database"))
                .cloned()
                .unwrap_or_else(|| node.parent_context.clone().unwrap_or_default());
            let table_name = node.name.clone();

            let connection_id_for_error = connection_id.clone();
            let db_string = db.clone();
            let table_string = table_name.clone();

            cx.spawn(async move |cx: &mut AsyncApp| {
                let config = global_state.get_config_async(&connection_id).await;

                if let Some(config) = config {
                    let config_id = config.id;
                    let db_for_view = db_string.clone();
                    let table_for_view = table_string.clone();

                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                let import_view = TableImportView::new(
                                    config_id.clone(),
                                    db_for_view.clone(),
                                    Some(table_for_view.clone()),
                                    window,
                                    cx,
                                );

                                window.open_dialog(cx, move |dialog, _window, _cx| {
                                    dialog
                                        .title("导入数据到表")
                                        .child(import_view.clone())
                                        .width(px(900.0))
                                        .on_cancel(|_, _window, _cx| true)
                                });
                            });
                        }
                    });
                } else {
                    let connection_id_for_error = connection_id_for_error.clone();
                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                Self::show_error(window, format!("导入数据失败：无法获取连接配置 {}", connection_id_for_error), cx);
                            });
                        }
                    });
                }
            }).detach();
        } else {
            // 数据库节点：使用原有的导入视图（支持 SQL）
            use crate::import_export::data_import_view::DataImportView;

            let database = node.name.clone();

            let connection_id_for_error = connection_id.clone();
            let database_string = database.clone();

            cx.spawn(async move |cx: &mut AsyncApp| {
                let config = global_state.get_config_async(&connection_id).await;

                if let Some(config) = config {
                    let config_id = config.id;
                    let database_for_view = database_string.clone();

                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                let import_view = DataImportView::new(
                                    config_id.clone(),
                                    database_for_view.clone(),
                                    window,
                                    cx,
                                );

                                window.open_dialog(cx, move |dialog, _window, _cx| {
                                    dialog
                                        .title("导入数据")
                                        .child(import_view.clone())
                                        .width(px(800.0))
                                        .on_cancel(|_, _window, _cx| true)
                                });
                            });
                        }
                    });
                } else {
                    let connection_id_for_error = connection_id_for_error.clone();
                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                Self::show_error(window, format!("导入数据失败：无法获取连接配置 {}", connection_id_for_error), cx);
                            });
                        }
                    });
                }
            }).detach();
        }
    }

    /// 处理导出数据事件
    fn handle_export_data(
        node: DbNode,
        global_state: GlobalDbState,
        _window: &mut Window,
        cx: &mut App,
    ) {
        use crate::import_export::data_export_view::DataExportView;
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database = Self::get_database_from_node(&node);
        let table_name = if node.node_type == DbNodeType::Table {
            Some(node.name.clone())
        } else {
            None
        };

        let connection_id_for_error = connection_id.clone();
        let database_string = database.clone();
        let table_name_option = table_name.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;

            if let Some(config) = config {
                let config_id = config.id;
                let database_for_view = database_string.clone();
                let table_name_for_view = table_name_option.clone();

                Self::with_window(cx, move |window, cx| {
                    let export_view = DataExportView::new(
                        config_id.clone(),
                        database_for_view.clone(),
                        window,
                        cx,
                    );

                    if let Some(table) = table_name_for_view.clone() {
                        export_view.update(cx, |view, cx| {
                            view.tables.update(cx, |state, cx| {
                                state.set_value(table, window, cx);
                            });
                        });
                    }

                    window.open_dialog(cx, move |dialog, _window, _cx| {
                        dialog
                            .title("Export Data")
                            .child(export_view.clone())
                            .width(px(800.0))
                            .on_cancel(|_, _window, _cx| true)
                    });
                }).await;
            } else {
                let _ = cx.update(|cx| {
                    Self::show_error_async(cx, format!("导出数据失败：无法获取连接配置 {}", connection_id_for_error));
                });
            }
        }).detach();
    }

    /// 处理关闭连接事件
    fn handle_close_connection(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let connection_name = node.name.clone();
        let tree_clone = tree_view.clone();
        let global_state = global_state.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let conn_name = connection_name.clone();
            let tree = tree_clone.clone();
            let global_state = global_state.clone();

            dialog
                .title("确认关闭连接")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要关闭连接 \"{}\" 吗？", conn_name))
                        .child("这将断开数据库连接并清理相关资源。")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let tree = tree.clone();
                    let global_state = global_state.clone();
                    cx.spawn(async move |cx: &mut AsyncApp| {
                        // 执行连接关闭逻辑
                        let result= global_state.disconnect_all(cx, conn_id.clone()).await;
                        match result {
                            Ok(_) => {
                                // 清理树视图节点状态并刷新
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree_view, cx| {
                                        tree_view.close_connection(&conn_id, cx);
                                    });
                                    Self::show_success_async(cx, "连接已成功关闭");
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("关闭连接失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理删除连接事件
    fn handle_delete_connection(
        node: DbNode,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use one_core::storage::traits::Repository;
        use one_core::storage::{ConnectionRepository, GlobalStorageState};

        let connection_id = node.connection_id.clone();
        let connection_name = node.name.clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let conn_name = connection_name.clone();
            let storage = storage_manager.clone();
            let tree = tree_view.clone();

            dialog
                .title("确认删除")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要删除连接 \"{}\" 吗？", conn_name))
                        .child("此操作不可恢复。")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let storage = storage.clone();
                    let tree = tree.clone();
                    cx.spawn(async move |cx| {
                        match conn_id.parse::<i64>() {
                            Ok(id) => {
                                if let Some(conn_repo_arc) = storage.get::<ConnectionRepository>().await {
                                    let conn_repo = (*conn_repo_arc).clone();
                                    match conn_repo.delete(id).await {
                                        Ok(_) => {
                                            // 刷新树
                                            let _ = cx.update(|cx| {
                                                tree.update(cx, |tree, cx| {
                                                    tree.refresh_tree(conn_id.clone(), cx);
                                                });
                                                Self::show_success_async(cx, "连接已成功删除");
                                            });
                                        }
                                        Err(e) => {
                                            let _ = cx.update(|cx| {
                                                Self::show_error_async(cx, format!("删除连接失败: {}", e));
                                            });
                                        }
                                    }
                                } else {
                                    let _ = cx.update(|cx| {
                                        Self::show_error_async(cx, "删除连接失败：无法获取存储库");
                                    });
                                }
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("删除连接失败：无效的连接ID {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理新建数据库事件
    fn handle_create_database(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database_type = node.database_type;

        let plugin_registry = cx.global::<DatabaseViewPluginRegistry>();
        let editor_view = if let Some(plugin) = plugin_registry.get(&database_type) {
            plugin.create_database_editor_view(connection_id.clone(), window, cx)
        } else {
            Self::show_error(window, format!("不支持的数据库类型: {:?}", database_type), cx);
            return;
        };

        let global_state_clone = global_state.clone();
        let connection_id_clone = connection_id.clone();
        let tree_view_clone = tree_view.clone();

        let editor_view_for_ok = editor_view.clone();
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor_view_ok = editor_view_for_ok.clone();
            let connection_id_for_ok = connection_id_clone.clone();
            let global_state_for_ok = global_state_clone.clone();
            let tree_view_for_ok = tree_view_clone.clone();

            dialog
                .title("创建数据库")
                .child(editor_view.clone())
                .width(px(700.0))
                .button_props(DialogButtonProps::default().ok_text("创建"))
                .footer(|ok, cancel, window, cx| {
                    vec![cancel(window, cx), ok(window, cx)]
                })
                .on_ok(move |_, _window, cx| {
                    let sql = editor_view_ok.read(cx).get_sql(cx);
                    if sql.trim().is_empty() {
                        editor_view_ok.update(cx, |view, cx| {
                            view.set_save_error("SQL 语句不能为空".to_string(), cx);
                        });
                        return false;
                    }

                    let connection_id = connection_id_for_ok.clone();
                    let global_state = global_state_for_ok.clone();
                    let tree_view = tree_view_for_ok.clone();
                    let editor_view = editor_view_ok.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let result = global_state.execute_single(
                            cx,
                            connection_id.clone(),
                            sql,
                            None,
                            None,
                        ).await;

                        match result {
                            Ok(sql_result) => {
                                match sql_result {
                                    SqlResult::Query(_) => {}
                                    SqlResult::Exec(_) => {
                                        if let Some(window_id) = cx.update(|cx| cx.active_window()).ok().flatten() {
                                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                                window.close_dialog(cx);
                                                tree_view.update(cx, |tree, cx| {
                                                    tree.refresh_tree(connection_id.clone(), cx);
                                                });
                                                window.push_notification(
                                                    Notification::success("数据库创建成功").autohide(true),
                                                    cx
                                                );
                                            });
                                        }
                                    }
                                    SqlResult::Error(err) => {
                                        let _ = editor_view.update(cx, |view, cx| {
                                            view.set_save_error(format!("创建数据库失败: {}", err.message), cx);
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = editor_view.update(cx, |view, cx| {
                                    view.set_save_error(format!("创建数据库失败: {}", e), cx);
                                });
                            }
                        }
                    }).detach();

                    false
                })
                .on_cancel(|_, _window, _cx| true)
        });
    }

    /// 处理编辑数据库事件
    fn handle_edit_database(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();
        let database_type = node.database_type;

        let plugin_registry = cx.global::<DatabaseViewPluginRegistry>();
        let editor_view = if let Some(plugin) = plugin_registry.get(&database_type) {
            plugin.create_database_editor_view_for_edit(
                connection_id.clone(),
                database_name.clone(),
                window,
                cx,
            )
        } else {
            Self::show_error(window, format!("不支持的数据库类型: {:?}", database_type), cx);
            return;
        };

        let global_state_clone = global_state.clone();
        let connection_id_clone = connection_id.clone();
        let tree_view_clone = tree_view.clone();

        let editor_view_for_ok = editor_view.clone();
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor_view_ok = editor_view_for_ok.clone();
            let connection_id_for_ok = connection_id_clone.clone();
            let global_state_for_ok = global_state_clone.clone();
            let tree_view_for_ok = tree_view_clone.clone();

            dialog
                .title(format!("编辑数据库: {}", database_name))
                .child(editor_view.clone())
                .width(px(700.0))
                .button_props(DialogButtonProps::default().ok_text("保存"))
                .footer(|ok, cancel, window, cx| {
                    vec![cancel(window, cx), ok(window, cx)]
                })
                .on_ok(move |_, _window, cx| {
                    let sql = editor_view_ok.read(cx).get_sql(cx);
                    if sql.trim().is_empty() {
                        editor_view_ok.update(cx, |view, cx| {
                            view.set_save_error("SQL 语句不能为空".to_string(), cx);
                        });
                        return false;
                    }

                    let connection_id = connection_id_for_ok.clone();
                    let global_state = global_state_for_ok.clone();
                    let tree_view = tree_view_for_ok.clone();
                    let editor_view = editor_view_ok.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let result = global_state.execute_single(
                            cx,
                            connection_id.clone(),
                            sql,
                            None,
                            None,
                        ).await;

                        match result {
                            Ok(sql_result) => {
                                match sql_result {
                                    SqlResult::Query(_) => {}
                                    SqlResult::Exec(_) => {
                                        if let Some(window_id) = cx.update(|cx| cx.active_window()).ok().flatten() {
                                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                                window.close_dialog(cx);
                                                tree_view.update(cx, |tree, cx| {
                                                    tree.refresh_tree(connection_id.clone(), cx);
                                                });
                                                window.push_notification(
                                                    Notification::success("数据库修改成功").autohide(true),
                                                    cx
                                                );
                                            });
                                        }
                                    }
                                    SqlResult::Error(err) => {
                                        let _ = editor_view.update(cx, |view, cx| {
                                            view.set_save_error(format!("修改数据库失败: {}", err.message), cx);
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = editor_view.update(cx, |view, cx| {
                                    view.set_save_error(format!("修改数据库失败: {}", e), cx);
                                });
                            }
                        }
                    }).detach();

                    false
                })
                .on_cancel(|_, _window, _cx| true)
        });
    }


    /// 处理关闭数据库事件
    fn handle_close_database(
        node: DbNode,
        _global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();

        let tree_clone = tree_view.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let db_name = database_name.clone();
            let tree = tree_clone.clone();

            dialog
                .title("确认关闭数据库")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要关闭数据库 \"{}\" 吗？", db_name))
                        .child("这将收起数据库节点并清理相关状态。")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let db_name = db_name.clone();
                    let db_name_log = db_name.clone();
                    let tree = tree.clone();
                    let db_node_id = format!("{}:{}", conn_id, db_name);

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        // 执行数据库关闭逻辑
                        let task_result = Tokio::spawn_result(cx, async move {
                            // 这里可以添加实际的数据库关闭逻辑
                            // 比如执行 USE mysql 切换到系统数据库
                            Ok(())
                        });

                        let task = match task_result {
                            Ok(t) => t,
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("启动关闭任务失败: {}", e));
                                });
                                return;
                            }
                        };

                        let result = task.await;

                        match result {
                            Ok(_) => {
                                // 收起数据库节点并清理状态
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree_view, cx| {
                                        tree_view.close_database(&db_node_id, cx);
                                    });
                                    Self::show_success_async(cx, format!("数据库 {} 已关闭", db_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("关闭数据库失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理删除数据库事件
    fn handle_delete_database(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let db_name = database_name.clone();
            let state = global_state.clone();
            let db_name_display = database_name.clone();
            let tree = tree_view.clone();

            dialog
                .title("确认删除")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要删除数据库 \"{}\" 吗？", db_name_display))
                        .child("此操作将删除数据库中的所有数据，不可恢复！")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let db_name = db_name.clone();
                    let db_name_log = db_name.clone();
                    let tree = tree.clone();
                    let state = state.clone();
                    let conn_id_for_refresh = conn_id.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let result = state.drop_database(cx, conn_id.clone(), db_name.clone()).await;
                        match result {
                            Ok(_) => {
                                // 刷新父节点（连接节点）
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(conn_id_for_refresh, cx);
                                    });
                                    Self::show_success_async(cx, format!("数据库 {} 已删除", db_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("删除数据库失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理新建模式事件
    fn handle_create_schema(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();
        let database_type = node.database_type;

        let plugin_registry = cx.global::<DatabaseViewPluginRegistry>();
        let editor_view = if let Some(plugin) = plugin_registry.get(&database_type) {
            if let Some(view) = plugin.create_schema_editor_view(connection_id.clone(), database_name.clone(), window, cx) {
                view
            } else {
                Self::show_error(window, format!("该数据库类型不支持创建模式: {:?}", database_type), cx);
                return;
            }
        } else {
            Self::show_error(window, format!("不支持的数据库类型: {:?}", database_type), cx);
            return;
        };

        let global_state_clone = global_state.clone();
        let connection_id_clone = connection_id.clone();
        let tree_view_clone = tree_view.clone();
        let database_name_clone = database_name.clone();

        let editor_view_for_ok = editor_view.clone();
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor_view_ok = editor_view_for_ok.clone();
            let connection_id_for_ok = connection_id_clone.clone();
            let global_state_for_ok = global_state_clone.clone();
            let tree_view_for_ok = tree_view_clone.clone();
            let database_for_ok = database_name_clone.clone();

            dialog
                .title(format!("新建模式 - {}", database_name))
                .child(editor_view.clone())
                .width(px(600.0))
                .button_props(DialogButtonProps::default().ok_text("创建"))
                .footer(|ok, cancel, window, cx| {
                    vec![cancel(window, cx), ok(window, cx)]
                })
                .on_ok(move |_, _window, cx| {
                    let sql = editor_view_ok.read(cx).get_sql(cx);
                    if sql.trim().is_empty() {
                        editor_view_ok.update(cx, |view, cx| {
                            view.set_save_error("SQL 语句不能为空".to_string(), cx);
                        });
                        return false;
                    }

                    let connection_id = connection_id_for_ok.clone();
                    let global_state = global_state_for_ok.clone();
                    let tree_view = tree_view_for_ok.clone();
                    let database = database_for_ok.clone();
                    let editor_view = editor_view_ok.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let result = global_state.execute_single(
                            cx,
                            connection_id.clone(),
                            sql,
                            Some(database.clone()),
                            None,
                        ).await;

                        match result {
                            Ok(sql_result) => {
                                match sql_result {
                                    SqlResult::Query(_) => {}
                                    SqlResult::Exec(_) => {
                                        let db_node_id = format!("{}:{}", connection_id, database);
                                        if let Some(window_id) = cx.update(|cx| cx.active_window()).ok().flatten() {
                                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                                window.close_dialog(cx);
                                                tree_view.update(cx, |tree, cx| {
                                                    tree.refresh_tree(db_node_id, cx);
                                                });
                                                window.push_notification(
                                                    Notification::success("模式创建成功").autohide(true),
                                                    cx
                                                );
                                            });
                                        }
                                    }
                                    SqlResult::Error(err) => {
                                        let _ = editor_view.update(cx, |view, cx| {
                                            view.set_save_error(format!("创建模式失败: {}", err.message), cx);
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = editor_view.update(cx, |view, cx| {
                                    view.set_save_error(format!("创建模式失败: {}", e), cx);
                                });
                            }
                        }
                    }).detach();

                    false
                })
                .on_cancel(|_, _window, _cx| true)
        });
    }

    /// 处理删除模式事件
    fn handle_delete_schema(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let schema_name = node.name.clone();
        let metadata = node.metadata.clone();
        let database_type = node.database_type;

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let schema = schema_name.clone();
            let meta = metadata.clone();
            let state = global_state.clone();
            let schema_display = schema_name.clone();
            let tree = tree_view.clone();

            dialog
                .title("确认删除")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要删除模式 \"{}\" 吗？", schema_display))
                        .child("此操作将删除模式中的所有对象，不可恢复！")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let schema = schema.clone();
                    let meta = meta.clone();
                    let state = state.clone();
                    let schema_log = schema.clone();
                    let tree = tree.clone();
                    let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();
                    let db_node_id = format!("{}:{}", conn_id, database);

                    let sql = state.get_plugin(&database_type)
                        .map(|p| p.build_drop_schema_sql(&schema))
                        .unwrap_or_else(|_| format!("DROP SCHEMA \"{}\"", schema));

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let result = state.execute_single(
                            cx,
                            conn_id.clone(),
                            sql,
                            Some(database.clone()),
                            None,
                        ).await;

                        match result {
                            Ok(sql_result) => {
                                match sql_result {
                                    SqlResult::Query(_) => {}
                                    SqlResult::Exec(_) => {
                                        let _ = cx.update(|cx| {
                                            tree.update(cx, |tree, cx| {
                                                tree.refresh_tree(db_node_id, cx);
                                            });
                                            Self::show_success_async(cx, format!("模式 {} 已删除", schema_log));
                                        });
                                    }
                                    SqlResult::Error(err) => {
                                        let _ = cx.update(|cx| {
                                            Self::show_error_async(cx, format!("删除模式失败: {}", err.message));
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("删除模式失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理删除表事件
    fn handle_delete_table(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let table_name = node.name.clone();
        let metadata = node.metadata.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let tbl_name = table_name.clone();
            let meta = metadata.clone();
            let state = global_state.clone();
            let tbl_name_display = table_name.clone();
            let tree = tree_view.clone();

            dialog
                .title("确认删除")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要删除表 \"{}\" 吗？", tbl_name_display))
                        .child("此操作将删除表中的所有数据，不可恢复！")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let tbl_name = tbl_name.clone();
                    let meta = meta.clone();
                    let state = state.clone();
                    let tbl_name_log = tbl_name.clone();
                    let tree = tree.clone();
                    let db_node_id = format!("{}:{}", conn_id, meta.as_ref().and_then(|m| m.get("database")).unwrap_or(&String::new()));

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();
                        let task = state.drop_table(cx, conn_id.clone(), database, tbl_name.clone()).await;
                        
                        match task {
                            Ok(_) => {
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                    Self::show_success_async(cx, format!("表 {} 已删除", tbl_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("删除表失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理重命名表事件
    fn handle_rename_table(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::{input::{Input, InputState}, WindowExt};

        let connection_id = node.connection_id.clone();
        let old_table_name = node.name.clone();
        let metadata = node.metadata.clone();

        // 创建输入框状态
        let input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder("输入新表名");
            state.set_value(old_table_name.clone(), window, cx);
            state
        });

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let old_name = old_table_name.clone();
            let meta = metadata.clone();
            let state = global_state.clone();
            let input = input_state.clone();
            let tree = tree_view.clone();

            dialog
                .title("重命名表")
                .confirm()
                .child(
                    v_flex()
                        .gap_4()
                        .p_4()
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(
                                    div()
                                        .w(px(80.))
                                        .child("原表名:")
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(old_name.clone())
                                )
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(
                                    div()
                                        .w(px(80.))
                                        .child("新表名:")
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Input::new(&input))
                                )
                        )
                )
                .on_ok(move |_, _, cx| {
                    let new_name = input.read(cx).text().to_string().trim().to_string();
                    if new_name.is_empty() || new_name == old_name {
                        return false; // 不关闭对话框
                    }

                    let conn_id = conn_id.clone();
                    let old_name = old_name.clone();
                    let meta = meta.clone();
                    let state = state.clone();
                    let tree = tree.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let old_name_log = old_name.clone();
                        let new_name_log = new_name.clone();
                        let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();
                        let db_node_id = format!("{}:{}", conn_id, database);

                        let task = state.rename_table(cx, conn_id.clone(), database, old_name.clone(), new_name.clone()).await;
                        match task {
                            Ok(_) => {
                                let _ = cx.update(|cx| {
                                    // 刷新数据库节点以显示新表名
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                    Self::show_success_async(cx, format!("表已重命名: {} -> {}", old_name_log, new_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("重命名表失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理清空表事件
    fn handle_truncate_table(
        node: DbNode,
        global_state: GlobalDbState,
        _tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let table_name = node.name.clone();
        let metadata = node.metadata.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let tbl_name = table_name.clone();
            let meta = metadata.clone();
            let state = global_state.clone();
            let tbl_name_display = table_name.clone();

            dialog
                .title("确认清空")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要清空表 \"{}\" 吗？", tbl_name_display))
                        .child("此操作将删除表中的所有数据，但保留表结构，不可恢复！")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let tbl_name = tbl_name.clone();
                    let meta = meta.clone();
                    let state = state.clone();
                    let tbl_name_log = tbl_name.clone();

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();
                        let task = state.truncate_table(cx, conn_id.clone(), database, tbl_name.clone()).await;
                        
                        match task {
                            Ok(_) => {
                                let _ = cx.update(|cx| {
                                    Self::show_success_async(cx, format!("表 {} 已清空", tbl_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("清空表失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理删除视图事件
    fn handle_delete_view(
        node: DbNode,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let view_name = node.name.clone();
        let metadata = node.metadata.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let conn_id = connection_id.clone();
            let v_name = view_name.clone();
            let meta = metadata.clone();
            let state = global_state.clone();
            let v_name_display = view_name.clone();
            let tree = tree_view.clone();

            dialog
                .title("确认删除")
                .confirm()
                .child(
                    v_flex()
                        .gap_2()
                        .child(format!("确定要删除视图 \"{}\" 吗？", v_name_display))
                        .child("此操作不可恢复。")
                )
                .on_ok(move |_, _, cx| {
                    let conn_id = conn_id.clone();
                    let v_name = v_name.clone();
                    let meta = meta.clone();
                    let state = state.clone();
                    let v_name_log = v_name.clone();
                    let tree = tree.clone();
                    let db_node_id = format!("{}:{}", conn_id, meta.as_ref().and_then(|m| m.get("database")).unwrap_or(&String::new()));

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();
                        let result = state.drop_view(cx, conn_id.clone(), database, v_name.clone()).await;
                        
                        match result {
                            Ok(_) => {
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                    Self::show_success_async(cx, format!("视图 {} 已删除", v_name_log));
                                });
                            }
                            Err(e) => {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, format!("删除视图失败: {}", e));
                                });
                            }
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理打开命名查询事件
    fn handle_open_named_query(
        node: DbNode,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let query_id = node.metadata.as_ref()
            .and_then(|m| m.get("query_id"))
            .and_then(|id| id.parse::<i64>().ok());

        if let Some(qid) = query_id {
            let connection_id = node.connection_id.clone();
            let query_name = node.name.clone();
            let tab_id = format!("query-{}", qid);

            tab_container.update(cx, |container, cx| {
                container.activate_or_add_tab_lazy(
                    tab_id.clone(),
                    move |window, cx| {
                        let sql_editor = SqlEditorTabContent::new_with_query_id(
                            qid,
                            query_name.clone(),
                            connection_id.clone(),
                            window,
                            cx,
                        );
                        TabItem::new(tab_id.clone(), sql_editor)
                    },
                    window,
                    cx,
                );
            });
        }
    }

    /// 处理重命名查询事件
    fn handle_rename_query(
        node: DbNode,
        db_tree: Entity<DbTreeView>,
        _global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use one_core::storage::traits::Repository;
        use one_core::storage::{GlobalStorageState, query_repository::QueryRepository};
        use gpui_component::{input::{Input, InputState}, WindowExt};

        let query_id = node.metadata.as_ref()
            .and_then(|m| m.get("query_id"))
            .and_then(|id| id.parse::<i64>().ok());

        if let Some(qid) = query_id {
            let old_query_name = node.name.clone();
            let storage_manager = cx.global::<GlobalStorageState>().storage.clone();

            // 创建输入框状态
            let input_state = cx.new(|cx| {
                let mut state = InputState::new(window, cx)
                    .placeholder("输入新查询名");
                state.set_value(old_query_name.clone(), window, cx);
                state
            });
            let clone_db_tree = db_tree.clone();
            let node_id = node.id.clone();

            window.open_dialog(cx, move |dialog, _window, _cx| {
                let old_name = old_query_name.clone();
                let storage = storage_manager.clone();
                let input = input_state.clone();
                let db_tree = clone_db_tree.clone();
                let node_id = node_id.clone();

                dialog
                    .title("重命名查询")
                    .confirm()
                    .child(
                        v_flex()
                            .gap_4()
                            .p_4()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        div()
                                            .w(px(80.))
                                            .child("原名称:")
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .child(old_name.clone())
                                    )
                            )
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        div()
                                            .w(px(80.))
                                            .child("新名称:")
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .child(Input::new(&input))
                                    )
                            )
                    )
                    .on_ok(move |_, _, cx| {
                        let new_name = input.read(cx).text().to_string().trim().to_string();
                        let old_name_check = old_name.clone();
                        if new_name.is_empty() || new_name == old_name_check {
                            return false; // 不关闭对话框
                        }

                        let storage = storage.clone();
                        let new_name_log = new_name.clone();
                        let db_tree = db_tree.clone();
                        let node_id = node_id.clone();
                        cx.spawn(async move |cx: &mut AsyncApp| {

                            let result = async  {
                                Tokio::spawn_result(cx, async move {
                                    let repo = storage.get::<QueryRepository>().await
                                        .ok_or_else(|| anyhow::anyhow!("QueryRepository not found"))?;
                                    let result: Option<Query> = repo.get(qid).await?;
                                    if let Some(mut query) = result {
                                        query.name = new_name_log.clone();
                                        repo.update(&query).await?;
                                    }
                                    Ok(())
                                })?.await
                            }.await;

                            if let Err(e) = result {
                                error!("{}", e);
                                _ = cx.update(|cx| {
                                    Self::show_error_async(cx, e.to_string());
                                })
                            } else {
                                _ = cx.update(|cx| {
                                    db_tree.update(cx, |db, cx| {
                                        db.refresh_tree(node_id, cx);
                                    });
                                    Self::show_success_async(cx, "查询已重命名");
                                });
                            }
                        }).detach();
                        true
                    })
            });
        }
    }

    /// 处理删除查询事件
    fn handle_delete_query(
        node: DbNode,
        tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use one_core::storage::traits::Repository;
        use one_core::storage::{GlobalStorageState, query_repository::QueryRepository};

        let query_id = node.metadata.as_ref()
            .and_then(|m| m.get("query_id"))
            .and_then(|id| id.parse::<i64>().ok());

        if let Some(qid) = query_id {
            let query_name = node.name.clone();
            let connection_id = node.connection_id.clone();
            let storage_manager = cx.global::<GlobalStorageState>().storage.clone();

            window.open_dialog(cx, move |dialog, _window, _cx| {
                let q_name = query_name.clone();
                let storage = storage_manager.clone();
                let tree = tree_view.clone();
                let conn_id = connection_id.clone();

                dialog
                    .title("确认删除")
                    .confirm()
                    .child(
                        v_flex()
                            .gap_2()
                            .child(format!("确定要删除查询 \"{}\" 吗？", q_name))
                            .child("此操作不可恢复。")
                    )
                    .on_ok(move |_, _, cx| {
                        let storage = storage.clone();
                        let tree = tree.clone();
                        let conn_id = conn_id.clone();

                        cx.spawn(async move |cx| {
                            if let Some(query_repo_arc) = storage.get::<QueryRepository>().await {
                                let query_repo = (*query_repo_arc).clone();
                                match query_repo.delete(qid).await {
                                    Ok(_) => {
                                        // 刷新树
                                        let _ = cx.update(|cx| {
                                            tree.update(cx, |tree, cx| {
                                                tree.refresh_tree(conn_id.clone(), cx);
                                            });
                                            Self::show_success_async(cx, "查询已删除");
                                        });
                                    }
                                    Err(e) => {
                                        let _ = cx.update(|cx| {
                                            Self::show_error_async(cx, format!("删除查询失败: {}", e));
                                        });
                                    }
                                }
                            } else {
                                let _ = cx.update(|cx| {
                                    Self::show_error_async(cx, "删除查询失败：无法获取存储库");
                                });
                            }
                        }).detach();
                        true
                    })
            });
        }
    }

    /// 处理运行SQL文件事件
    fn handle_run_sql_file(
        node: DbNode,
        _global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_run_view::SqlRunView;

        let connection_id = node.connection_id.clone();
        let database = if node.node_type == DbNodeType::Database {
            Some(node.name.clone())
        } else {
            None
        };

        let run_view = SqlRunView::new(connection_id, database, window, cx);
        window.open_dialog(cx, move |dialog, _window, _cx| {
            dialog
                .title("运行SQL文件")
                .child(run_view.clone())
                .width(px(800.0))
                .on_cancel(|_, _window, _cx| true)
        });
    }

    /// 处理转储SQL文件事件
    fn handle_dump_sql_file(
        node: DbNode,
        mode: crate::db_tree_view::SqlDumpMode,
        global_state: GlobalDbState,
        _window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_dump_view::SqlDumpView;

        let connection_id = node.connection_id.clone();
        let database = node.name.clone();

        let connection_id_for_error = connection_id.clone();
        let database_string = database.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;

            if let Some(config) = config {
                let config_id = config.id;
                let database_for_view = database_string.clone();

                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            let dump_view = SqlDumpView::new(config_id.clone(), database_for_view.clone(), mode, window, cx);

                            window.open_dialog(cx, move |dialog, _window, _cx| {
                                dialog
                                    .title("转储SQL文件")
                                    .child(dump_view.clone())
                                    .width(px(800.0))
                                    .on_cancel(|_, _window, _cx| true)
                            });
                        });
                    }
                });
            } else {
                let connection_id_for_error = connection_id_for_error.clone();
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            Self::show_error(window, format!("转储SQL文件失败：无法获取连接配置 {}", connection_id_for_error), cx);
                        });
                    }
                });
            }
        }).detach();
    }
}