use one_core::gpui_tokio::Tokio;
use gpui::{div, px, App, AppContext, AsyncApp, Context, Entity, ParentElement, Styled, Subscription, Window};
use tracing::log::warn;
use uuid::Uuid;
use db::{DbNode, DbNodeType, GlobalDbState};
use gpui_component::{h_flex, v_flex, WindowExt};
use one_core::tab_container::{TabContainer, TabItem};
use crate::{database_objects_tab::DatabaseObjectsPanel, db_tree_view::{DbTreeView, DbTreeViewEvent}};
use crate::sql_editor_view::SqlEditorTabContent;





// Event handler for database tree view events
pub struct DatabaseEventHandler {
    _tree_subscription: Subscription,
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
                tree_view.read(cx).get_node(node_id).cloned()
            };

            match event {
                DbTreeViewEvent::NodeSelected { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_node_selected(node, global_state, objects_panel, cx);
                    } else {
                        warn!("NodeSelected event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::CreateNewQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_new_query(node, tab_container, tree_view.clone(), window, cx);
                    } else {
                        warn!("CreateNewQuery event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::OpenTableData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_table_data(node, global_state, tab_container, window, cx);
                    } else {
                        warn!("OpenTableData event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::OpenViewData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_view_data(node, global_state, tab_container, window, cx);
                    } else {
                        warn!("OpenViewData event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::OpenTableStructure { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_table_structure(node, global_state, tab_container, window, cx);
                    } else {
                        warn!("OpenTableStructure event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::ImportData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_import_data(node, global_state, window, cx);
                    } else {
                        warn!("ImportData event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::ExportData { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_export_data(node, global_state, window, cx);
                    } else {
                        warn!("ExportData event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::CloseConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_connection(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("CloseConnection event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DeleteConnection { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_connection(node, tree_view.clone(), window, cx);
                    } else {
                        warn!("DeleteConnection event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::CreateDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_create_database(node, global_state, window, cx);
                    } else {
                        warn!("EditDatabase event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::EditDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_edit_database(node, global_state, window, cx);
                    } else {
                        warn!("EditDatabase event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::CloseDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_close_database(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("CloseDatabase event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DeleteDatabase { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_database(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("DeleteDatabase event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DeleteTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_table(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("DeleteTable event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::RenameTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_rename_table(node, global_state, window, cx);
                    } else {
                        warn!("RenameTable event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::TruncateTable { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_truncate_table(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("TruncateTable event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DeleteView { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_view(node, global_state, tree_view.clone(), window, cx);
                    } else {
                        warn!("DeleteView event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::OpenNamedQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_open_named_query(node, tab_container, window, cx);
                    } else {
                        warn!("OpenNamedQuery event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::RenameQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_rename_query(node, global_state, window, cx);
                    } else {
                        warn!("RenameQuery event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DeleteQuery { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_delete_query(node, tree_view.clone(), window, cx);
                    } else {
                        warn!("DeleteQuery event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::RunSqlFile { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_run_sql_file(node, global_state, window, cx);
                    } else {
                        warn!("RunSqlFile event with missing node: {}", node_id);
                    }
                }
                DbTreeViewEvent::DumpSqlFile { node_id } => {
                    if let Some(node) = get_node(&node_id, cx) {
                        Self::handle_dump_sql_file(node, global_state, window, cx);
                    } else {
                        warn!("DumpSqlFile event with missing node: {}", node_id);
                    }
                }
            }
        });

        Self {
            _tree_subscription: tree_subscription,
        }
    }

    /// 处理节点选中事件
    fn handle_node_selected(
        node: DbNode,
        global_state: GlobalDbState,
        objects_panel: Entity<DatabaseObjectsPanel>,
        cx: &mut App,
    ) {
        // 当连接节点未连接时显示连接列表信息
        if node.node_type == DbNodeType::Connection && !node.children_loaded {
            use one_core::storage::traits::Repository;
            use one_core::storage::{ConnectionRepository, GlobalStorageState, WorkspaceRepository};

            let connection_id = node.connection_id.clone();
            let storage_manager = cx.global::<GlobalStorageState>().storage.clone();

            let result = Tokio::block_on(cx, async move {
                let pool = storage_manager.get_pool().await.ok()?;

                // 获取当前连接的信息
                let conn_id = connection_id.parse::<i64>().ok()?;
                let conn_repo_arc = storage_manager.get::<ConnectionRepository>().await?;
                let conn_repo = (*conn_repo_arc).clone();
                let current_conn = conn_repo.get(&pool, conn_id).await.ok()??;
                let workspace_id = current_conn.workspace_id;

                // 获取工作区名称
                let workspace_name = if let Some(ws_id) = workspace_id {
                    let workspace_repo_arc = storage_manager.get::<WorkspaceRepository>().await?;
                    let workspace_repo = (*workspace_repo_arc).clone();
                    workspace_repo
                        .get(&pool, ws_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|ws| ws.name)
                } else {
                    None
                };

                // 获取同工作区的所有连接
                let connections = conn_repo.list_by_workspace(&pool, workspace_id).await.ok()?;

                Some((connections, workspace_name))
            });

            if let Some((connections, workspace_name)) = result {
                objects_panel.update(cx, |panel, cx| {
                    panel.show_connection_list(connections, workspace_name, cx);
                });
            }

            return;
        }

        let connection_id = node.connection_id.clone();
        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;
            if let Some(config) = config {
                _ = objects_panel.update(cx, |panel, cx| {
                    panel.handle_node_selected(node, config, cx);
                });
            }
        }).detach();

    }

    /// 处理创建新查询事件
    fn handle_create_new_query(
        node: DbNode,
        tab_container: Entity<TabContainer>,
        db_tree_view: Entity<DbTreeView>,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_editor_view::SqlEditorTabContent;

        let connection_id = node.connection_id.clone();

        // 获取数据库名：
        // 1. 如果是数据库节点，直接使用 node.name
        // 2. 如果是 QueriesFolder 或其他节点，从 metadata 中获取 database
        // 3. 如果 metadata 没有，尝试从 parent_context 解析
        let database = if node.node_type == db::DbNodeType::Database {
            node.name.clone()
        } else if let Some(metadata) = &node.metadata {
            metadata.get("database").cloned().unwrap_or_else(|| {
                // 从 parent_context 解析数据库名
                // parent_context 格式: "connection_id:database_name"
                if let Some(parent) = &node.parent_context {
                    parent.split(':').nth(1).unwrap_or("").to_string()
                } else {
                    "".to_string()
                }
            })
        } else if let Some(parent) = &node.parent_context {
            // 从 parent_context 解析数据库名
            parent.split(':').nth(1).unwrap_or("").to_string()
        } else {
            "".to_string()
        };

        let sql_editor = SqlEditorTabContent::new_with_config(
            format!("{} - Query", if database.is_empty() { "New Query" } else { &database }),
            connection_id,
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
        let metadata = &node.metadata.unwrap();
        let database = metadata.get("database").unwrap();
        let tab_id = format!("table-data-{}.{}", database, table);

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });
        if let Some(config) = config {
            let database_clone = database.clone();
            let table_clone = table.clone();
            let config_id = config.id.clone();
            let tab_id_clone = tab_id.clone();

            tab_container.update(cx, |container, cx| {
                container.activate_or_add_tab_lazy(
                    tab_id,
                    move |window, cx| {
                        let table_data = TableDataTabContent::new(
                            database_clone,
                            table_clone,
                            config_id,
                            config.database_type,
                            window,
                            cx,
                        );
                        TabItem::new(tab_id_clone, table_data)
                    },
                    window,
                    cx,
                );
            });
        }
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
        let metadata = &node.metadata.unwrap();
        let database = metadata.get("database").unwrap();
        let tab_id = format!("view-data-{}.{}", database, view);

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let database_clone = database.clone();
            let view_clone = view.clone();
            let config_id = config.id.clone();
            let tab_id_clone = tab_id.clone();

            tab_container.update(cx, |container, cx| {
                container.activate_or_add_tab_lazy(
                    tab_id,
                    move |window, cx| {
                        let view_data = TableDataTabContent::new(
                            database_clone,
                            view_clone,
                            config_id,
                            config.database_type,
                            window,
                            cx,
                        );
                        TabItem::new(tab_id_clone, view_data)
                    },
                    window,
                    cx,
                );
            });
        }
    }

    /// 处理打开表结构事件
    fn handle_open_table_structure(
        node: DbNode,
        global_state: GlobalDbState,
        tab_container: Entity<TabContainer>,
        window: &mut Window,
        cx: &mut App,
    ) {

        let connection_id = node.connection_id.clone();
        let table = node.name.clone();
        let metadata = &node.metadata.unwrap();
        let database = metadata.get("database").unwrap();
        let tab_id = format!("table-designer-{}.{}", database, table);

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let database_clone = database.clone();
            let table_clone = table.clone();
            let config_id = config.id.clone();
            let database_type = config.database_type;
            let tab_id_clone = tab_id.clone();

            // tab_container.update(cx, |container, cx| {
            //     container.activate_or_add_tab_lazy(
            //         tab_id,
            //         move |window, cx| {
            //             let table_designer = TableDesignerView::edit_table(
            //                 database_clone,
            //                 table_clone,
            //                 config_id,
            //                 database_type,
            //                 window,
            //                 cx,
            //             );
            //             TabItem::new(tab_id_clone, table_designer.read(cx).clone())
            //         },
            //         window,
            //         cx,
            //     );
            // });
        }
    }

    /// 处理导入数据事件
    fn handle_import_data(
        node: DbNode,
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();

        // 根据节点类型选择不同的导入视图
        if node.node_type == db::DbNodeType::Table {
            // 表节点：使用表导入视图（支持 TXT/CSV/JSON）
            use crate::table_import_view::TableImportView;

            let db = node.metadata.as_ref()
                .and_then(|m| m.get("database"))
                .cloned()
                .unwrap_or_else(|| node.parent_context.clone().unwrap_or_default());
            let table_name = node.name.clone();

            let config = Tokio::block_on(cx, async move {
                global_state.get_config_async(&connection_id).await
            });

            if let Some(config) = config {
                let import_view = TableImportView::new(
                    config.id,
                    db,
                    Some(table_name),
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
            }
        } else {
            // 数据库节点：使用原有的导入视图（支持 SQL）
            use crate::data_import_view::DataImportView;

            let database = node.name.clone();

            let config = Tokio::block_on(cx, async move {
                global_state.get_config_async(&connection_id).await
            });

            if let Some(config) = config {
                let import_view = DataImportView::new(
                    config.id,
                    database,
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
            }
        }
    }

    /// 处理导出数据事件
    fn handle_export_data(
        node: DbNode,
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::data_export_view::DataExportView;
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        // 获取数据库名：如果是数据库节点则用 name，否则用 parent_context
        let database = node.parent_context.clone().unwrap_or_else(|| node.name.clone());
        // 如果是表节点，预填表名
        let table_name = if node.node_type == db::DbNodeType::Table {
            Some(node.name.clone())
        } else {
            None
        };

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let export_view = DataExportView::new(
                config.id,
                database.clone(),
                window,
                cx,
            );

            // 如果有表名则预填
            if let Some(table) = table_name {
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
        }
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
                            Ok(task) => {
                                // 清理树视图节点状态并刷新
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree_view, cx| {
                                        tree_view.close_connection(&conn_id, cx);
                                    });
                                });

                            }
                            Err(e) => {
                                eprintln!("Failed to close connection: {}", e);
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
                        if let Ok(pool) = storage.get_pool().await {
                            if let Ok(id) = conn_id.parse::<i64>() {
                                if let Some(conn_repo_arc) = storage.get::<ConnectionRepository>().await {
                                    let conn_repo = (*conn_repo_arc).clone();
                                    let _ = conn_repo.delete(&pool, id).await;
                                    eprintln!("Connection deleted: {}", conn_id);

                                    // 刷新树
                                    let _ = cx.update(|cx| {
                                        tree.update(cx, |tree, cx| {
                                            tree.refresh_tree(conn_id.clone(), cx);
                                        });
                                    });
                                }
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
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::database_view_plugin::DatabaseViewPluginRegistry;
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let db_type = config.database_type;
            let registry = DatabaseViewPluginRegistry::new();
            
            if let Some(plugin) = registry.get(&db_type) {
                let form = plugin.create_database_form(window, cx);
                
                window.open_dialog(cx, move |dialog, _window, _cx| {
                    dialog
                        .title("创建数据库")
                        .child(form.clone())
                        .width(px(600.0))
                        .on_ok(move |_, _window, _cx| {
                           true 
                        })
                        .on_cancel(|_, _window, _cx| {
                             true
                        })
                });
            }
        }
    }

    /// 处理编辑数据库事件
    fn handle_edit_database(
        node: DbNode,
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::database_view_plugin::DatabaseViewPluginRegistry;
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let db_type = config.database_type;
            let registry = DatabaseViewPluginRegistry::new();
            
            if let Some(plugin) = registry.get(&db_type) {
                let form = plugin.create_database_form(window, cx);
                
                // TODO: 预填充现有数据库的配置信息
                
                window.open_dialog(cx, move |dialog, _window, _cx| {
                    dialog
                        .title(format!("编辑数据库: {}", database_name))
                        .child(form.clone())
                        .width(px(600.0))
                        .on_cancel(|_, _window, _cx| true)
                });
            }
        }
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
                        let result = Tokio::spawn_result(cx, async move {
                            // 这里可以添加实际的数据库关闭逻辑
                            // 比如执行 USE mysql 切换到系统数据库
                            Ok(())
                        }).unwrap().await;

                        match result {
                            Ok(_) => {
                                eprintln!("Database closed: {}", db_name_log);

                                // 收起数据库节点并清理状态
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree_view, cx| {
                                        tree_view.close_database(&db_node_id, cx);
                                    });
                                });
                            }
                            Err(e) => {
                                eprintln!("Failed to close database: {}", e);
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
                                eprintln!("Database deleted: {}", db_name_log);
                                // 刷新父节点（连接节点）
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(conn_id_for_refresh, cx);
                                    });
                                });
                            }
                            Err(e) => eprintln!("Failed to start delete task: {}", e),
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
                                eprintln!("Table deleted: {}", tbl_name_log);
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                });
                            }
                            Err(e) => eprintln!("Failed to start delete task: {}", e),
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

                    cx.spawn(async move |cx: &mut AsyncApp| {
                        let old_name_log = old_name.clone();
                        let new_name_log = new_name.clone();
                        let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.to_string()).unwrap_or_default();

                        let task = state.rename_table(cx, conn_id.clone(), database, old_name.clone(), new_name.clone()).await;
                        match task {
                            Ok(task) => {
                                eprintln!("Table renamed: {} -> {}", old_name_log, new_name_log)
                            }
                            Err(e) => eprintln!("Failed to start rename task: {}", e),
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
                            Ok(task) => {
                                eprintln!("Table truncated: {}", tbl_name_log);
                            }
                            Err(e) => eprintln!("Failed to start truncate task: {}", e),
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
                            Ok(task) => {
                                eprintln!("View deleted: {}", v_name_log);
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                });
                            }
                            Err(e) => eprintln!("Failed to start delete task: {}", e),
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

            window.open_dialog(cx, move |dialog, _window, _cx| {
                let old_name = old_query_name.clone();
                let storage = storage_manager.clone();
                let input = input_state.clone();

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
                        let old_name_log = old_name.clone();
                        let new_name_log = new_name.clone();

                        cx.spawn(async move |_cx| {
                            if let Ok(pool) = storage.get_pool().await {
                                if let Some(query_repo_arc) = storage.get::<QueryRepository>().await {
                                    let query_repo = (*query_repo_arc).clone();
                                    if let Ok(Some(mut query)) = query_repo.get(&pool, qid).await {
                                        query.name = new_name_log.clone();
                                        match query_repo.update(&pool, &query).await {
                                            Ok(_) => eprintln!("Query renamed: {} -> {}", old_name_log, new_name_log),
                                            Err(e) => eprintln!("Failed to rename query: {}", e),
                                        }
                                    }
                                }
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
                            if let Ok(pool) = storage.get_pool().await {
                                if let Some(query_repo_arc) = storage.get::<QueryRepository>().await {
                                    let query_repo = (*query_repo_arc).clone();
                                    let _ = query_repo.delete(&pool, qid).await;
                                    eprintln!("Query deleted: {}", qid);

                                    // 刷新树
                                    let _ = cx.update(|cx| {
                                        tree.update(cx, |tree, cx| {
                                            tree.refresh_tree(conn_id.clone(), cx);
                                        });
                                    });
                                }
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
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_run_view::SqlRunView;

        let connection_id = node.connection_id.clone();
        // 获取数据库名：如果是数据库节点则用 name，否则为空（连接级别）
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
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        use crate::sql_dump_view::SqlDumpView;

        let connection_id = node.connection_id.clone();
        let database = node.name.clone();

        let config = Tokio::block_on(cx, async move {
            global_state.get_config_async(&connection_id).await
        });

        if let Some(config) = config {
            let dump_view = SqlDumpView::new(config.id, database, window, cx);

            window.open_dialog(cx, move |dialog, _window, _cx| {
                dialog
                    .title("转储SQL文件")
                    .child(dump_view.clone())
                    .width(px(800.0))
                    .on_cancel(|_, _window, _cx| true)
            });
        }
    }
}