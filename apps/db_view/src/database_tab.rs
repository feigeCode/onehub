use std::any::Any;

use db::{DbNode, DbNodeType, GlobalDbState};
use gpui::{div, px, prelude::FluentBuilder, AnyElement, App, AppContext, Context, Entity, FontWeight, Hsla, IntoElement, ParentElement, SharedString, Styled, Subscription, Window};
use gpui_component::{button::ButtonVariants, h_flex, resizable::{h_resizable, resizable_panel}, v_flex, ActiveTheme, IconName, WindowExt};

use crate::sql_editor_view::SqlEditorTabContent;
use one_core::{gpui_tokio::Tokio, storage::{StoredConnection}, tab_container::{TabContainer, TabContent, TabContentType, TabItem}};
use uuid::Uuid;

// 字符集选择项
#[derive(Clone, Debug)]
struct CharsetItem {
    name: String,
}

impl CharsetItem {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl gpui_component::select::SelectItem for CharsetItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }
}

// 排序规则选择项
#[derive(Clone, Debug)]
struct CollationItem {
    name: String,
}

impl CollationItem {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl gpui_component::select::SelectItem for CollationItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }
}

use crate::{database_objects_tab::DatabaseObjectsPanel, db_tree_view::{DbTreeView, DbTreeViewEvent}};

// Event handler for database tree view events
struct DatabaseEventHandler {
    _tree_subscription: Subscription,
}

impl DatabaseEventHandler {
    fn new(
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

            match event {
                DbTreeViewEvent::NodeSelected { node } => {
                    Self::handle_node_selected(node.clone(), global_state, objects_panel, cx);
                }
                DbTreeViewEvent::CreateNewQuery { node } => {
                    Self::handle_create_new_query(node.clone(), tab_container, tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::OpenTableData { node } => {
                    Self::handle_open_table_data(node.clone(), global_state, tab_container, window, cx);
                }
                DbTreeViewEvent::OpenViewData { node } => {
                    Self::handle_open_view_data(node.clone(), global_state, tab_container, window, cx);
                }
                DbTreeViewEvent::OpenTableStructure { node } => {
                    Self::handle_open_table_structure(node.clone(), global_state, tab_container, window, cx);
                }
                DbTreeViewEvent::ImportData { node } => {
                    Self::handle_import_data(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::ExportData { node } => {
                    Self::handle_export_data(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::CloseConnection { node } => {
                    Self::handle_close_connection(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::EditConnection { node } => {
                    Self::handle_edit_connection(node.clone(), window, cx);
                }
                DbTreeViewEvent::DeleteConnection { node } => {
                    Self::handle_delete_connection(node.clone(), tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::EditDatabase { node } => {
                    Self::handle_edit_database(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::CloseDatabase { node } => {
                    Self::handle_close_database(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::DeleteDatabase { node } => {
                    Self::handle_delete_database(node.clone(), global_state, tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::DeleteTable { node } => {
                    Self::handle_delete_table(node.clone(), global_state, tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::RenameTable { node } => {
                    Self::handle_rename_table(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::TruncateTable { node } => {
                    Self::handle_truncate_table(node.clone(), global_state, tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::DeleteView { node } => {
                    Self::handle_delete_view(node.clone(), global_state, tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::OpenNamedQuery { node } => {
                    Self::handle_open_named_query(node.clone(), tab_container, window, cx);
                }
                DbTreeViewEvent::RenameQuery { node } => {
                    Self::handle_rename_query(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::DeleteQuery { node } => {
                    Self::handle_delete_query(node.clone(), tree_view.clone(), window, cx);
                }
                DbTreeViewEvent::RunSqlFile { node } => {
                    Self::handle_run_sql_file(node.clone(), global_state, window, cx);
                }
                DbTreeViewEvent::DumpSqlFile { node } => {
                    Self::handle_dump_sql_file(node.clone(), global_state, window, cx);
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

        let config = Tokio::block_on(cx, async move {
            global_state.get_config(&connection_id).await
        });

        if let Some(config) = config {
            objects_panel.update(cx, |panel, cx| {
                panel.handle_node_selected(node, config, cx);
            });
        }
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
            global_state.get_config(&connection_id).await
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
            global_state.get_config(&connection_id).await
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
            global_state.get_config(&connection_id).await
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
        use crate::data_import_view::DataImportView;
        use gpui_component::WindowExt;

        let connection_id = node.connection_id.clone();
        // 获取数据库名和表名
        let (database, table_name) = if node.node_type == db::DbNodeType::Table {
            // 表节点：从 metadata 获取数据库名，表名是 node.name
            let db = node.metadata.as_ref()
                .and_then(|m| m.get("database"))
                .cloned()
                .unwrap_or_else(|| node.parent_context.clone().unwrap_or_default());
            (db, Some(node.name.clone()))
        } else {
            // 数据库节点：数据库名是 node.name
            (node.name.clone(), None)
        };

        eprintln!("Opening import dialog for database: {}", database);

        let config = Tokio::block_on(cx, async move {
            global_state.get_config(&connection_id).await
        });

        if let Some(config) = config {
            let import_view = DataImportView::new(
                config.id,
                database.clone(),
                window,
                cx,
            );

            // 如果有表名则预填
            if let Some(table) = table_name {
                import_view.update(cx, |view, cx| {
                    view.table.update(cx, |state, cx| {
                        state.set_value(table, window, cx);
                    });
                });
            }

            eprintln!("Import view created, opening dialog...");

            window.open_dialog(cx, move |dialog, _window, _cx| {
                eprintln!("Dialog builder called");
                dialog
                    .title("导入数据")
                    .child(import_view.clone())
                    .width(px(800.0))
                    .on_cancel(|_, _window, _cx| true)
            });

            eprintln!("Dialog opened");
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
            global_state.get_config(&connection_id).await
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
        _global_state: GlobalDbState,
        _window: &mut Window,
        _cx: &mut App,
    ) {
        eprintln!("Close connection: {}", node.connection_id);
        // TODO: 实现关闭连接逻辑
    }

    /// 处理编辑连接事件
    fn handle_edit_connection(
        node: DbNode,
        _window: &mut Window,
        cx: &mut App,
    ) {
        use one_core::storage::traits::Repository;
        use one_core::storage::{ConnectionRepository, GlobalStorageState};

        let connection_id = node.connection_id.clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();

        let conn_id_log = connection_id.clone();
        let result = Tokio::block_on(cx, async move {
            let pool = storage_manager.get_pool().await.ok()?;
            let conn_id = connection_id.parse::<i64>().ok()?;
            let conn_repo_arc = storage_manager.get::<ConnectionRepository>().await?;
            let conn_repo = (*conn_repo_arc).clone();
            conn_repo.get(&pool, conn_id).await.ok()?
        });

        if let Some(_connection) = result {
            eprintln!("Edit connection: {}", conn_id_log);
            // TODO: 实现编辑连接对话框
        }
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

    /// 处理编辑数据库事件
    fn handle_edit_database(
        node: DbNode,
        global_state: GlobalDbState,
        window: &mut Window,
        cx: &mut App,
    ) {
        let connection_id = node.connection_id.clone();
        let database_name = node.name.clone();

        let config = Tokio::block_on(cx, async move {
            global_state.get_config(&connection_id).await
        });

        if let Some(config) = config {
            Self::show_edit_database_dialog(database_name, config, window, cx);
        }
    }

    /// 显示编辑数据库对话框
    fn show_edit_database_dialog(
        database_name: String,
        config: one_core::storage::DbConnectionConfig,
        window: &mut Window,
        cx: &mut App,
    ) {
        use gpui_component::select::{Select, SelectItem, SelectState};

        // 创建字符集选择器
        let charset_items = vec![
            CharsetItem::new("utf8mb3"),
            CharsetItem::new("utf8mb4"),
            CharsetItem::new("latin1"),
            CharsetItem::new("gbk"),
            CharsetItem::new("utf8"),
        ];
        let charset_select = cx.new(|cx| {
            SelectState::new(charset_items, Some(Default::default()), window, cx)
        });

        // 创建排序规则选择器
        let collation_items = vec![
            CollationItem::new("utf8mb4_general_ci"),
            CollationItem::new("utf8mb4_unicode_ci"),
            CollationItem::new("utf8mb4_bin"),
            CollationItem::new("utf8mb3_general_ci"),
            CollationItem::new("utf8mb3_unicode_ci"),
            CollationItem::new("latin1_swedish_ci"),
            CollationItem::new("gbk_chinese_ci"),
        ];
        let collation_select = cx.new(|cx| {
            SelectState::new(collation_items, Some(Default::default()), window, cx)
        });

        let db_name = database_name.clone();
        let config_id = config.id.clone();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let charset_sel = charset_select.clone();
            let collation_sel = collation_select.clone();
            let db_name_display = db_name.clone();
            let db_name_for_update = db_name.clone();
            let cfg_id = config_id.clone();

            dialog
                .title("编辑数据库")
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
                                        .w(px(100.))
                                        .child("数据库名称:")
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(db_name_display.clone())
                                )
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(
                                    div()
                                        .w(px(100.))
                                        .child("字符集:")
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Select::new(&charset_sel))
                                )
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(
                                    div()
                                        .w(px(100.))
                                        .child("排序规则:")
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Select::new(&collation_sel))
                                )
                        )
                )
                .on_ok(move |_, _, cx| {
                    let charset = charset_sel.read(cx).selected_value().cloned();
                    let collation = collation_sel.read(cx).selected_value().cloned();
                    let db_name = db_name_for_update.clone();
                    let config_id = cfg_id.clone();
                    let global_state = cx.global::<GlobalDbState>().clone();
                    let db_name_log = db_name.clone();

                    cx.spawn(async move |_cx| {
                        let result = db::spawn_result(async move {
                            let (plugin, conn_arc) = global_state.get_plugin_and_connection(&config_id).await?;
                            let conn = conn_arc.read().await;
                            
                            // 构建 ALTER DATABASE 语句
                            let mut sql = format!("ALTER DATABASE {} ", plugin.quote_identifier(&db_name));
                            if let Some(cs) = charset {
                                sql.push_str(&format!("CHARACTER SET {} ", cs));
                            }
                            if let Some(col) = collation {
                                sql.push_str(&format!("COLLATE {}", col));
                            }
                            
                            plugin.execute_query(&**conn, "", &sql, None).await?;
                            Ok(())
                        }).await;

                        match result {
                            Ok(_) => eprintln!("Database updated: {}", db_name_log),
                            Err(e) => eprintln!("Failed to update database: {}", e),
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理关闭数据库事件
    fn handle_close_database(
        node: DbNode,
        _global_state: GlobalDbState,
        _window: &mut Window,
        _cx: &mut App,
    ) {
        eprintln!("Close database: {}", node.name);
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
                    let state = state.clone();
                    let db_name_log = db_name.clone();
                    let tree = tree.clone();
                    let conn_id_for_refresh = conn_id.clone();
                    
                    cx.spawn(async move |cx| {
                        let result = db::spawn_result(async move {
                            let (plugin, conn_arc) = state.get_plugin_and_connection(&conn_id).await?;
                            let conn = conn_arc.read().await;
                            plugin.drop_database(&**conn, &db_name).await
                        }).await;

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
                            Err(e) => eprintln!("Failed to delete database: {}", e),
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
                    
                    cx.spawn(async move |cx| {
                        let result = db::spawn_result(async move {
                            let (plugin, conn_arc) = state.get_plugin_and_connection(&conn_id).await?;
                            let conn = conn_arc.read().await;
                            let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.as_str()).unwrap_or("");
                            plugin.drop_table(&**conn, database, &tbl_name).await
                        }).await;

                        match result {
                            Ok(_) => {
                                eprintln!("Table deleted: {}", tbl_name_log);
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                });
                            }
                            Err(e) => eprintln!("Failed to delete table: {}", e),
                        }
                    }).detach();
                    true
                })
        });
    }

    /// 处理重命名表事件
    fn handle_rename_table(
        node: DbNode,
        _global_state: GlobalDbState,
        _window: &mut Window,
        _cx: &mut App,
    ) {
        eprintln!("Rename table: {}", node.name);
        // TODO: 实现重命名表对话框
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
                    
                    cx.spawn(async move |_cx| {
                        let result = db::spawn_result(async move {
                            let (plugin, conn_arc) = state.get_plugin_and_connection(&conn_id).await?;
                            let conn = conn_arc.read().await;
                            let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.as_str()).unwrap_or("");
                            plugin.truncate_table(&**conn, database, &tbl_name).await
                        }).await;

                        match result {
                            Ok(_) => {
                                eprintln!("Table truncated: {}", tbl_name_log);
                                // 清空表不需要刷新树，因为表结构没变
                            }
                            Err(e) => eprintln!("Failed to truncate table: {}", e),
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
                    
                    cx.spawn(async move |cx| {
                        let result = db::spawn_result(async move {
                            let (plugin, conn_arc) = state.get_plugin_and_connection(&conn_id).await?;
                            let conn = conn_arc.read().await;
                            let database = meta.as_ref().and_then(|m| m.get("database")).map(|s| s.as_str()).unwrap_or("");
                            plugin.drop_view(&**conn, database, &v_name).await
                        }).await;

                        match result {
                            Ok(_) => {
                                eprintln!("View deleted: {}", v_name_log);
                                // 刷新数据库节点
                                let _ = cx.update(|cx| {
                                    tree.update(cx, |tree, cx| {
                                        tree.refresh_tree(db_node_id, cx);
                                    });
                                });
                            }
                            Err(e) => eprintln!("Failed to delete view: {}", e),
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
        _window: &mut Window,
        _cx: &mut App,
    ) {
        eprintln!("Rename query: {}", node.name);
        // TODO: 实现重命名查询对话框
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
        let database = if node.node_type == db::DbNodeType::Database {
            Some(node.name.clone())
        } else {
            None
        };

        let config = Tokio::block_on(cx, async move {
            global_state.get_config(&connection_id).await
        });

        if let Some(config) = config {
            let run_view = SqlRunView::new(config.id, database, window, cx);

            window.open_dialog(cx, move |dialog, _window, _cx| {
                dialog
                    .title("运行SQL文件")
                    .child(run_view.clone())
                    .width(px(800.0))
                    .on_cancel(|_, _window, _cx| true)
            });
        }
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
            global_state.get_config(&connection_id).await
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

// Database connection tab content - using TabContainer architecture
pub struct DatabaseTabContent {
    connections: Vec<StoredConnection>,
    tab_container: Entity<TabContainer>,
    db_tree_view: Entity<DbTreeView>,
    objects_panel: Entity<DatabaseObjectsPanel>,
    status_msg: Entity<String>,
    is_connected: Entity<bool>,
    event_handler: Option<Entity<DatabaseEventHandler>>,
    tab_name: Option< String>
}

impl DatabaseTabContent {

    pub fn new( connections: Vec<StoredConnection>, window: &mut Window, cx: &mut App) -> Self {
        Self::new_with_name(None, connections, window, cx)
    }
    pub fn new_with_name(tab_name: Option<String>, connections: Vec<StoredConnection>, window: &mut Window, cx: &mut App) -> Self {
        // Create database tree view
        let db_tree_view = cx.new(|cx| {
            DbTreeView::new(&connections, window, cx)
        });

        // Create tab container - use default theme colors for automatic theme switching
        let tab_container = cx.new(|cx| {
            TabContainer::new(window, cx)
        });

        // Create objects panel
        let objects_panel = cx.new(|cx| {
            DatabaseObjectsPanel::new(window, cx)
        });
        

        // Add objects panel to tab container
        tab_container.update(cx, |container, cx| {
            let panel_content = objects_panel.read(cx).clone();
            let tab = TabItem::new("objects-panel", panel_content);
            container.add_and_activate_tab(tab, cx);
        });

        let status_msg = cx.new(|_| "Ready".to_string());
        let is_connected = cx.new(|_| true);

        // Create event handler to handle tree view events
        let event_handler = cx.new(|cx| {
            DatabaseEventHandler::new(&db_tree_view, tab_container.clone(), objects_panel.clone(), window, cx)
        });

        // 注册连接配置到 GlobalDbState，然后自动连接
        let global_state = cx.global::<GlobalDbState>().clone();
        let connections_clone = connections.clone();

        cx.spawn(async move |_cx| {
            // 先注册所有连接
            for conn in &connections_clone {
                if let Ok(db_config) = conn.to_db_connection() {
                    let _ = global_state.register_connection(db_config).await;
                }
            }
        }).detach();

        Self {
            connections: connections.clone(),
            tab_container,
            db_tree_view,
            objects_panel,
            status_msg,
            is_connected,
            event_handler: Some(event_handler),
            tab_name
        }
    }

    fn render_connection_status(&self, cx: &mut App) -> AnyElement {
        let status_text = self.status_msg.read(cx).clone();
        let is_error = status_text.contains("Failed") || status_text.contains("failed");

        // 获取第一个连接信息用于显示
        let first_conn = self.connections.first();
        let conn_name = first_conn.map(|c| c.name.clone()).unwrap_or_else(|| "Unknown".to_string());
        let (conn_host, conn_port, conn_username, conn_database) = first_conn
            .and_then(|c| c.to_database_params().ok())
            .map(|p| (p.host, p.port, p.username, p.database))
            .unwrap_or_default();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_6()
            .child(
                // Loading animation or error icon
                div()
                    .w(px(64.0))
                    .h(px(64.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .w(px(48.0))
                            .h(px(48.0))
                            .rounded(px(24.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .when(!is_error, |this| {
                                // Loading animation - simple circle
                                this.border_4()
                                    .border_color(cx.theme().accent)
                                    .text_2xl()
                                    .text_color(cx.theme().accent)
                                    .child("⟳")
                            })
                            .when(is_error, |this| {
                                // Error state - red circle
                                this.bg(Hsla::red())
                                    .text_color(gpui::white())
                                    .text_2xl()
                                    .child("✕")
                            })
                    )
            )
            .child(
                div()
                    .text_xl()
                    .font_weight(FontWeight::BOLD)
                    .child(format!("Database Connection: {}", conn_name))
            )
            .child(
                v_flex()
                    .gap_2()
                    .p_4()
                    .bg(cx.theme().muted)
                    .rounded(px(8.0))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Host:")
                            )
                            .child(conn_host)
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Port:")
                            )
                            .child(format!("{}", conn_port))
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Username:")
                            )
                            .child(conn_username)
                    )
                    .when_some(conn_database, |this, db| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .child(
                                    div()
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .child("Database:")
                                )
                                .child(db)
                        )
                    })
            )
            .child(
                div()
                    .text_lg()
                    .when(!is_error, |this| {
                        this.text_color(cx.theme().accent)
                    })
                    .when(is_error, |this| {
                        this.text_color(Hsla::red())
                    })
                    .child(status_text)
            )
            .into_any_element()
    }

    fn render_toolbar(&self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        use gpui_component::button::Button;


        h_flex()
            .w_full()
            .h(px(36.0))
            .px_2()
            .gap_2()
            .items_center()
            .bg(cx.theme().background)
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                Button::new("refresh-tree")
                    .icon(IconName::Loader)
                    .child("刷新")
                    .ghost()
                    .tooltip("刷新")
            )
            .child(
                Button::new("new-query")
                    .icon(IconName::File)
                    .child("新建查询")
                    .ghost()
                    .tooltip("新建查询")
            )
            .child(
                Button::new("new-table")
                    .icon(IconName::Table)
                    .child("新建表")
                    .ghost()
                    .tooltip("新建表")
                    .on_click(move |_, window, cx| {

                        // if let Some(conn) = first_conn.as_ref() {
                        //     // 获取当前选中的数据库
                        //     let current_db = db_tree_view.read(cx).get_selected_database();
                        //     let database = current_db.unwrap_or_else(|| "default".to_string());
                        //     if let Ok(config) = conn.to_db_connection() {
                        //         let tab_id = format!("new-table-{}", Uuid::new_v4());
                        //
                        //         tab_container.update(cx, |container, cx| {
                        //             let table_designer = TableDesignerView::new_table(
                        //                 database,
                        //                 config.id,
                        //                 config.database_type,
                        //                 window,
                        //                 cx,
                        //             );
                        //             let tab = TabItem::new(tab_id, table_designer.read(cx).clone());
                        //             container.add_and_activate_tab(tab, cx);
                        //         });
                        //     }
                        // }
                    })
            )
    }
}

impl TabContent for DatabaseTabContent {
    fn title(&self) -> SharedString {
        if let Some(name) = self.tab_name.clone() {
            name.into()
        }else {
            self.connections.first()
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "Database".to_string())
                .into()
        }

    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::File)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        let is_connected_flag = *self.is_connected.read(cx);

        if !is_connected_flag {
            // Show loading/connection status
            self.render_connection_status(cx)
        } else {
            // Show layout with toolbar on top, resizable panels below
            v_flex()
                .size_full()
                .child(self.render_toolbar(window, cx))
                .child(
                    h_resizable("db-panels")
                        .child(
                            resizable_panel()
                                .size(px(280.0))
                                .size_range(px(200.0)..px(500.0))
                                .child(self.db_tree_view.clone())
                        )
                        .child(
                            resizable_panel()
                                .child(self.tab_container.clone())
                        )
                )
                .into_any_element()
        }
    }

    fn content_type(&self) -> TabContentType {
        let name = self.connections.first()
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        TabContentType::Custom(format!("database-{}", name))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for DatabaseTabContent {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            tab_container: self.tab_container.clone(),
            db_tree_view: self.db_tree_view.clone(),
            objects_panel: self.objects_panel.clone(),
            status_msg: self.status_msg.clone(),
            is_connected: self.is_connected.clone(),
            event_handler: self.event_handler.clone(),
            tab_name: self.tab_name.clone(),
        }
    }
}
