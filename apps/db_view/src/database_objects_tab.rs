use std::any::Any;

use gpui::{div, AnyElement, App, AppContext, Context, Entity, Focusable, FocusHandle, IntoElement, ParentElement, SharedString, Styled, Window, AsyncApp};
use db::{DbNode, DbNodeType, ObjectView};
use gpui_component::{
    table::{Table, TableState},
    v_flex, ActiveTheme, Size,
};
use one_core::gpui_tokio::Tokio;
use crate::{connection_list_panel::ConnectionListPanel, results_delegate::ResultsDelegate};
use one_core::storage::{DbConnectionConfig, StoredConnection};
use one_core::tab_container::{TabContent, TabContentType};

#[derive(Clone)]
enum LoadedData {
    ObjectView(ObjectView),
    ConnectionList(Entity<ConnectionListPanel>),
    None,
}

pub struct DatabaseObjectsPanel {
    selected_node: Entity<Option<DbNode>>,
    loaded_data: Entity<LoadedData>,
    connection_config: Entity<Option<DbConnectionConfig>>,
    table_state: Entity<TableState<ResultsDelegate>>,
    focus_handle: FocusHandle,
}

impl DatabaseObjectsPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let selected_node = cx.new(|_| None);
        let loaded_data = cx.new(|_| LoadedData::None);
        let connection_config = cx.new(|_| None);
        let delegate = ResultsDelegate::new(vec![], vec![]);
        let table_state = cx.new(|cx| TableState::new(delegate, window, cx));
        let focus_handle = cx.focus_handle();

        Self {
            selected_node,
            loaded_data,
            connection_config,
            table_state,
            focus_handle,
        }
    }

    pub fn handle_node_selected(&self, node: DbNode, config: DbConnectionConfig, cx: &mut App) {
        self.selected_node.update(cx, |n, cx| {
            *n = Some(node.clone());
            cx.notify();
        });

        self.connection_config.update(cx, |c, cx| {
            *c = Some(config.clone());
            cx.notify();
        });

        self.load_data_for_node(node, config, cx);
    }

    pub fn show_connection_list(&self, connections: Vec<StoredConnection>, workspace_name: Option<String>, cx: &mut App) {
        let panel = cx.new(|_cx| ConnectionListPanel::new(connections, workspace_name));
        
        self.loaded_data.update(cx, |data, cx| {
            *data = LoadedData::ConnectionList(panel);
            cx.notify();
        });
    }

    fn load_data_for_node(&self, node: DbNode, config: DbConnectionConfig, cx: &mut App) {
        let loaded_data = self.loaded_data.clone();
        let table_state = self.table_state.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let global_state = cx.update(|cx| cx.global::<db::GlobalDbState>().clone()).ok()?;

            let result = Tokio::block_on(cx, async move {
                let plugin = global_state.db_manager.get_plugin(&config.database_type).ok()?;
                let conn_arc = global_state
                    .connection_pool
                    .get_connection(config, &global_state.db_manager)
                    .await
                    .ok()?;
                let conn = conn_arc.read().await;
                match node.node_type {
                    DbNodeType::Connection => {
                        plugin.list_databases_view(&**conn).await.ok()
                    },
                    DbNodeType::Database | DbNodeType::TablesFolder => {
                        let mut database = &node.name;
                        if node.metadata.is_some() {
                            database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        }
                        plugin.list_tables_view(&**conn, database).await.ok()
                    }
                    DbNodeType::Table | DbNodeType::ColumnsFolder => {
                        let database = node.metadata.as_ref()?.get("database")?;
                        let mut table = &node.name;
                        if node.metadata.is_some() {
                            table = node.metadata.as_ref()?.get("table").or(Some(&node.name))?;
                        }
                        plugin.list_columns_view(&**conn, database, table).await.ok()
                    }
                    DbNodeType::ViewsFolder => {
                        let database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        plugin.list_views_view(&**conn, database).await.ok()
                    }
                    DbNodeType::FunctionsFolder => {
                        let database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        plugin.list_functions_view(&**conn, database).await.ok()
                    }
                    DbNodeType::ProceduresFolder => {
                        let database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        plugin.list_procedures_view(&**conn, database).await.ok()
                    }
                    DbNodeType::TriggersFolder => {
                        let database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        plugin.list_triggers_view(&**conn, database).await.ok()
                    }
                    DbNodeType::SequencesFolder => {
                        let database = node.metadata.as_ref()?.get("database").or(Some(&node.name))?;
                        plugin.list_sequences_view(&**conn, database).await.ok()
                    }
                    _ => None,
                }
            }).unwrap();

            if let Some(view) = result {
                let columns = view.columns.clone();
                let rows = view.rows.clone();

                cx.update(|cx| {
                    loaded_data.update(cx, |data, cx| {
                        *data = LoadedData::ObjectView(view);
                        cx.notify();
                    });

                    table_state.update(cx, |state, cx| {
                        state.delegate_mut().update_data(columns, rows);
                        state.refresh(cx);
                    });
                })
                .ok();
            } else {
                cx.update(|cx| {
                    loaded_data.update(cx, |data, cx| {
                        *data = LoadedData::None;
                        cx.notify();
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
        let loaded_data = self.loaded_data.read(cx).clone();
        let selected_node = self.selected_node.read(cx).clone();

        div()
            .size_full()
            .child(match loaded_data {
                LoadedData::ObjectView(object_view) => {
                    let title = object_view.title.clone();

                    v_flex()
                        .size_full()
                        .gap_2()
                        .child(div().p_2().text_sm().child(title))
                        .child(
                            div()
                                .flex_1()
                                .overflow_hidden()
                                .child(Table::new(&self.table_state).stripe(true).bordered(true)),
                        )
                        .into_any_element()
                }
                LoadedData::ConnectionList(panel) => panel.clone().into_any_element(),
                LoadedData::None => {
                    let message = if selected_node.is_none() {
                        "Select a database object to view details"
                    } else {
                        "Loading..."
                    };

                    v_flex()
                        .size_full()
                        .items_center()
                        .justify_center()
                        .child(div().text_color(cx.theme().muted_foreground).child(message))
                        .into_any_element()
                }
            })
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
            selected_node: self.selected_node.clone(),
            loaded_data: self.loaded_data.clone(),
            connection_config: self.connection_config.clone(),
            table_state: self.table_state.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}
