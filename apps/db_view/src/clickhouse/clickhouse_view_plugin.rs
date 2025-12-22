use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::common::DatabaseEditorView;
use crate::database_view_plugin::{DatabaseViewPlugin, TableDesignerCapabilities, NodeMenuCapabilities};
use crate::db_connection_form::{DbConnectionForm, DbFormConfig};
use crate::clickhouse::database_form::ClickHouseDatabaseForm;

pub struct ClickHouseDatabaseViewPlugin;

impl ClickHouseDatabaseViewPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseViewPlugin for ClickHouseDatabaseViewPlugin {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::ClickHouse
    }

    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm> {
        cx.new(|cx| DbConnectionForm::new(DbFormConfig::clickhouse(), window, cx))
    }

    fn create_database_editor_view(
        &self,
        _connection_id: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView> {
        cx.new(|cx| {
            let form = cx.new(|cx| ClickHouseDatabaseForm::new(window, cx));
            DatabaseEditorView::new(form, DatabaseType::ClickHouse, false, window, cx)
        })
    }

    fn create_database_editor_view_for_edit(
        &self,
        _connection_id: String,
        database_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView> {
        cx.new(|cx| {
            let form = cx.new(|cx| ClickHouseDatabaseForm::new_for_edit(&database_name, window, cx));
            DatabaseEditorView::new(form, DatabaseType::ClickHouse, true, window, cx)
        })
    }

    fn get_table_designer_capabilities(&self) -> TableDesignerCapabilities {
        TableDesignerCapabilities {
            supports_engine: true,
            supports_charset: false,
            supports_collation: false,
            supports_auto_increment: false,
            supports_tablespace: false,
        }
    }

    fn get_engines(&self) -> Vec<String> {
        vec![
            "MergeTree".to_string(),
            "ReplacingMergeTree".to_string(),
            "SummingMergeTree".to_string(),
            "AggregatingMergeTree".to_string(),
            "CollapsingMergeTree".to_string(),
            "VersionedCollapsingMergeTree".to_string(),
            "GraphiteMergeTree".to_string(),
            "ReplicatedMergeTree".to_string(),
            "Log".to_string(),
            "TinyLog".to_string(),
            "StripeLog".to_string(),
            "Memory".to_string(),
            "Distributed".to_string(),
        ]
    }

    fn get_node_menu_capabilities(&self) -> NodeMenuCapabilities {
        NodeMenuCapabilities {
            supports_truncate_table: true,
            supports_rename_table: true,
            supports_table_import: true,
            supports_table_export: true,
            supports_create_database: true,
            supports_edit_database: false,
            supports_drop_database: true,
            supports_dump_database: true,
            supports_create_schema: false,
            supports_delete_schema: false,
            supports_create_view: true,
            supports_edit_view: true,
            supports_sequences: false,
            supports_triggers: false,
            supports_stored_procedures: false,
            supports_functions: true,
        }
    }
}
