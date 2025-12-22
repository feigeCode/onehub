use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::common::DatabaseEditorView;
use crate::database_view_plugin::{DatabaseViewPlugin, TableDesignerCapabilities, NodeMenuCapabilities};
use crate::db_connection_form::{DbConnectionForm, DbFormConfig};
use crate::mysql::database_form::MySqlDatabaseForm;

/// MySQL 数据库视图插件
pub struct MySqlDatabaseViewPlugin;

impl MySqlDatabaseViewPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseViewPlugin for MySqlDatabaseViewPlugin {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::MySQL
    }

    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm> {
        cx.new(|cx| DbConnectionForm::new(DbFormConfig::mysql(), window, cx))
    }

    fn create_database_editor_view(
        &self,
        _connection_id: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView> {
        cx.new(|cx| {
            let form = cx.new(|cx| MySqlDatabaseForm::new(window, cx));
            DatabaseEditorView::new(form, DatabaseType::MySQL, false, window, cx)
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
            let form = cx.new(|cx| MySqlDatabaseForm::new_for_edit(&database_name, window, cx));
            DatabaseEditorView::new(form, DatabaseType::MySQL, true, window, cx)
        })
    }

    fn get_table_designer_capabilities(&self) -> TableDesignerCapabilities {
        TableDesignerCapabilities {
            supports_engine: true,
            supports_charset: true,
            supports_collation: true,
            supports_auto_increment: true,
            supports_tablespace: false,
        }
    }

    fn get_engines(&self) -> Vec<String> {
        vec![
            "InnoDB".to_string(),
            "MyISAM".to_string(),
            "MEMORY".to_string(),
            "CSV".to_string(),
            "ARCHIVE".to_string(),
            "BLACKHOLE".to_string(),
            "FEDERATED".to_string(),
        ]
    }

    fn get_node_menu_capabilities(&self) -> NodeMenuCapabilities {
        NodeMenuCapabilities {
            supports_truncate_table: true,
            supports_rename_table: true,
            supports_table_import: true,
            supports_table_export: true,
            supports_create_database: true,
            supports_edit_database: true,
            supports_drop_database: true,
            supports_dump_database: true,
            supports_create_schema: false,
            supports_delete_schema: false,
            supports_create_view: true,
            supports_edit_view: true,
            supports_sequences: false,
            supports_triggers: true,
            supports_stored_procedures: true,
            supports_functions: true,
        }
    }
}
