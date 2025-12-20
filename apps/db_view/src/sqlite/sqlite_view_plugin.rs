use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::common::DatabaseEditorView;
use crate::database_view_plugin::{DatabaseViewPlugin, TableDesignerCapabilities, NodeMenuCapabilities};
use crate::db_connection_form::{DbConnectionForm, DbFormConfig};
use crate::sqlite::database_form::SqliteDatabaseForm;

pub struct SqliteDatabaseViewPlugin;

impl SqliteDatabaseViewPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseViewPlugin for SqliteDatabaseViewPlugin {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::SQLite
    }

    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm> {
        cx.new(|cx| DbConnectionForm::new(DbFormConfig::sqlite(), window, cx))
    }

    fn create_database_editor_view(
        &self,
        _connection_id: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView> {
        cx.new(|cx| {
            let form = cx.new(|cx| SqliteDatabaseForm::new(window, cx));
            DatabaseEditorView::new(form, DatabaseType::SQLite, false, window, cx)
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
            let form = cx.new(|cx| SqliteDatabaseForm::new_for_edit(&database_name, window, cx));
            DatabaseEditorView::new(form, DatabaseType::SQLite, true, window, cx)
        })
    }

    fn get_table_designer_capabilities(&self) -> TableDesignerCapabilities {
        TableDesignerCapabilities {
            supports_engine: false,
            supports_charset: false,
            supports_collation: false,
            supports_auto_increment: true,
            supports_tablespace: false,
        }
    }

    fn get_engines(&self) -> Vec<String> {
        vec![]
    }

    fn get_node_menu_capabilities(&self) -> NodeMenuCapabilities {
        NodeMenuCapabilities {
            supports_truncate_table: false,
            supports_rename_table: true,
            supports_table_import: true,
            supports_table_export: true,
            supports_create_database: false,
            supports_edit_database: false,
            supports_drop_database: false,
            supports_dump_database: true,
            supports_create_view: true,
            supports_edit_view: true,
            supports_sequences: false,
            supports_triggers: true,
            supports_stored_procedures: false,
            supports_functions: false,
        }
    }
}
