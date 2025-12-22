use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::common::{DatabaseEditorView, SchemaEditorView};
use crate::database_view_plugin::{DatabaseViewPlugin, TableDesignerCapabilities, NodeMenuCapabilities};
use crate::db_connection_form::{DbConnectionForm, DbFormConfig};
use crate::mssql::database_form::MsSqlDatabaseForm;
use crate::mssql::schema_form::MsSqlSchemaForm;

pub struct MsSqlDatabaseViewPlugin;

impl MsSqlDatabaseViewPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseViewPlugin for MsSqlDatabaseViewPlugin {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::MSSQL
    }

    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm> {
        cx.new(|cx| DbConnectionForm::new(DbFormConfig::mssql(), window, cx))
    }

    fn create_database_editor_view(
        &self,
        _connection_id: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView> {
        cx.new(|cx| {
            let form = cx.new(|cx| MsSqlDatabaseForm::new(window, cx));
            DatabaseEditorView::new(form, DatabaseType::MSSQL, false, window, cx)
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
            let form = cx.new(|cx| MsSqlDatabaseForm::new_for_edit(&database_name, window, cx));
            DatabaseEditorView::new(form, DatabaseType::MSSQL, true, window, cx)
        })
    }

    fn create_schema_editor_view(
        &self,
        _connection_id: String,
        _database_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Option<Entity<SchemaEditorView>> {
        Some(cx.new(|cx| {
            let form = cx.new(|cx| MsSqlSchemaForm::new(window, cx));
            SchemaEditorView::new(form, DatabaseType::MSSQL, window, cx)
        }))
    }

    fn get_table_designer_capabilities(&self) -> TableDesignerCapabilities {
        TableDesignerCapabilities {
            supports_engine: false,
            supports_charset: false,
            supports_collation: true,
            supports_auto_increment: false,
            supports_tablespace: false,
        }
    }

    fn get_engines(&self) -> Vec<String> {
        vec![]
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
            supports_create_schema: true,
            supports_delete_schema: true,
            supports_create_view: true,
            supports_edit_view: true,
            supports_sequences: true,
            supports_triggers: true,
            supports_stored_procedures: true,
            supports_functions: true,
        }
    }
}
