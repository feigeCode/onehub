use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::common::DatabaseEditorView;
use crate::database_view_plugin::DatabaseViewPlugin;
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
}
