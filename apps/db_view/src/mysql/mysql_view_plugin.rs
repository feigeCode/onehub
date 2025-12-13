use gpui::{App, AppContext, Entity, Window};
use one_core::storage::DatabaseType;
use crate::database_view_plugin::DatabaseViewPlugin;
use crate::db_connection_form::{DbConnectionForm, DbFormConfig};
use crate::mysql::database_form::DatabaseForm;

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

    fn create_database_form(&self, window: &mut Window, cx: &mut App) -> Entity<DatabaseForm> {
        cx.new(|cx| DatabaseForm::new_mysql(window, cx))
    }

    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm> {
        cx.new(|cx| DbConnectionForm::new(DbFormConfig::mysql(), window, cx))
    }
}