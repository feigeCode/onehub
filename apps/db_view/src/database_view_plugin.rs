use std::{collections::HashMap, sync::Arc};

use gpui::{App, Entity, Global, Window};
use one_core::storage::DatabaseType;

use crate::common::DatabaseEditorView;
use crate::db_connection_form::DbConnectionForm;
use crate::mysql::mysql_view_plugin::MySqlDatabaseViewPlugin;

/// 数据库视图插件接口
/// 每种数据库类型实现此 trait 来提供特定的 UI 组件
pub trait DatabaseViewPlugin: Send + Sync {
    fn database_type(&self) -> DatabaseType;

    /// 创建连接表单视图
    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm>;

    /// 创建数据库编辑器视图（用于新建数据库）
    fn create_database_editor_view(
        &self,
        connection_id: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView>;

    /// 创建数据库编辑器视图（用于编辑现有数据库）
    fn create_database_editor_view_for_edit(
        &self,
        connection_id: String,
        database_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<DatabaseEditorView>;
}

pub type DatabaseViewPluginRef = Arc<dyn DatabaseViewPlugin>;

/// 插件注册表：用 HashMap 实现 O(1) 查找
pub struct DatabaseViewPluginRegistry {
    plugins: HashMap<DatabaseType, DatabaseViewPluginRef>,
}

impl DatabaseViewPluginRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            plugins: HashMap::new()
        };

        registry.register(MySqlDatabaseViewPlugin::new());

        registry
    }

    pub fn register<P>(&mut self, plugin: P)
    where
        P: DatabaseViewPlugin + 'static,
    {
        let plugin_ref = Arc::new(plugin);
        let db_type = plugin_ref.database_type();
        self.plugins.insert(db_type, plugin_ref);
    }

    pub fn get(&self, db_type: &DatabaseType) -> Option<DatabaseViewPluginRef> {
        self.plugins.get(db_type).cloned()
    }

    pub fn all(&self) -> impl Iterator<Item = DatabaseViewPluginRef> + '_ {
        self.plugins.values().cloned()
    }
}

impl Default for DatabaseViewPluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Global for DatabaseViewPluginRegistry {}
