use std::{collections::HashMap, sync::Arc};

use gpui::{ App, Entity, Window};
use one_core::storage::DatabaseType;

use crate::db_connection_form::{DbConnectionForm};
use crate::mysql::database_form::DatabaseForm;
use crate::mysql::mysql_view_plugin::MySqlDatabaseViewPlugin;

/// 简化的数据库视图插件接口
/// 直接返回视图，不搞配置抽象层
pub trait DatabaseViewPlugin: Send + Sync {
    fn database_type(&self) -> DatabaseType;
    
    /// 创建数据库表单视图
    fn create_database_form(&self, window: &mut Window, cx: &mut App) -> Entity<DatabaseForm>;
    
    /// 创建连接表单视图  
    fn create_connection_form(&self, window: &mut Window, cx: &mut App) -> Entity<DbConnectionForm>;
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

        // 只注册 MySQL 插件，先保证走通
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
