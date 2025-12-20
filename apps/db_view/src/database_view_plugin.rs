use std::{collections::HashMap, sync::Arc};

use gpui::{App, Entity, Global, Window};
use one_core::storage::DatabaseType;

use crate::common::DatabaseEditorView;
use crate::db_connection_form::DbConnectionForm;
use crate::mysql::mysql_view_plugin::MySqlDatabaseViewPlugin;
use crate::postgresql::postgresql_view_plugin::PostgreSqlDatabaseViewPlugin;
use crate::mssql::mssql_view_plugin::MsSqlDatabaseViewPlugin;
use crate::oracle::oracle_view_plugin::OracleDatabaseViewPlugin;
use crate::clickhouse::clickhouse_view_plugin::ClickHouseDatabaseViewPlugin;
use crate::sqlite::sqlite_view_plugin::SqliteDatabaseViewPlugin;

/// 表设计器 UI 配置能力
#[derive(Clone, Debug)]
pub struct TableDesignerCapabilities {
    /// 是否支持存储引擎选择（MySQL: InnoDB/MyISAM）
    pub supports_engine: bool,
    /// 是否支持字符集选择
    pub supports_charset: bool,
    /// 是否支持排序规则选择
    pub supports_collation: bool,
    /// 是否支持自增起始值设置
    pub supports_auto_increment: bool,
    /// 是否支持表空间（PostgreSQL）
    pub supports_tablespace: bool,
}

impl Default for TableDesignerCapabilities {
    fn default() -> Self {
        Self {
            supports_engine: false,
            supports_charset: false,
            supports_collation: false,
            supports_auto_increment: false,
            supports_tablespace: false,
        }
    }
}

/// 节点右键菜单能力配置
#[derive(Clone, Debug)]
pub struct NodeMenuCapabilities {
    // === Table 节点菜单项 ===
    /// 是否支持 TRUNCATE TABLE（清空表）
    pub supports_truncate_table: bool,
    /// 是否支持重命名表
    pub supports_rename_table: bool,
    /// 是否支持导入数据
    pub supports_table_import: bool,
    /// 是否支持导出数据
    pub supports_table_export: bool,

    // === Database 节点菜单项 ===
    /// 是否支持新建数据库
    pub supports_create_database: bool,
    /// 是否支持编辑数据库属性
    pub supports_edit_database: bool,
    /// 是否支持删除数据库
    pub supports_drop_database: bool,
    /// 是否支持转储数据库（导出 SQL）
    pub supports_dump_database: bool,

    // === View 节点菜单项 ===
    /// 是否支持新建视图
    pub supports_create_view: bool,
    /// 是否支持编辑视图
    pub supports_edit_view: bool,

    // === 其他功能 ===
    /// 是否支持序列（PostgreSQL 特有）
    pub supports_sequences: bool,
    /// 是否支持触发器
    pub supports_triggers: bool,
    /// 是否支持存储过程
    pub supports_stored_procedures: bool,
    /// 是否支持函数
    pub supports_functions: bool,
}

impl Default for NodeMenuCapabilities {
    fn default() -> Self {
        Self {
            supports_truncate_table: false,
            supports_rename_table: false,
            supports_table_import: false,
            supports_table_export: false,
            supports_create_database: false,
            supports_edit_database: false,
            supports_drop_database: false,
            supports_dump_database: false,
            supports_create_view: false,
            supports_edit_view: false,
            supports_sequences: false,
            supports_triggers: false,
            supports_stored_procedures: false,
            supports_functions: false,
        }
    }
}

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

    /// 获取表设计器 UI 配置能力
    fn get_table_designer_capabilities(&self) -> TableDesignerCapabilities {
        TableDesignerCapabilities::default()
    }

    /// 获取存储引擎列表（用于表设计器下拉框）
    fn get_engines(&self) -> Vec<String> {
        vec![]
    }

    /// 获取节点右键菜单能力配置
    fn get_node_menu_capabilities(&self) -> NodeMenuCapabilities {
        NodeMenuCapabilities::default()
    }
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
        registry.register(PostgreSqlDatabaseViewPlugin::new());
        registry.register(MsSqlDatabaseViewPlugin::new());
        registry.register(OracleDatabaseViewPlugin::new());
        registry.register(ClickHouseDatabaseViewPlugin::new());
        registry.register(SqliteDatabaseViewPlugin::new());

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
