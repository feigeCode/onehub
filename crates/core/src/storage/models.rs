use crate::storage::traits::Entity;
use gpui_component::Size::Large;
use gpui_component::{Icon, IconName, Sizable};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnectionType {
    All,
    Database,
    SshSftp,
    Redis,
    MongoDB,
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ConnectionType::All => "All",
            ConnectionType::Database => "Database",
            ConnectionType::SshSftp => "SshSftp",
            ConnectionType::Redis => "Redis",
            ConnectionType::MongoDB => "MongoDB",
        };
        write!(f, "{}", s)
    }
}

impl ConnectionType {
    pub fn from_str(s: &str) -> Self {
        match s {
            "Database" => ConnectionType::Database,
            "SshSftp" => ConnectionType::SshSftp,
            "Redis" => ConnectionType::Redis,
            "MongoDB" => ConnectionType::MongoDB,
            _ => ConnectionType::Database,
        }
    }
}

/// Database type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    SQLite,
    MSSQL,
    Oracle,
    ClickHouse,
}

impl DatabaseType {
    pub fn all() -> &'static [DatabaseType] {
        &[
            DatabaseType::MySQL,
            DatabaseType::PostgreSQL,
            DatabaseType::SQLite,
            DatabaseType::MSSQL,
            DatabaseType::Oracle,
            DatabaseType::ClickHouse,
        ]
    }

    pub fn as_str(&self) -> &str {
        match self {
            DatabaseType::MySQL => "MySQL",
            DatabaseType::PostgreSQL => "PostgreSQL",
            DatabaseType::SQLite => "SQLite",
            DatabaseType::MSSQL => "MSSQL",
            DatabaseType::Oracle => "Oracle",
            DatabaseType::ClickHouse => "ClickHouse",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "MySQL" => Some(DatabaseType::MySQL),
            "PostgreSQL" => Some(DatabaseType::PostgreSQL),
            "SQLite" => Some(DatabaseType::SQLite),
            "MSSQL" => Some(DatabaseType::MSSQL),
            "Oracle" => Some(DatabaseType::Oracle),
            "ClickHouse" => Some(DatabaseType::ClickHouse),
            _ => None,
        }
    }

    pub fn as_icon(&self) -> Icon {
        match self {
            DatabaseType::MySQL => IconName::MySQLColor.color().with_size(Large),
            DatabaseType::PostgreSQL => IconName::PostgreSQLColor.color().with_size(Large),
            DatabaseType::SQLite => IconName::SQLiteColor.color().with_size(Large),
            DatabaseType::MSSQL => IconName::MSSQLColor.color().with_size(Large),
            DatabaseType::Oracle => IconName::OracleColor.color().with_size(Large),
            DatabaseType::ClickHouse => IconName::ClickHouseColor.color().with_size(Large),
        }
    }
    pub fn as_node_icon(&self) -> Icon {
        match self {
            DatabaseType::MySQL => IconName::MySQLLineColor.color().with_size(Large),
            DatabaseType::PostgreSQL => IconName::PostgreSQLLineColor.color().with_size(Large),
            DatabaseType::SQLite => IconName::SQLiteLineColor.color().with_size(Large),
            DatabaseType::MSSQL => IconName::MSSQLLineColor.color().with_size(Large),
            DatabaseType::Oracle => IconName::OracleLineColor.color().with_size(Large),
            DatabaseType::ClickHouse => IconName::ClickHouseLineColor.color().with_size(Large),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshParams {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub auth_method: SshAuthMethod,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SshAuthMethod {
    Password { password: String },
    PrivateKey { key_path: String, passphrase: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisParams {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub db_index: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoDBParams {
    pub connection_string: String,
}


/// Connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConnectionConfig {
    #[serde(skip)]
    pub id: String,
    pub database_type: DatabaseType,
    #[serde(skip)]
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    #[serde(skip)]
    pub workspace_id: Option<i64>,
    #[serde(default)]
    pub extra_params: std::collections::HashMap<String, String>,
}

impl DbConnectionConfig {
    pub fn get_param(&self, key: &str) -> Option<&String> {
        self.extra_params.get(key)
    }

    pub fn get_param_as<T: std::str::FromStr>(&self, key: &str) -> Option<T> {
        self.extra_params.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_param_bool(&self, key: &str) -> bool {
        self.extra_params.get(key)
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    }
}

impl ConnectionType {
    pub fn label(&self) -> &'static str {
        match self {
            ConnectionType::All => "全部",
            ConnectionType::Database => "数据库",
            ConnectionType::SshSftp => "SSH/SFTP",
            ConnectionType::Redis => "Redis",
            ConnectionType::MongoDB => "MongoDB",
        }
    }

    pub fn icon(&self) -> IconName {
        match self {
            ConnectionType::All => IconName::Server,
            ConnectionType::Database => IconName::Database,
            ConnectionType::SshSftp => IconName::Terminal,
            ConnectionType::Redis => IconName::Redis,
            ConnectionType::MongoDB => IconName::MongoDB,
        }
    }
}

/// Workspace for organizing connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

impl Entity for Workspace {
    fn id(&self) -> Option<i64> {
        self.id
    }

    fn created_at(&self) -> i64 {
        self.created_at.expect("created_at 在从数据库读取后应该存在")
    }

    fn updated_at(&self) -> i64 {
        self.updated_at.expect("updated_at 在从数据库读取后应该存在")
    }
}

impl Workspace {
    pub fn new(name: String) -> Self {
        Self {
            id: None,
            name,
            color: None,
            icon: None,
            created_at: None,
            updated_at: None,
        }
    }
}

/// Stored connection with ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub name: String,
    pub connection_type: ConnectionType,
    pub params: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<i64>,
    /// 已选中的数据库ID列表（JSON数组），None表示全选
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_databases: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

impl Entity for StoredConnection {
    fn id(&self) -> Option<i64> {
        self.id
    }

    fn created_at(&self) -> i64 {
        self.created_at.expect("created_at 在从数据库读取后应该存在")
    }

    fn updated_at(&self) -> i64 {
        self.updated_at.expect("updated_at 在从数据库读取后应该存在")
    }
}

impl StoredConnection {
    pub fn new_database(name: String, params: DbConnectionConfig, workspace_id: Option<i64>) -> Self {
        Self {
            id: None,
            name,
            connection_type: ConnectionType::Database,
            params: serde_json::to_string(&params).expect("DbConnectionConfig 序列化不应失败"),
            workspace_id,
            selected_databases: if let Some(database) = &params.database { Some(format!("[\"{}\"]", database)) } else {None},
            remark: None,
            created_at: None,
            updated_at: None,
        }
    }

    pub fn new_ssh(name: String, params: SshParams, workspace_id: Option<i64>) -> Self {
        Self {
            id: None,
            name,
            connection_type: ConnectionType::SshSftp,
            params: serde_json::to_string(&params).expect("SshParams 序列化不应失败"),
            workspace_id,
            selected_databases: None,
            remark: None,
            created_at: None,
            updated_at: None,
        }
    }

    pub fn new_redis(name: String, params: RedisParams, workspace_id: Option<i64>) -> Self {
        Self {
            id: None,
            name,
            connection_type: ConnectionType::Redis,
            params: serde_json::to_string(&params).expect("RedisParams 序列化不应失败"),
            workspace_id,
            selected_databases: None,
            remark: None,
            created_at: None,
            updated_at: None,
        }
    }

    pub fn new_mongodb(name: String, params: MongoDBParams, workspace_id: Option<i64>) -> Self {
        Self {
            id: None,
            name,
            connection_type: ConnectionType::MongoDB,
            params: serde_json::to_string(&params).expect("MongoDBParams 序列化不应失败"),
            workspace_id,
            selected_databases: None,
            remark: None,
            created_at: None,
            updated_at: None,
        }
    }

    pub fn to_ssh_params(&self) -> Result<SshParams, serde_json::Error> {
        serde_json::from_str(&self.params)
    }

    pub fn to_redis_params(&self) -> Result<RedisParams, serde_json::Error> {
        serde_json::from_str(&self.params)
    }

    pub fn to_mongodb_params(&self) -> Result<MongoDBParams, serde_json::Error> {
        serde_json::from_str(&self.params)
    }

    pub fn to_db_connection(&self) -> Result<DbConnectionConfig, serde_json::Error> {
        let mut params: DbConnectionConfig =  serde_json::from_str(&self.params)?;
        params.name = self.name.clone();
        params.workspace_id = self.workspace_id;
        params.id = self.id.unwrap_or(0).to_string();
        Ok(params)
    }

    pub fn from_db_connection(connection: DbConnectionConfig) -> Self {
        let name = connection.name.clone();
        let workspace_id = connection.workspace_id.clone();
        Self::new_database(name, connection, workspace_id)
    }

    /// 获取已选中的数据库列表，None表示全选
    pub fn get_selected_databases(&self) -> Option<Vec<String>> {
        self.selected_databases.as_ref().and_then(|json| {
            serde_json::from_str(json).ok()
        })
    }

    /// 设置已选中的数据库列表，None表示全选
    pub fn set_selected_databases(&mut self, databases: Option<Vec<String>>) {
        self.selected_databases = databases.map(|dbs| {
            serde_json::to_string(&dbs).unwrap_or_default()
        });
    }
}

/// Generic key-value storage model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub key: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> Self {
        Self {
            id: None,
            key,
            value,
            created_at: None,
            updated_at: None,
        }
    }
}

pub fn parse_db_type(s: &str) -> DatabaseType {
    match s {
        "MySQL" => DatabaseType::MySQL,
        "PostgreSQL" => DatabaseType::PostgreSQL,
        "SQLite" => DatabaseType::SQLite,
        _ => DatabaseType::MySQL,
    }
}
