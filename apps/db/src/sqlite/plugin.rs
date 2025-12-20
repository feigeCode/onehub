use anyhow::Result;
use async_trait::async_trait;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, ExecResult, SqlResult};
use crate::plugin::{DatabasePlugin, SqlCompletionInfo};
use crate::sqlite::SqliteDbConnection;
use crate::types::*;

/// SQLite database plugin implementation
pub struct SqlitePlugin;

impl SqlitePlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DatabasePlugin for SqlitePlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::SQLite
    }

    fn identifier_quote(&self) -> &str {
        "\""
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                ("AUTOINCREMENT", "Auto-increment column"),
                ("VACUUM", "Rebuild database file"),
                ("ATTACH", "Attach another database"),
                ("DETACH", "Detach attached database"),
                ("PRAGMA", "SQLite configuration"),
                ("GLOB", "Unix-style pattern matching"),
                ("REPLACE", "Insert or replace row"),
                ("INDEXED BY", "Force index usage"),
                ("NOT INDEXED", "Disable index usage"),
                ("NULLS FIRST", "Sort NULLs first"),
                ("NULLS LAST", "Sort NULLs last"),
            ],
            functions: vec![
                ("IFNULL(x, y)", "Return y if x is NULL"),
                ("NULLIF(x, y)", "Return NULL if x equals y"),
                ("IIF(cond, x, y)", "If-then-else expression"),
                ("TYPEOF(x)", "Return type name"),
                ("INSTR(str, substr)", "Find substring position"),
                ("PRINTF(fmt, ...)", "Formatted string"),
                ("SUBSTR(str, start, len)", "Extract substring"),
                ("UNICODE(str)", "First character Unicode code"),
                ("CHAR(x1, x2, ...)", "Create string from codes"),
                ("HEX(x)", "Convert to hexadecimal"),
                ("ZEROBLOB(n)", "Create n zero bytes"),
                ("LAST_INSERT_ROWID()", "Last inserted rowid"),
                ("CHANGES()", "Rows changed by last statement"),
                ("TOTAL_CHANGES()", "Total rows changed"),
                ("RANDOM()", "Random 64-bit integer"),
                ("ABS(x)", "Absolute value"),
                ("DATE(time, ...)", "Extract date"),
                ("TIME(time, ...)", "Extract time"),
                ("DATETIME(time, ...)", "Date and time"),
                ("JULIANDAY(time)", "Julian day number"),
                ("STRFTIME(fmt, time)", "Format date/time"),
                ("JSON(json)", "Parse JSON"),
                ("JSON_ARRAY(...)", "Create JSON array"),
                ("JSON_OBJECT(...)", "Create JSON object"),
                ("JSON_EXTRACT(json, path)", "Extract JSON value"),
                ("JSON_TYPE(json, path)", "Get JSON type"),
                ("GROUP_CONCAT(x, sep)", "Concatenate group values"),
            ],
            operators: vec![
                ("||", "String concatenation"),
                ("->", "JSON extract (value)"),
                ("->>", "JSON extract (text)"),
                ("GLOB", "Unix pattern match"),
                ("REGEXP", "Regular expression (if loaded)"),
            ],
            data_types: vec![
                ("INTEGER", "Signed integer"),
                ("REAL", "Floating point"),
                ("TEXT", "UTF-8 text"),
                ("BLOB", "Binary data"),
                ("NUMERIC", "Numeric affinity"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id INTEGER PRIMARY KEY AUTOINCREMENT,\n  $2\n)", "Create table"),
                ("idx", "CREATE INDEX $1 ON $2 ($3)", "Create index"),
                ("uidx", "CREATE UNIQUE INDEX $1 ON $2 ($3)", "Create unique index"),
                ("vac", "VACUUM", "Vacuum database"),
                ("pragma", "PRAGMA $1", "Pragma statement"),
            ],
        }.with_standard_sql()
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        let mut conn = SqliteDbConnection::new(config);
        conn.connect().await?;
        Ok(Box::new(conn))
    }

    async fn list_databases(&self, _connection: &dyn DbConnection) -> Result<Vec<String>> {
        Ok(vec!["main".to_string()])
    }

    async fn list_databases_view(&self, _connection: &dyn DbConnection) -> Result<ObjectView> {
        use gpui::px;

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
        ];

        let rows = vec![vec!["main".to_string()]];

        Ok(ObjectView {
            db_node_type: DbNodeType::Database,
            title: "1 database(s)".to_string(),
            columns,
            rows,
        })
    }

    async fn list_databases_detailed(&self, _connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        Ok(vec![DatabaseInfo {
            name: "main".to_string(),
            charset: None,
            collation: None,
            size: None,
            table_count: None,
            comment: None,
        }])
    }

    async fn list_tables(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<TableInfo>> {
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TableInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    schema: None,
                    comment: None,
                    engine: None,
                    row_count: None,
                    create_time: None,
                    charset: None,
                    collation: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let tables = self.list_tables(connection, database).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];

        let rows: Vec<Vec<String>> = tables.iter().map(|table| {
            vec![table.name.clone()]
        }).collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Table,
            title: format!("{} table(s)", tables.len()),
            columns,
            rows,
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, _database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!("PRAGMA table_info(\"{}\")", table);

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ColumnInfo {
                    name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    data_type: row.get(2).and_then(|v| v.clone()).unwrap_or_default(),
                    is_nullable: row.get(3).and_then(|v| v.clone()).map(|v| v == "0").unwrap_or(true),
                    is_primary_key: row.get(5).and_then(|v| v.clone()).map(|v| v == "1").unwrap_or(false),
                    default_value: row.get(4).and_then(|v| v.clone()),
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns_data = self.list_columns(connection, database, table).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("nullable", "Nullable").width(px(80.0)),
            Column::new("key", "Key").width(px(80.0)),
            Column::new("default", "Default").width(px(120.0)),
        ];

        let rows: Vec<Vec<String>> = columns_data.iter().map(|col| {
            vec![
                col.name.clone(),
                col.data_type.clone(),
                if col.is_nullable { "YES" } else { "NO" }.to_string(),
                if col.is_primary_key { "PRI" } else { "" }.to_string(),
                col.default_value.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Column,
            title: format!("{} column(s)", columns_data.len()),
            columns,
            rows,
        })
    }

    async fn list_indexes(&self, connection: &dyn DbConnection, _database: &str, table: &str) -> Result<Vec<IndexInfo>> {
        let sql = format!("PRAGMA index_list(\"{}\")", table);

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list indexes: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut indexes = Vec::new();

            for row in query_result.rows {
                let index_name = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
                let is_unique = row.get(2).and_then(|v| v.clone()).map(|v| v == "1").unwrap_or(false);

                let info_sql = format!("PRAGMA index_info(\"{}\")", index_name);
                let info_result = connection.query(&info_sql, None, ExecOptions::default()).await;

                let columns = if let Ok(SqlResult::Query(info_query)) = info_result {
                    info_query.rows.iter()
                        .filter_map(|r| r.get(2).and_then(|v| v.clone()))
                        .collect()
                } else {
                    Vec::new()
                };

                indexes.push(IndexInfo {
                    name: index_name,
                    columns,
                    is_unique,
                    index_type: None,
                });
            }

            Ok(indexes)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let indexes = self.list_indexes(connection, database, table).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("columns", "Columns").width(px(250.0)),
            Column::new("unique", "Unique").width(px(80.0)),
        ];

        let rows: Vec<Vec<String>> = indexes.iter().map(|idx| {
            vec![
                idx.name.clone(),
                idx.columns.join(", "),
                if idx.is_unique { "YES" } else { "NO" }.to_string(),
            ]
        }).collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Index,
            title: format!("{} index(es)", indexes.len()),
            columns,
            rows,
        })
    }

    async fn list_views(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<ViewInfo>> {
        let sql = "SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list views: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ViewInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    schema: None,
                    definition: row.get(1).and_then(|v| v.clone()),
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_views_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let views = self.list_views(connection, database).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("definition", "Definition").width(px(400.0)),
        ];

        let rows: Vec<Vec<String>> = views.iter().map(|view| {
            vec![
                view.name.clone(),
                view.definition.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::View,
            title: format!("{} view(s)", views.len()),
            columns,
            rows,
        })
    }

    async fn list_functions(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_functions_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];

        Ok(ObjectView {
            db_node_type: DbNodeType::Function,
            title: "0 function(s)".to_string(),
            columns,
            rows: vec![],
        })
    }

    async fn list_procedures(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_procedures_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];

        Ok(ObjectView {
            db_node_type: DbNodeType::Procedure,
            title: "0 procedure(s)".to_string(),
            columns,
            rows: vec![],
        })
    }

    async fn list_triggers(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<TriggerInfo>> {
        let sql = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TriggerInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    table_name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    event: String::new(),
                    timing: String::new(),
                    definition: row.get(2).and_then(|v| v.clone()),
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_triggers_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let triggers = self.list_triggers(connection, database).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("table", "Table").width(px(150.0)),
        ];

        let rows: Vec<Vec<String>> = triggers.iter().map(|trigger| {
            vec![
                trigger.name.clone(),
                trigger.table_name.clone(),
            ]
        }).collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Trigger,
            title: format!("{} trigger(s)", triggers.len()),
            columns,
            rows,
        })
    }

    async fn list_sequences(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }

    async fn list_sequences_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];

        Ok(ObjectView {
            db_node_type: DbNodeType::Sequence,
            title: "0 sequence(s)".to_string(),
            columns,
            rows: vec![],
        })
    }

    async fn switch_db(&self, _connection: &dyn DbConnection, _database: &str) -> Result<SqlResult> {
        Ok(SqlResult::Exec(ExecResult {
            sql: String::new(),
            rows_affected: 0,
            elapsed_ms: 0,
            message: Some("SQLite uses single database per file".to_string()),
        }))
    }

    fn get_data_types(&self) -> Vec<DataTypeInfo> {
        vec![
            DataTypeInfo::new("INTEGER", "Signed integer (up to 8 bytes)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("REAL", "8-byte floating point").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("TEXT", "UTF-8 text string").with_category(DataTypeCategory::String),
            DataTypeInfo::new("BLOB", "Binary large object").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("NUMERIC", "Numeric affinity").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("BOOLEAN", "Boolean (stored as INTEGER)").with_category(DataTypeCategory::Boolean),
            DataTypeInfo::new("DATE", "Date (stored as TEXT)").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("DATETIME", "Date and time (stored as TEXT)").with_category(DataTypeCategory::DateTime),
        ]
    }

    fn build_create_database_sql(&self, _request: &crate::plugin::DatabaseOperationRequest) -> String {
        "-- SQLite: database is created when opening a file".to_string()
    }

    fn build_modify_database_sql(&self, _request: &crate::plugin::DatabaseOperationRequest) -> String {
        "-- SQLite: database modification not supported".to_string()
    }

    fn build_drop_database_sql(&self, _database_name: &str) -> String {
        "-- SQLite: delete the database file to drop the database".to_string()
    }

    fn drop_table(&self, _database: &str, table: &str) -> String {
        format!("DROP TABLE IF EXISTS \"{}\"", table)
    }

    fn truncate_table(&self, _database: &str, table: &str) -> String {
        format!("DELETE FROM \"{}\"", table)
    }

    fn rename_table(&self, _database: &str, old_name: &str, new_name: &str) -> String {
        format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", old_name, new_name)
    }

    fn drop_view(&self, _database: &str, view: &str) -> String {
        format!("DROP VIEW IF EXISTS \"{}\"", view)
    }
}

impl Default for SqlitePlugin {
    fn default() -> Self {
        Self::new()
    }
}
