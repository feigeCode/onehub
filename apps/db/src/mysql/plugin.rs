use std::collections::HashMap;

use anyhow::Result;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, SqlResult};
use crate::mysql::connection::MysqlDbConnection;
use crate::plugin::{DatabasePlugin, SqlCompletionInfo};
use crate::types::*;

/// MySQL database plugin implementation (stateless)
pub struct MySqlPlugin;

impl MySqlPlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DatabasePlugin for MySqlPlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::MySQL
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                // MySQL-specific keywords only
                ("AUTO_INCREMENT", "Auto-increment column attribute"),
                ("ENGINE", "Storage engine specification"),
                ("CHARSET", "Character set specification"),
                ("COLLATE", "Collation specification"),
                ("UNSIGNED", "Unsigned integer attribute"),
                ("ZEROFILL", "Zero-fill display attribute"),
                ("BINARY", "Binary string comparison"),
                ("IGNORE", "Ignore errors during operation"),
                ("REPLACE", "Replace existing rows"),
                ("DUPLICATE KEY UPDATE", "On duplicate key update"),
                ("STRAIGHT_JOIN", "Force join order"),
                ("SQL_CALC_FOUND_ROWS", "Calculate total rows"),
                ("HIGH_PRIORITY", "High priority query"),
                ("LOW_PRIORITY", "Low priority query"),
                ("DELAYED", "Delayed insert"),
                ("FORCE INDEX", "Force index usage"),
                ("USE INDEX", "Suggest index usage"),
                ("IGNORE INDEX", "Ignore index"),
            ],
            functions: vec![
                // MySQL-specific functions only (standard SQL functions are added via with_standard_sql())
                ("CONCAT_WS(sep, str1, str2, ...)", "Concatenate with separator"),
                ("CHAR_LENGTH(str)", "String length in characters"),
                ("LPAD(str, len, pad)", "Left pad string"),
                ("RPAD(str, len, pad)", "Right pad string"),
                ("LOCATE(substr, str)", "Find substring position"),
                ("INSTR(str, substr)", "Find substring position"),
                ("REPEAT(str, count)", "Repeat string"),
                ("SPACE(n)", "Generate spaces"),
                ("FORMAT(num, decimals)", "Format number"),
                ("TRUNCATE(x, d)", "Truncate to d decimal places"),
                ("POW(x, y)", "Power function"),
                ("RAND()", "Random number 0-1"),
                ("CURDATE()", "Current date"),
                ("CURTIME()", "Current time"),
                ("DATE(expr)", "Extract date part"),
                ("TIME(expr)", "Extract time part"),
                ("YEAR(date)", "Extract year"),
                ("MONTH(date)", "Extract month"),
                ("DAY(date)", "Extract day"),
                ("HOUR(time)", "Extract hour"),
                ("MINUTE(time)", "Extract minute"),
                ("SECOND(time)", "Extract second"),
                ("DAYOFWEEK(date)", "Day of week (1=Sunday)"),
                ("DAYOFMONTH(date)", "Day of month"),
                ("DAYOFYEAR(date)", "Day of year"),
                ("WEEK(date)", "Week number"),
                ("WEEKDAY(date)", "Weekday (0=Monday)"),
                ("DATE_ADD(date, INTERVAL)", "Add interval to date"),
                ("DATE_SUB(date, INTERVAL)", "Subtract interval from date"),
                ("DATEDIFF(date1, date2)", "Difference in days"),
                ("TIMESTAMPDIFF(unit, dt1, dt2)", "Difference in specified unit"),
                ("DATE_FORMAT(date, format)", "Format date"),
                ("STR_TO_DATE(str, format)", "Parse string to date"),
                ("UNIX_TIMESTAMP()", "Current Unix timestamp"),
                ("FROM_UNIXTIME(ts)", "Convert Unix timestamp"),
                ("GROUP_CONCAT(col)", "Concatenate group values"),
                ("IF(cond, then, else)", "Conditional expression"),
                ("IFNULL(expr, alt)", "Return alt if expr is NULL"),
                ("JSON_EXTRACT(doc, path)", "Extract JSON value"),
                ("JSON_UNQUOTE(json)", "Unquote JSON string"),
                ("JSON_OBJECT(key, val, ...)", "Create JSON object"),
                ("JSON_ARRAY(val, ...)", "Create JSON array"),
                ("JSON_CONTAINS(doc, val)", "Check if JSON contains value"),
                ("JSON_LENGTH(doc)", "JSON document length"),
                ("CONVERT(expr, type)", "Type conversion"),
                ("UUID()", "Generate UUID"),
                ("LAST_INSERT_ID()", "Last auto-increment ID"),
                ("FOUND_ROWS()", "Rows found by previous query"),
                ("ROW_COUNT()", "Affected rows count"),
                ("DATABASE()", "Current database name"),
                ("USER()", "Current user"),
                ("VERSION()", "MySQL version"),
            ],
            operators: vec![
                ("REGEXP", "Regular expression match"),
                ("RLIKE", "Regular expression match (alias)"),
                ("SOUNDS LIKE", "Soundex comparison"),
                ("<=>", "NULL-safe equal"),
                ("DIV", "Integer division"),
                ("XOR", "Logical XOR"),
                (":=", "Assignment operator"),
            ],
            data_types: vec![
                ("TINYINT", "1 byte integer"),
                ("SMALLINT", "2 byte integer"),
                ("MEDIUMINT", "3 byte integer"),
                ("INT", "4 byte integer"),
                ("BIGINT", "8 byte integer"),
                ("DECIMAL(M,D)", "Fixed-point number"),
                ("FLOAT", "Single-precision float"),
                ("DOUBLE", "Double-precision float"),
                ("BIT(M)", "Bit field"),
                ("CHAR(N)", "Fixed-length string"),
                ("VARCHAR(N)", "Variable-length string"),
                ("TINYTEXT", "Tiny text (255 bytes)"),
                ("TEXT", "Text (64KB)"),
                ("MEDIUMTEXT", "Medium text (16MB)"),
                ("LONGTEXT", "Long text (4GB)"),
                ("BINARY(N)", "Fixed-length binary"),
                ("VARBINARY(N)", "Variable-length binary"),
                ("TINYBLOB", "Tiny BLOB"),
                ("BLOB", "BLOB (64KB)"),
                ("MEDIUMBLOB", "Medium BLOB (16MB)"),
                ("LONGBLOB", "Long BLOB (4GB)"),
                ("DATE", "Date (YYYY-MM-DD)"),
                ("TIME", "Time (HH:MM:SS)"),
                ("DATETIME", "Date and time"),
                ("TIMESTAMP", "Timestamp"),
                ("YEAR", "Year (4 digits)"),
                ("ENUM('a','b')", "Enumeration"),
                ("SET('a','b')", "Set of values"),
                ("JSON", "JSON document"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id INT AUTO_INCREMENT PRIMARY KEY,\n  $2\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "Create table"),
                ("idx", "CREATE INDEX $1 ON $2 ($3)", "Create index"),
                ("alt", "ALTER TABLE $1 ADD COLUMN $2", "Add column"),
                ("jn", "JOIN $1 ON $2.$3 = $4.$5", "Join clause"),
                ("lj", "LEFT JOIN $1 ON $2.$3 = $4.$5", "Left join clause"),
            ],
        }.with_standard_sql()
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        let mut conn = MysqlDbConnection::new(config);
        conn.connect().await?;
        Ok(Box::new(conn))
    }

    // === Database/Schema Level Operations ===

    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>> {
        let result = connection.query(
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter()
                .filter_map(|row| row.first().and_then(|v| v.clone()))
                .collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView> {
        use gpui::px;
        
        let databases = self.list_databases_detailed(connection).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("charset", "Charset").width(px(120.0)),
            Column::new("collation", "Collation").width(px(180.0)),
            Column::new("size", "Size").width(px(100.0)).text_right(),
            Column::new("tables", "Tables").width(px(80.0)).text_right(),
            Column::new("comment", "Comment").width(px(250.0)),
        ];
        
        let rows: Vec<Vec<String>> = databases.iter().map(|db| {
            vec![
                db.name.clone(),
                db.charset.as_deref().unwrap_or("-").to_string(),
                db.collation.as_deref().unwrap_or("-").to_string(),
                db.size.as_deref().unwrap_or("-").to_string(),
                db.table_count.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                db.comment.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Database,
            title: format!("{} database(s)", databases.len()),
            columns,
            rows,
        })
    }

    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        let result = connection.query(
            "SELECT 
                s.SCHEMA_NAME as name,
                s.DEFAULT_CHARACTER_SET_NAME as charset,
                s.DEFAULT_COLLATION_NAME as collation,
                COUNT(t.TABLE_NAME) as table_count
            FROM INFORMATION_SCHEMA.SCHEMATA s
            LEFT JOIN INFORMATION_SCHEMA.TABLES t 
                ON s.SCHEMA_NAME = t.TABLE_SCHEMA AND t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY s.SCHEMA_NAME, s.DEFAULT_CHARACTER_SET_NAME, s.DEFAULT_COLLATION_NAME
            ORDER BY s.SCHEMA_NAME",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let databases: Vec<DatabaseInfo> = query_result.rows.iter()
                .filter_map(|row| {
                    let name = row.first().and_then(|v| v.clone())?;
                    let charset = row.get(1).and_then(|v| v.clone());
                    let collation = row.get(2).and_then(|v| v.clone());
                    let table_count = row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse::<i64>().ok());
                    
                    Some(DatabaseInfo {
                        name,
                        charset,
                        collation,
                        size: None,
                        table_count,
                        comment: None,
                    })
                })
                .collect();
            Ok(databases)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }


    // === Table Operations ===

    async fn list_tables(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TableInfo>> {
        // Query to get all tables with their description/metadata
        let sql = format!(
            "SELECT \
                TABLE_NAME, \
                TABLE_COMMENT, \
                ENGINE, \
                TABLE_ROWS, \
                CREATE_TIME, \
                TABLE_COLLATION \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
            database
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let tables: Vec<TableInfo> = query_result.rows.iter().map(|row| {
                let collation = row.get(5).and_then(|v| v.clone());
                // Extract charset from collation (e.g., "utf8mb4_general_ci" -> "utf8mb4")
                let charset = collation.as_ref().and_then(|c| {
                    c.split('_').next().map(|s| s.to_string())
                });

                // Parse row count
                let row_count = row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse::<i64>().ok());

                TableInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    schema: None,
                    comment: row.get(1).and_then(|v| v.clone()).filter(|s| !s.is_empty()),
                    engine: row.get(2).and_then(|v| v.clone()),
                    row_count,
                    create_time: row.get(4).and_then(|v| v.clone()),
                    charset,
                    collation,
                }
            }).collect();

            Ok(tables)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let tables = self.list_tables(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("engine", "Engine").width(px(150.0)),
            Column::new("rows", "Rows").width(px(100.0)).text_right(),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("comment", "Comment").width(px(300.0)),
        ];
        
        let rows: Vec<Vec<String>> = tables.iter().map(|table| {
            vec![
                table.name.clone(),
                table.engine.as_deref().unwrap_or("-").to_string(),
                table.row_count.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                table.create_time.as_deref().unwrap_or("-").to_string(),
                table.comment.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Table,
            title: format!("{} table(s)", tables.len()),
            columns,
            rows,
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' \
             ORDER BY ORDINAL_POSITION",
            database, table
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ColumnInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    data_type: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    is_nullable: row.get(2).and_then(|v| v.clone()).map(|v| v == "YES").unwrap_or(true),
                    is_primary_key: row.get(3).and_then(|v| v.clone()).map(|v| v == "PRI").unwrap_or(false),
                    default_value: row.get(4).and_then(|v| v.clone()),
                    comment: row.get(5).and_then(|v| v.clone()),
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
            Column::new("comment", "Comment").width(px(250.0)),
        ];
        
        let rows: Vec<Vec<String>> = columns_data.iter().map(|col| {
            vec![
                col.name.clone(),
                col.data_type.clone(),
                if col.is_nullable { "YES" } else { "NO" }.to_string(),
                if col.is_primary_key { "PRI" } else { "" }.to_string(),
                col.default_value.as_deref().unwrap_or("").to_string(),
                col.comment.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Column,
            title: format!("{} column(s)", columns_data.len()),
            columns,
            rows,
        })
    }

    async fn list_indexes(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, INDEX_TYPE \
             FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            database, table
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list indexes: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut indexes: HashMap<String, IndexInfo> = HashMap::new();

            for row in query_result.rows {
                let index_name = row.first().and_then(|v| v.clone()).unwrap_or_default();
                let column_name = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
                let is_unique = row.get(2).and_then(|v| v.clone()).map(|v| v == "0").unwrap_or(false);
                let index_type = row.get(3).and_then(|v| v.clone());

                indexes.entry(index_name.clone())
                    .or_insert_with(|| IndexInfo {
                        name: index_name,
                        columns: Vec::new(),
                        is_unique,
                        index_type: index_type.clone(),
                    })
                    .columns.push(column_name);
            }

            Ok(indexes.into_values().collect())
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
            Column::new("type", "Type").width(px(120.0)),
        ];
        
        let rows: Vec<Vec<String>> = indexes.iter().map(|idx| {
            vec![
                idx.name.clone(),
                idx.columns.join(", "),
                if idx.is_unique { "YES" } else { "NO" }.to_string(),
                idx.index_type.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Index,
            title: format!("{} index(es)", indexes.len()),
            columns,
            rows,
        })
    }
    // === View Operations ===

    async fn list_views(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<ViewInfo>> {
        let sql = format!(
            "SELECT TABLE_NAME, VIEW_DEFINITION \
             FROM INFORMATION_SCHEMA.VIEWS \
             WHERE TABLE_SCHEMA = '{}' \
             ORDER BY TABLE_NAME",
            database
        );

        let result = connection.query(&sql, None, ExecOptions::default())
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


    // === Function Operations ===

    async fn list_functions(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            "SELECT ROUTINE_NAME, DTD_IDENTIFIER \
             FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA = '{}' AND ROUTINE_TYPE = 'FUNCTION' \
             ORDER BY ROUTINE_NAME",
            database
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list functions: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: row.get(1).and_then(|v| v.clone()),
                    parameters: Vec::new(),
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_functions_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let functions = self.list_functions(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("return_type", "Return Type").width(px(150.0)),
        ];
        
        let rows: Vec<Vec<String>> = functions.iter().map(|func| {
            vec![
                func.name.clone(),
                func.return_type.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Function,
            title: format!("{} function(s)", functions.len()),
            columns,
            rows,
        })
    }


    // === Procedure Operations ===

    async fn list_procedures(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            "SELECT ROUTINE_NAME \
             FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA = '{}' AND ROUTINE_TYPE = 'PROCEDURE' \
             ORDER BY ROUTINE_NAME",
            database
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list procedures: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: None,
                    parameters: Vec::new(),
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_procedures_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let procedures = self.list_procedures(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];
        
        let rows: Vec<Vec<String>> = procedures.iter().map(|proc| {
            vec![proc.name.clone()]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Procedure,
            title: format!("{} procedure(s)", procedures.len()),
            columns,
            rows,
        })
    }

    // === Trigger Operations ===

    async fn list_triggers(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TriggerInfo>> {
        let sql = format!(
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING \
             FROM INFORMATION_SCHEMA.TRIGGERS \
             WHERE TRIGGER_SCHEMA = '{}' \
             ORDER BY TRIGGER_NAME",
            database
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TriggerInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    table_name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    event: row.get(2).and_then(|v| v.clone()).unwrap_or_default(),
                    timing: row.get(3).and_then(|v| v.clone()).unwrap_or_default(),
                    definition: None,
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
            Column::new("event", "Event").width(px(100.0)),
            Column::new("timing", "Timing").width(px(100.0)),
        ];
        
        let rows: Vec<Vec<String>> = triggers.iter().map(|trigger| {
            vec![
                trigger.name.clone(),
                trigger.table_name.clone(),
                trigger.event.clone(),
                trigger.timing.clone(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Trigger,
            title: format!("{} trigger(s)", triggers.len()),
            columns,
            rows,
        })
    }


    // === Sequence Operations ===
    // MySQL doesn't support sequences natively (until MySQL 8.0 which has AUTO_INCREMENT only)
    // Return empty results

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

    fn get_charsets(&self) -> Vec<CharsetInfo> {
        vec![
            CharsetInfo { name: "utf8mb4".into(), description: "UTF-8 Unicode (4 bytes)".into(), default_collation: "utf8mb4_general_ci".into() },
            CharsetInfo { name: "utf8mb3".into(), description: "UTF-8 Unicode (3 bytes)".into(), default_collation: "utf8mb3_general_ci".into() },
            CharsetInfo { name: "utf8".into(), description: "UTF-8 Unicode (alias for utf8mb3)".into(), default_collation: "utf8_general_ci".into() },
            CharsetInfo { name: "latin1".into(), description: "West European (ISO 8859-1)".into(), default_collation: "latin1_swedish_ci".into() },
            CharsetInfo { name: "latin2".into(), description: "Central European (ISO 8859-2)".into(), default_collation: "latin2_general_ci".into() },
            CharsetInfo { name: "ascii".into(), description: "US ASCII".into(), default_collation: "ascii_general_ci".into() },
            CharsetInfo { name: "gbk".into(), description: "GBK Simplified Chinese".into(), default_collation: "gbk_chinese_ci".into() },
            CharsetInfo { name: "gb2312".into(), description: "GB2312 Simplified Chinese".into(), default_collation: "gb2312_chinese_ci".into() },
            CharsetInfo { name: "gb18030".into(), description: "GB18030 Chinese".into(), default_collation: "gb18030_chinese_ci".into() },
            CharsetInfo { name: "big5".into(), description: "Big5 Traditional Chinese".into(), default_collation: "big5_chinese_ci".into() },
            CharsetInfo { name: "sjis".into(), description: "Shift-JIS Japanese".into(), default_collation: "sjis_japanese_ci".into() },
            CharsetInfo { name: "euckr".into(), description: "EUC-KR Korean".into(), default_collation: "euckr_korean_ci".into() },
            CharsetInfo { name: "greek".into(), description: "ISO 8859-7 Greek".into(), default_collation: "greek_general_ci".into() },
            CharsetInfo { name: "hebrew".into(), description: "ISO 8859-8 Hebrew".into(), default_collation: "hebrew_general_ci".into() },
            CharsetInfo { name: "cp1251".into(), description: "Windows Cyrillic".into(), default_collation: "cp1251_general_ci".into() },
            CharsetInfo { name: "cp1256".into(), description: "Windows Arabic".into(), default_collation: "cp1256_general_ci".into() },
            CharsetInfo { name: "binary".into(), description: "Binary pseudo charset".into(), default_collation: "binary".into() },
        ]
    }

    fn get_collations(&self, charset: &str) -> Vec<CollationInfo> {
        match charset {
            "utf8mb4" => vec![
                CollationInfo { name: "utf8mb4_general_ci".into(), charset: "utf8mb4".into(), is_default: true },
                CollationInfo { name: "utf8mb4_unicode_ci".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_unicode_520_ci".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_bin".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_0900_ai_ci".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_0900_as_ci".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_0900_as_cs".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_zh_0900_as_cs".into(), charset: "utf8mb4".into(), is_default: false },
                CollationInfo { name: "utf8mb4_ja_0900_as_cs".into(), charset: "utf8mb4".into(), is_default: false },
            ],
            "utf8mb3" | "utf8" => vec![
                CollationInfo { name: "utf8_general_ci".into(), charset: "utf8".into(), is_default: true },
                CollationInfo { name: "utf8_unicode_ci".into(), charset: "utf8".into(), is_default: false },
                CollationInfo { name: "utf8_bin".into(), charset: "utf8".into(), is_default: false },
            ],
            "latin1" => vec![
                CollationInfo { name: "latin1_swedish_ci".into(), charset: "latin1".into(), is_default: true },
                CollationInfo { name: "latin1_general_ci".into(), charset: "latin1".into(), is_default: false },
                CollationInfo { name: "latin1_general_cs".into(), charset: "latin1".into(), is_default: false },
                CollationInfo { name: "latin1_bin".into(), charset: "latin1".into(), is_default: false },
            ],
            "latin2" => vec![
                CollationInfo { name: "latin2_general_ci".into(), charset: "latin2".into(), is_default: true },
                CollationInfo { name: "latin2_bin".into(), charset: "latin2".into(), is_default: false },
            ],
            "ascii" => vec![
                CollationInfo { name: "ascii_general_ci".into(), charset: "ascii".into(), is_default: true },
                CollationInfo { name: "ascii_bin".into(), charset: "ascii".into(), is_default: false },
            ],
            "gbk" => vec![
                CollationInfo { name: "gbk_chinese_ci".into(), charset: "gbk".into(), is_default: true },
                CollationInfo { name: "gbk_bin".into(), charset: "gbk".into(), is_default: false },
            ],
            "gb2312" => vec![
                CollationInfo { name: "gb2312_chinese_ci".into(), charset: "gb2312".into(), is_default: true },
                CollationInfo { name: "gb2312_bin".into(), charset: "gb2312".into(), is_default: false },
            ],
            "gb18030" => vec![
                CollationInfo { name: "gb18030_chinese_ci".into(), charset: "gb18030".into(), is_default: true },
                CollationInfo { name: "gb18030_bin".into(), charset: "gb18030".into(), is_default: false },
                CollationInfo { name: "gb18030_unicode_520_ci".into(), charset: "gb18030".into(), is_default: false },
            ],
            "big5" => vec![
                CollationInfo { name: "big5_chinese_ci".into(), charset: "big5".into(), is_default: true },
                CollationInfo { name: "big5_bin".into(), charset: "big5".into(), is_default: false },
            ],
            "sjis" => vec![
                CollationInfo { name: "sjis_japanese_ci".into(), charset: "sjis".into(), is_default: true },
                CollationInfo { name: "sjis_bin".into(), charset: "sjis".into(), is_default: false },
            ],
            "euckr" => vec![
                CollationInfo { name: "euckr_korean_ci".into(), charset: "euckr".into(), is_default: true },
                CollationInfo { name: "euckr_bin".into(), charset: "euckr".into(), is_default: false },
            ],
            "greek" => vec![
                CollationInfo { name: "greek_general_ci".into(), charset: "greek".into(), is_default: true },
                CollationInfo { name: "greek_bin".into(), charset: "greek".into(), is_default: false },
            ],
            "hebrew" => vec![
                CollationInfo { name: "hebrew_general_ci".into(), charset: "hebrew".into(), is_default: true },
                CollationInfo { name: "hebrew_bin".into(), charset: "hebrew".into(), is_default: false },
            ],
            "cp1251" => vec![
                CollationInfo { name: "cp1251_general_ci".into(), charset: "cp1251".into(), is_default: true },
                CollationInfo { name: "cp1251_bin".into(), charset: "cp1251".into(), is_default: false },
            ],
            "cp1256" => vec![
                CollationInfo { name: "cp1256_general_ci".into(), charset: "cp1256".into(), is_default: true },
                CollationInfo { name: "cp1256_bin".into(), charset: "cp1256".into(), is_default: false },
            ],
            "binary" => vec![
                CollationInfo { name: "binary".into(), charset: "binary".into(), is_default: true },
            ],
            _ => vec![],
        }
    }

    fn get_data_types(&self) -> Vec<DataTypeInfo> {
        vec![
            // 数值类型
            DataTypeInfo::new("TINYINT", "Very small integer (-128 to 127)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("SMALLINT", "Small integer (-32768 to 32767)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("MEDIUMINT", "Medium integer (-8388608 to 8388607)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("INT", "Standard integer (-2147483648 to 2147483647)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("BIGINT", "Large integer").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("DECIMAL", "Fixed-point number").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("FLOAT", "Single-precision floating-point").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("DOUBLE", "Double-precision floating-point").with_category(DataTypeCategory::Numeric),

            // 字符串类型
            DataTypeInfo::new("CHAR", "Fixed-length string").with_category(DataTypeCategory::String),
            DataTypeInfo::new("VARCHAR", "Variable-length string").with_category(DataTypeCategory::String),
            DataTypeInfo::new("TINYTEXT", "Very small text (255 bytes)").with_category(DataTypeCategory::String),
            DataTypeInfo::new("TEXT", "Text (65,535 bytes)").with_category(DataTypeCategory::String),
            DataTypeInfo::new("MEDIUMTEXT", "Medium text (16MB)").with_category(DataTypeCategory::String),
            DataTypeInfo::new("LONGTEXT", "Large text (4GB)").with_category(DataTypeCategory::String),

            // 日期时间类型
            DataTypeInfo::new("DATE", "Date (YYYY-MM-DD)").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("TIME", "Time (HH:MM:SS)").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("DATETIME", "Date and time").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("TIMESTAMP", "Timestamp with timezone").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("YEAR", "Year (1901-2155)").with_category(DataTypeCategory::DateTime),

            // 二进制类型
            DataTypeInfo::new("BINARY", "Fixed-length binary").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("VARBINARY", "Variable-length binary").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("TINYBLOB", "Very small BLOB (255 bytes)").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("BLOB", "BLOB (65KB)").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("MEDIUMBLOB", "Medium BLOB (16MB)").with_category(DataTypeCategory::Binary),
            DataTypeInfo::new("LONGBLOB", "Large BLOB (4GB)").with_category(DataTypeCategory::Binary),

            // 其他类型
            DataTypeInfo::new("BOOLEAN", "Boolean (TINYINT(1))").with_category(DataTypeCategory::Boolean),
            DataTypeInfo::new("JSON", "JSON document").with_category(DataTypeCategory::Structured),
            DataTypeInfo::new("ENUM", "Enumeration").with_category(DataTypeCategory::Other),
            DataTypeInfo::new("SET", "Set of values").with_category(DataTypeCategory::Other),
        ]
    }

    // === Database Management Operations ===
    fn build_create_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        let charset = request.field_values.get("charset").map(|s| s.as_str()).unwrap_or("utf8mb4");
        let collation = request.field_values.get("collation").map(|s| s.as_str()).unwrap_or("utf8mb4_general_ci");

        format!(
            "CREATE DATABASE `{}` CHARACTER SET {} COLLATE {};",
            db_name, charset, collation
        )
    }

    fn build_modify_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        let charset = request.field_values.get("charset").map(|s| s.as_str()).unwrap_or("utf8mb4");
        let collation = request.field_values.get("collation").map(|s| s.as_str()).unwrap_or("utf8mb4_general_ci");

        format!(
            "ALTER DATABASE `{}` CHARACTER SET {} COLLATE {};",
            db_name, charset, collation
        )
    }

    fn build_drop_database_sql(&self, database_name: &str) -> String {
        format!("DROP DATABASE `{}`;", database_name)
    }



}

impl Default for MySqlPlugin {
    fn default() -> Self {
        Self::new()
    }
}
