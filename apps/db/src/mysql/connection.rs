use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter, SqlStatementClassifier};
use crate::runtime::TOKIO_HANDLE;
use sqlx::mysql::{MySqlArguments, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, MySql, MySqlPool, Row, ValueRef};
use std::time::Instant;
use async_trait::async_trait;
use std::sync::RwLock;
use sqlx::types::chrono;
use sqlx::types::chrono::Utc;
use one_core::storage::DbConnectionConfig;
use crate::{SqlValue};

pub struct MysqlDbConnection {
    config: Option<DbConnectionConfig>,
    // Active connection pool; wrapped for interior mutability to allow switching DB in execute()
    pool: RwLock<Option<MySqlPool>>,
    // Track current database selected on this connection
    current_database: RwLock<Option<String>>,
}

impl MysqlDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config: Some(config),
            pool: RwLock::new(None),
            current_database: RwLock::new(None),
        }
    }

    fn ensure_connected(&self) -> Result<MySqlPool, DbError> {
        self
            .pool
            .read()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))
    }

    fn bind_parameter(
        query: sqlx::query::Query<MySql, MySqlArguments>,
        param: SqlValue,
    ) -> sqlx::query::Query<MySql, MySqlArguments> {
        match param {
            SqlValue::Null => {
                // Bind NULL as Option::<i32>::None (MySQL supports NULL for any type)
                query.bind(None::<i32>)
            }
            SqlValue::Bool(v) => query.bind(v),
            SqlValue::Int(v) => query.bind(v),
            SqlValue::Float(v) => query.bind(v),
            SqlValue::String(v) => query.bind(v),
            SqlValue::Bytes(v) => query.bind(v),
            SqlValue::Json(v) => query.bind(v.to_string()), // MySQL stores JSON as text
        }
    }
    fn extract_value(row: &MySqlRow, index: usize) -> Option<String> {
        use sqlx::Row;
        use sqlx::TypeInfo;
        use sqlx::types::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
        use sqlx::types::BigDecimal;

        // Check if NULL
        if let Ok(val) = row.try_get_raw(index) {
            if val.is_null() {
                return None;
            }
        }

        // Get column type info for type-specific handling
        let column = row.column(index);
        let type_name = column.type_info().name().to_uppercase();

        // Date and time types - must be checked BEFORE String to avoid incorrect parsing
        // MySQL types: DATE, DATETIME, TIME, YEAR
        // ───────── TIME / DATE / DATETIME / TIMESTAMP 处理 ─────────
        match type_name.as_str() {
            "TIMESTAMP" => {
                // 带时区类型 → DateTime<Utc>
                if let Ok(val) = row.try_get::<chrono::DateTime<Utc>, _>(index) {
                    return Some(val.format("%Y-%m-%d %H:%M:%S").to_string());
                }
            }
            "DATETIME" => {
                if let Ok(val) = row.try_get::<NaiveDateTime, _>(index) {
                    return Some(val.format("%Y-%m-%d %H:%M:%S").to_string());
                }
            }
            "DATE" => {
                if let Ok(val) = row.try_get::<NaiveDate, _>(index) {
                    return Some(val.format("%Y-%m-%d").to_string());
                }
            }
            "TIME" => {
                if let Ok(val) = row.try_get::<NaiveTime, _>(index) {
                    return Some(val.format("%H:%M:%S").to_string());
                }
            }
            "YEAR" => {
                if let Ok(val) = row.try_get::<i16, _>(index) {
                    return Some(val.to_string());
                }
            }
            _ => {}
        }

        // Try different types in order of likelihood
        // String types (VARCHAR, CHAR, TEXT, etc.)
        if let Ok(val) = row.try_get::<String, _>(index) {
            return Some(val);
        }

        // Integer types (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT)
        if let Ok(val) = row.try_get::<i64, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<i32, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<i16, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<i8, _>(index) {
            return Some(val.to_string());
        }

        // Unsigned integer types
        if let Ok(val) = row.try_get::<u64, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<u32, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<u16, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<u8, _>(index) {
            return Some(val.to_string());
        }

        // Floating point types (FLOAT, DOUBLE)
        if let Ok(val) = row.try_get::<f64, _>(index) {
            return Some(val.to_string());
        }
        if let Ok(val) = row.try_get::<f32, _>(index) {
            return Some(val.to_string());
        }

        // DECIMAL/NUMERIC type
        if let Ok(val) = row.try_get::<BigDecimal, _>(index) {
            return Some(val.to_string());
        }

        // Boolean (BOOL, BOOLEAN which are aliases for TINYINT(1))
        if let Ok(val) = row.try_get::<bool, _>(index) {
            return Some(if val { "1" } else { "0" }.to_string());
        }

        // Binary types (BINARY, VARBINARY, BLOB)
        if let Ok(val) = row.try_get::<Vec<u8>, _>(index) {
            // For binary data, try to show as UTF-8 string first, otherwise hex
            if let Ok(s) = String::from_utf8(val.clone()) {
                return Some(s);
            }
            return Some(format!("0x{}", hex::encode(&val)));
        }

        // JSON type
        if let Ok(val) = row.try_get::<serde_json::Value, _>(index) {
            return Some(val.to_string());
        }

        // If all else fails, return column type information
        Some(format!("<{}>", type_name))
    }
}

#[async_trait]
impl DbConnection for MysqlDbConnection {
    fn config(&self) -> Option<DbConnectionConfig> {
        self.config.clone()
    }

    async fn connect(&mut self) -> anyhow::Result<(), DbError> {
        let config = self.config.clone();
        if let Some(conf) = config {
            let clone_conf = conf.clone();
            let url = if let Some(db) = conf.database {
                format!(
                    "mysql://{}:{}@{}:{}/{}",
                    conf.username, conf.password, conf.host, conf.port, db.clone()
                )
            } else {
                format!(
                    "mysql://{}:{}@{}:{}",
                    conf.username, conf.password, conf.host, conf.port
                )
            };

            let pool = TOKIO_HANDLE.spawn(async move {
                MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
            })
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to spawn connection task: {}", e)))?
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect to database: {}", e)))?;
            // Persist pool and current database
            {
                let mut guard = self.pool.write().unwrap();
                *guard = Some(pool);
            }
            {
                let mut db_guard = self.current_database.write().unwrap();
                db_guard.clone_from(&clone_conf.database);
            }
            Ok(())
        }else { 
            Err(DbError::ConnectionError("No database configuration provided".to_string()))
        }
        
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let pool_opt = {
            let mut guard = self.pool.write().unwrap();
            guard.take()
        };
        if let Some(pool) = pool_opt {
            pool.close().await;
        }
        Ok(())
    }


    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let base_pool = self.ensure_connected()?;
        // Work off a current pool; switch pools on USE and persist DB context
        let active_pool = base_pool.clone();

        // Split script into individual statements
        let statements = SqlScriptSplitter::split(script);
        let mut results = Vec::new();


        // Execute each statement on the pool
        for sql in statements {
            let sql = sql.trim();
            if sql.is_empty() {
                continue;
            }

            // Check if this is a USE statement
            let sql_upper = sql.to_uppercase();
            let is_use_statement = sql_upper.starts_with("USE ");

            // If it's a USE statement, execute it directly and update current database
            if is_use_statement {
                let start = Instant::now();
                // Extract database name from USE statement
                let db_name = sql.trim_start_matches("USE ")
                    .trim_start_matches("use ")
                    .trim()
                    .trim_matches('`')
                    .trim_matches(';')
                    .to_string();

                // Execute USE statement on current connection
                let pool = active_pool.clone();
                let sql_to_exec = sql.to_string();
                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&sql_to_exec).execute(&pool).await
                }).await
                {
                    Ok(Ok(_exec_result)) => {
                        // Update current database property
                        {
                            let mut db_guard = self.current_database.write().unwrap();
                            *db_guard = Some(db_name.clone());
                        }
                        let elapsed_ms = start.elapsed().as_millis();
                        results.push(SqlResult::Exec(ExecResult {
                            sql: sql.to_string(),
                            rows_affected: 0,
                            elapsed_ms,
                            message: Some(format!("Database changed to '{}'", db_name)),
                        }));
                        continue;
                    }
                    Ok(Err(e)) => {
                        results.push(SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }));
                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        results.push(SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }));
                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                }
            }

            // Apply max_rows limit for non-USE statements
            let modified_sql = if let Some(max_rows) = options.max_rows {
                if SqlStatementClassifier::is_query_statement(sql) && !sql.to_uppercase().contains(" LIMIT ") {
                    format!("{} LIMIT {}", sql, max_rows)
                } else {
                    sql.to_string()
                }
            } else {
                sql.to_string()
            };

            // Determine statement type
            let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

            let start = Instant::now();
            let result = if is_query {
                // Execute query statement using raw_sql on the active pool - wrap in Tokio context
                let pool = active_pool.clone();
                let sql_to_exec = modified_sql.clone();
                let original_sql = sql.to_string();

                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&sql_to_exec).fetch_all(&pool).await
                }).await {
                    Ok(Ok(rows)) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            SqlResult::Query(QueryResult {
                                sql: original_sql,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                            })
                        } else {
                            // Extract column names
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            // Extract row data
                            let data_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            SqlResult::Query(QueryResult {
                                sql: original_sql,
                                columns,
                                rows: data_rows,
                                elapsed_ms,
                            })
                        }
                    }
                    Ok(Err(e)) => {
                        let result = SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        });

                        results.push(result);

                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        let result = SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        });

                        results.push(result);

                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                }
            } else {
                // Execute non-query statement using raw_sql on the active pool - wrap in Tokio context
                let pool = active_pool.clone();
                let sql_to_exec = modified_sql.clone();
                let original_sql = sql.to_string();

                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&sql_to_exec).execute(&pool).await
                }).await {
                    Ok(Ok(exec_result)) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = exec_result.rows_affected();
                        let message = SqlStatementClassifier::format_message(&original_sql, rows_affected);

                        SqlResult::Exec(ExecResult {
                            sql: original_sql,
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        })
                    }
                    Ok(Err(e)) => {
                        let result = SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        });

                        results.push(result);

                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        let result = SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        });

                        results.push(result);

                        if options.stop_on_error {
                            break;
                        }
                        continue;
                    }
                }
            };

            results.push(result);
        }

        // Connection will be automatically returned to the pool when dropped
        Ok(results)
    }


    async fn query(&self, query: &str, params: Option<Vec<SqlValue>>, options: ExecOptions) -> Result<SqlResult, DbError> {
        let pool = self.ensure_connected()?;
        let start = Instant::now();
        // Determine if it's a query or execution statement
        let is_query = SqlStatementClassifier::is_query_statement(query);

        let result = if let Some(params) = params {
            // Use prepared statement with parameter binding
            if is_query {
                // For SELECT queries with parameters - use raw_sql with parameters
                // Build parameterized query string
                let query_str = query.to_string();
                let params_vec: Vec<String> = params.iter().map(|p| match p {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Bool(v) => v.to_string(),
                    SqlValue::Int(v) => v.to_string(),
                    SqlValue::Float(v) => v.to_string(),
                    SqlValue::String(v) => format!("'{}'", v.replace("'", "''")),
                    SqlValue::Bytes(v) => format!("0x{}", hex::encode(v)),
                    SqlValue::Json(v) => format!("'{}'", v.to_string().replace("'", "''")),
                }).collect();

                // Simple parameter substitution (not ideal, but works for now)
                let mut final_query = query_str.clone();
                for (i, param_val) in params_vec.iter().enumerate() {
                    final_query = final_query.replacen("?", param_val, 1);
                }

                let pool = pool.clone();
                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&final_query).fetch_all(&pool).await
                }).await {
                    Ok(Ok(rows)) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            SqlResult::Query(QueryResult {
                                sql: query_str,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                            })
                        } else {
                            // Extract column names
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            // Extract row data
                            let data_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            SqlResult::Query(QueryResult {
                                sql: query_str,
                                columns,
                                rows: data_rows,
                                elapsed_ms,
                            })
                        }
                    }
                    Ok(Err(e)) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    }),
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    })
                }
            } else {
                // For DML/DDL queries with parameters - use raw_sql with parameters
                let query_str = query.to_string();
                let params_vec: Vec<String> = params.iter().map(|p| match p {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Bool(v) => v.to_string(),
                    SqlValue::Int(v) => v.to_string(),
                    SqlValue::Float(v) => v.to_string(),
                    SqlValue::String(v) => format!("'{}'", v.replace("'", "''")),
                    SqlValue::Bytes(v) => format!("0x{}", hex::encode(v)),
                    SqlValue::Json(v) => format!("'{}'", v.to_string().replace("'", "''")),
                }).collect();

                // Simple parameter substitution
                let mut final_query = query_str.clone();
                for param_val in params_vec.iter() {
                    final_query = final_query.replacen("?", param_val, 1);
                }

                let pool = pool.clone();
                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&final_query).execute(&pool).await
                }).await {
                    Ok(Ok(exec_result)) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = exec_result.rows_affected();
                        let message = SqlStatementClassifier::format_message(&query_str, rows_affected);

                        SqlResult::Exec(ExecResult {
                            sql: query_str,
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        })
                    }
                    Ok(Err(e)) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    }),
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    })
                }
            }
        } else {
            // Use raw SQL without parameter binding (for user input)
            if is_query {
                let pool = pool.clone();
                let query_str = query.to_string();
                let query_str_clone = query_str.clone();
                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&query_str_clone).fetch_all(&pool).await
                }).await {
                    Ok(Ok(rows)) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            SqlResult::Query(QueryResult {
                                sql: query_str,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                            })
                        } else {
                            // Extract column names
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            // Extract row data
                            let data_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            SqlResult::Query(QueryResult {
                                sql: query_str,
                                columns,
                                rows: data_rows,
                                elapsed_ms,
                            })
                        }
                    }
                    Ok(Err(e)) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    }),
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    })
                }
            } else {
                let pool = pool.clone();
                let query_str = query.to_string();
                let query_str_clone = query_str.clone();
                match TOKIO_HANDLE.spawn(async move {
                    sqlx::raw_sql(&query_str_clone).execute(&pool).await
                }).await {
                    Ok(Ok(exec_result)) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = exec_result.rows_affected();
                        let message = SqlStatementClassifier::format_message(&query_str, rows_affected);

                        SqlResult::Exec(ExecResult {
                            sql: query_str,
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        })
                    }
                    Ok(Err(e)) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    }),
                    Err(e) => SqlResult::Error(SqlErrorInfo {
                        sql: query.to_string(),
                        message: e.to_string(),
                    })
                }
            }
        };

        Ok(result)
    }
}
