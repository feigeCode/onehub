use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter, SqlStatementClassifier};
use crate::SqlValue;

use mysql_async::{prelude::*, Conn, Opts, OptsBuilder, Pool, Row, Value};
use std::time::Instant;
use async_trait::async_trait;
use std::sync::RwLock;
use one_core::storage::DbConnectionConfig;

pub struct MysqlDbConnection {
    config: Option<DbConnectionConfig>,
    // Use Pool for connection management
    pool: RwLock<Option<Pool>>,
    // Track current database
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

    fn ensure_connected(&self) -> Result<Pool, DbError> {
        self.pool
            .read()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))
    }

    /// Extract value from mysql_async::Value - much simpler than sqlx!
    fn extract_value(value: &Value) -> Option<String> {
        match value {
            Value::NULL => None,
            Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::UInt(u) => Some(u.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Double(d) => Some(d.to_string()),
            Value::Date(year, month, day, hour, min, sec, micro) => {
                if *hour == 0 && *min == 0 && *sec == 0 && *micro == 0 {
                    // Pure DATE
                    Some(format!("{:04}-{:02}-{:02}", year, month, day))
                } else {
                    // DATETIME or TIMESTAMP
                    Some(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, min, sec
                    ))
                }
            }
            Value::Time(is_neg, days, hours, minutes, seconds, _micros) => {
                let sign = if *is_neg { "-" } else { "" };
                if *days == 0 {
                    Some(format!("{}{}:{:02}:{:02}", sign, hours, minutes, seconds))
                } else {
                    Some(format!(
                        "{}{} {:02}:{:02}:{:02}",
                        sign, days, hours, minutes, seconds
                    ))
                }
            }
        }
    }

    /// Convert SqlValue to mysql_async::Value for parameter binding
    fn convert_param(param: &SqlValue) -> Value {
        match param {
            SqlValue::Null => Value::NULL,
            SqlValue::Bool(v) => Value::Int(*v as i64),
            SqlValue::Int(v) => Value::Int(*v),
            SqlValue::Float(v) => Value::Double(*v),
            SqlValue::String(v) => Value::Bytes(v.as_bytes().to_vec()),
            SqlValue::Bytes(v) => Value::Bytes(v.clone()),
            SqlValue::Json(v) => Value::Bytes(v.to_string().as_bytes().to_vec()),
        }
    }

    /// Execute a single statement and return the result
    async fn execute_single(
        conn: &mut Conn,
        sql: &str,
        is_query: bool,
    ) -> Result<SqlResult, DbError> {
        let start = Instant::now();
        let sql_string = sql.to_string();

        if is_query {
            // Execute SELECT or other query statements
            match conn.query::<Row, _>(sql).await {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();

                    if rows.is_empty() {
                        Ok(SqlResult::Query(QueryResult {
                            sql: sql_string,
                            columns: Vec::new(),
                            rows: Vec::new(),
                            elapsed_ms,
                        }))
                    } else {
                        // Get column names from first row
                        let columns: Vec<String> = rows[0]
                            .columns_ref()
                            .iter()
                            .map(|col| col.name_str().to_string())
                            .collect();

                        // Extract row data
                        let all_rows: Vec<Vec<Option<String>>> = rows
                            .iter()
                            .map(|row| {
                                (0..row.len())
                                    .map(|i| Self::extract_value(&row[i]))
                                    .collect()
                            })
                            .collect();

                        Ok(SqlResult::Query(QueryResult {
                            sql: sql_string,
                            columns,
                            rows: all_rows,
                            elapsed_ms,
                        }))
                    }
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        } else {
            // Execute DML/DDL statements
            match conn.query_drop(sql).await {
                Ok(_) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = conn.affected_rows();
                    let message = SqlStatementClassifier::format_message(&sql_string, rows_affected);

                    Ok(SqlResult::Exec(ExecResult {
                        sql: sql_string,
                        rows_affected,
                        elapsed_ms,
                        message: Some(message),
                    }))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql_string,
                    message: e.to_string(),
                })),
            }
        }
    }
}

#[async_trait]
impl DbConnection for MysqlDbConnection {
    fn config(&self) -> Option<DbConnectionConfig> {
        self.config.clone()
    }

    async fn connect(&mut self) -> anyhow::Result<(), DbError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| DbError::ConnectionError("No database configuration provided".to_string()))?;

        let mut opts_builder = OptsBuilder::default()
            .ip_or_hostname(&config.host)
            .tcp_port(config.port)
            .user(Some(&config.username))
            .pass(Some(&config.password));

        if let Some(ref db) = config.database {
            opts_builder = opts_builder.db_name(Some(db));
        }

        let opts = Opts::from(opts_builder);
        let pool = Pool::new(opts);

        // Test the connection
        let conn = pool
            .get_conn()
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        // Return connection to pool
        drop(conn);

        // Store pool and current database
        {
            let mut guard = self.pool.write().unwrap();
            *guard = Some(pool);
        }
        {
            let mut db_guard = self.current_database.write().unwrap();
            *db_guard = config.database.clone();
        }

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let pool_opt = {
            let mut guard = self.pool.write().unwrap();
            guard.take()
        };

        if let Some(pool) = pool_opt {
            pool.disconnect()
                .await
                .map_err(|e| DbError::ConnectionError(format!("Failed to disconnect: {}", e)))?;
        }

        Ok(())
    }

    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let pool = self.ensure_connected()?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to get connection: {}", e)))?;

        // Split script into statements
        let statements = SqlScriptSplitter::split(script);
        let mut results = Vec::new();

        if options.transactional {
            // Start transaction
            let mut tx = conn
                .start_transaction(Default::default())
                .await
                .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;

            for sql in statements {
                let sql = sql.trim();
                if sql.is_empty() {
                    continue;
                }

                // Apply max_rows limit
                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if SqlStatementClassifier::is_query_statement(sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.to_string()
                    }
                } else {
                    sql.to_string()
                };

                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                // Execute within transaction
                let start = Instant::now();
                let sql_string = modified_sql.clone();

                let result = if is_query {
                    match tx.query::<Row, _>(&modified_sql).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns_ref()
                                    .iter()
                                    .map(|col| col.name_str().to_string())
                                    .collect();

                                let all_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..row.len())
                                            .map(|i| Self::extract_value(&row[i]))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns,
                                    rows: all_rows,
                                    elapsed_ms,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match tx.query_drop(&modified_sql).await {
                        Ok(_) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let rows_affected = tx.affected_rows();
                            let message = SqlStatementClassifier::format_message(&sql_string, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.to_string(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }),
                    }
                };

                let is_error = result.is_error();
                results.push(result);

                if is_error {
                    break;
                }
            }

            // Commit or rollback
            let has_error = results.iter().any(|r| r.is_error());
            if has_error {
                tx.rollback()
                    .await
                    .map_err(|e| DbError::QueryError(format!("Failed to rollback: {}", e)))?;
            } else {
                tx.commit()
                    .await
                    .map_err(|e| DbError::QueryError(format!("Failed to commit: {}", e)))?;
            }
        } else {
            // Execute statements one by one
            for sql in statements {
                let sql = sql.trim();
                if sql.is_empty() {
                    continue;
                }

                let sql_upper = sql.to_uppercase();
                let is_use_statement = sql_upper.starts_with("USE ");

                // Handle USE statement specially
                if is_use_statement {
                    let start = Instant::now();
                    let db_name = sql
                        .trim_start_matches("USE ")
                        .trim_start_matches("use ")
                        .trim()
                        .trim_matches('`')
                        .trim_matches(';')
                        .to_string();

                    match conn.query_drop(sql).await {
                        Ok(_) => {
                            // Update current database
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
                        }
                        Err(e) => {
                            results.push(SqlResult::Error(SqlErrorInfo {
                                sql: sql.to_string(),
                                message: e.to_string(),
                            }));

                            if options.stop_on_error {
                                break;
                            }
                        }
                    }
                    continue;
                }

                // Apply max_rows limit
                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if SqlStatementClassifier::is_query_statement(sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.to_string()
                    }
                } else {
                    sql.to_string()
                };

                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);
                let result = Self::execute_single(&mut conn, &modified_sql, is_query).await?;

                let is_error = result.is_error();
                results.push(result);

                if is_error && options.stop_on_error {
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn query(
        &self,
        query: &str,
        params: Option<Vec<SqlValue>>,
        options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let pool = self.ensure_connected()?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to get connection: {}", e)))?;

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);
        let query_string = query.to_string();

        if let Some(params) = params {
            // Use prepared statement with parameters
            let mysql_params: Vec<Value> = params.iter().map(Self::convert_param).collect();

            if is_query {
                match conn.exec::<Row, _, _>(query, mysql_params).await {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                            }))
                        } else {
                            let columns: Vec<String> = rows[0]
                                .columns_ref()
                                .iter()
                                .map(|col| col.name_str().to_string())
                                .collect();

                            let all_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..row.len())
                                        .map(|i| Self::extract_value(&row[i]))
                                        .collect()
                                })
                                .collect();

                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns,
                                rows: all_rows,
                                elapsed_ms,
                            }))
                        }
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            } else {
                match conn.exec_drop(query, mysql_params).await {
                    Ok(_) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = conn.affected_rows();
                        let message = SqlStatementClassifier::format_message(&query_string, rows_affected);

                        Ok(SqlResult::Exec(ExecResult {
                            sql: query_string,
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        }))
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            }
        } else {
            // Execute without parameters
            Self::execute_single(&mut conn, query, is_query).await
        }
    }
}