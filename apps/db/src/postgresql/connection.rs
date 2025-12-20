use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use one_core::storage::DbConnectionConfig;
use tokio::sync::{Mutex};
use tokio_postgres::{Client, Config, NoTls, Row, types::Type};

use crate::connection::{DbConnection, DbError, StreamingProgress};
use crate::executor::{ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter, SqlStatementClassifier};
use tokio::sync::mpsc;
use crate::SqlValue;

pub struct PostgresDbConnection {
    config: DbConnectionConfig,
    client: Arc<Mutex<Option<Client>>>,
}

impl PostgresDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
        }
    }

    /// Extract value from PostgreSQL row
    fn extract_value(row: &Row, index: usize) -> Option<String> {
        // Get column type
        let column = &row.columns()[index];
        let col_type = column.type_();

        // Try to get the value based on type
        match col_type {
            // NULL
            _ if row.try_get::<_, Option<String>>(index).ok().flatten().is_none() => None,

            // Boolean
            &Type::BOOL => row.try_get::<_, bool>(index).ok().map(|v| v.to_string()),

            // Integer types
            &Type::INT2 => row.try_get::<_, i16>(index).ok().map(|v| v.to_string()),
            &Type::INT4 => row.try_get::<_, i32>(index).ok().map(|v| v.to_string()),
            &Type::INT8 => row.try_get::<_, i64>(index).ok().map(|v| v.to_string()),

            // Floating point types
            &Type::FLOAT4 => row.try_get::<_, f32>(index).ok().map(|v| v.to_string()),
            &Type::FLOAT8 => row.try_get::<_, f64>(index).ok().map(|v| v.to_string()),

            // Numeric/Decimal - try as string first
            &Type::NUMERIC => {
                // Try to get as string representation
                if let Ok(Some(val)) = row.try_get::<_, Option<String>>(index) {
                    return Some(val);
                }
                // Fallback to f64
                row.try_get::<_, f64>(index).ok().map(|v| v.to_string())
            },

            // Text types
            &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME => {
                row.try_get::<_, String>(index).ok()
            },

            // Date and Time types
            &Type::TIMESTAMP => {
                use chrono::NaiveDateTime;
                row.try_get::<_, NaiveDateTime>(index)
                    .ok()
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S").to_string())
            },
            &Type::TIMESTAMPTZ => {
                use chrono::{DateTime, Utc};
                row.try_get::<_, DateTime<Utc>>(index)
                    .ok()
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S %z").to_string())
            },
            &Type::DATE => {
                use chrono::NaiveDate;
                row.try_get::<_, NaiveDate>(index)
                    .ok()
                    .map(|v| v.format("%Y-%m-%d").to_string())
            },
            &Type::TIME => {
                use chrono::NaiveTime;
                row.try_get::<_, NaiveTime>(index)
                    .ok()
                    .map(|v| v.format("%H:%M:%S").to_string())
            },

            // Binary types
            &Type::BYTEA => {
                row.try_get::<_, Vec<u8>>(index)
                    .ok()
                    .map(|v| format!("\\x{}", hex::encode(&v)))
            },

            // JSON types
            &Type::JSON | &Type::JSONB => {
                row.try_get::<_, serde_json::Value>(index)
                    .ok()
                    .map(|v| v.to_string())
            },

            // UUID
            &Type::UUID => {
                row.try_get::<_, uuid::Uuid>(index)
                    .ok()
                    .map(|v| v.to_string())
            },

            // Array types - try to get as string representation
            _ if col_type.name().ends_with("[]") => {
                // For arrays, try to get as string
                row.try_get::<_, String>(index).ok()
                    .or_else(|| Some(format!("<array: {}>", col_type.name())))
            },

            // Default: try as string, otherwise show type info
            _ => {
                row.try_get::<_, String>(index)
                    .ok()
                    .or_else(|| Some(format!("<{}>", col_type.name())))
            }
        }
    }

    /// Convert SqlValue to a concrete type for PostgreSQL parameter binding
    /// Returns an enum that implements ToSql + Send + Sync
    fn build_params(params: &[SqlValue]) -> Vec<PgParam> {
        params.iter().map(|param| match param {
            SqlValue::Null => PgParam::Null,
            SqlValue::Bool(v) => PgParam::Bool(*v),
            SqlValue::Int(v) => PgParam::Int(*v),
            SqlValue::Float(v) => PgParam::Float(*v),
            SqlValue::String(v) => PgParam::String(v.clone()),
            SqlValue::Bytes(v) => PgParam::Bytes(v.clone()),
            SqlValue::Json(v) => PgParam::Json(v.clone()),
        }).collect()
    }
}

/// Concrete enum for PostgreSQL parameters that implements ToSql + Send + Sync
#[derive(Debug, Clone)]
enum PgParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl tokio_postgres::types::ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            PgParam::Bool(v) => v.to_sql(ty, out),
            PgParam::Int(v) => v.to_sql(ty, out),
            PgParam::Float(v) => v.to_sql(ty, out),
            PgParam::String(v) => v.to_sql(ty, out),
            PgParam::Bytes(v) => v.to_sql(ty, out),
            PgParam::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <bool as tokio_postgres::types::ToSql>::accepts(ty)
            || <i64 as tokio_postgres::types::ToSql>::accepts(ty)
            || <f64 as tokio_postgres::types::ToSql>::accepts(ty)
            || <String as tokio_postgres::types::ToSql>::accepts(ty)
            || <Vec<u8> as tokio_postgres::types::ToSql>::accepts(ty)
            || <serde_json::Value as tokio_postgres::types::ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}

#[async_trait]
impl DbConnection for PostgresDbConnection {
    fn config(&self) -> &DbConnectionConfig {
        &self.config
    }

    fn set_config_database(&mut self, database: Option<String>) {
        self.config.database = database;
    }

    fn supports_database_switch(&self) -> bool {
        false
    }

    async fn connect(&mut self) -> anyhow::Result<(), DbError> {
        let config = &self.config;

        let mut pg_config = Config::new();
        pg_config
            .host(&config.host)
            .port(config.port)
            .user(&config.username)
            .password(&config.password);

        if let Some(ref db) = config.database {
            pg_config.dbname(db);
        }

        // Connect to PostgreSQL
        let (client, connection) = pg_config
            .connect(NoTls)
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        connection.await.map_err(|e| DbError::ConnectionError(format!("Connection error: {}", e)))?;
        {
            let mut guard = self.client.lock().await;
            *guard = Some(client);
        }

        Ok(())
    }

    async fn current_database(&self) -> Result<Option<String>, DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected".into()))?;

        let row = client
            .query_one("SELECT current_database()", &[])
            .await
            .map_err(|e| DbError::QueryError(format!("Failed to get current database: {}", e)))?;

        Ok(row.try_get::<_, Option<String>>(0).ok().flatten())
    }

    async fn disconnect(&mut self) -> Result<(), DbError> {
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }


    async fn execute(&self, script: &str, options: ExecOptions) -> Result<Vec<SqlResult>, DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected".into()))?;

        // Split script into statements
        let statements = SqlScriptSplitter::split(script);
        let mut results = Vec::new();

        if options.transactional {
            // Start transaction
            let tx = client
                .transaction()
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

                let start = Instant::now();
                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                let result = if is_query {
                    match tx.query(&modified_sql, &[]).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let all_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns,
                                    rows: all_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.to_string(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match tx.execute(&modified_sql, &[]).await {
                        Ok(rows_affected) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let message = SqlStatementClassifier::format_message(sql, rows_affected);

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

                // PostgreSQL doesn't have USE statement, but has SET search_path
                let sql_upper = sql.to_uppercase();
                if sql_upper.starts_with("SET SEARCH_PATH") {
                    let start = Instant::now();
                    match client.execute(sql, &[]).await {
                        Ok(_) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            results.push(SqlResult::Exec(ExecResult {
                                sql: sql.to_string(),
                                rows_affected: 0,
                                elapsed_ms,
                                message: Some("Search path changed".to_string()),
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

                let start = Instant::now();
                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                let result = if is_query {
                    match client.query(&modified_sql, &[]).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let all_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.to_string(),
                                    columns,
                                    rows: all_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
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
                    match client.execute(&modified_sql, &[]).await {
                        Ok(rows_affected) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let message = SqlStatementClassifier::format_message(sql, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.to_string(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
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
        }

        Ok(results)
    }

    async fn query(
        &self,
        query: &str,
        params: Option<Vec<SqlValue>>,
        _options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected".into()))?;

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);
        let query_string = query.to_string();

        if let Some(params) = params {
            // Convert parameters - PostgreSQL uses $1, $2, etc.
            let pg_params = Self::build_params(&params);

            // Convert to references
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                pg_params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

            if is_query {
                match client.query(&query_string, &param_refs).await {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            }))
                        } else {
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            let all_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns,
                                rows: all_rows,
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            }))
                        }
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            } else {
                match client.execute(&query_string, &param_refs).await {
                    Ok(rows_affected) => {
                        let elapsed_ms = start.elapsed().as_millis();
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
            if is_query {
                match client.query(&query_string, &[]).await {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            }))
                        } else {
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

                            let all_rows: Vec<Vec<Option<String>>> = rows
                                .iter()
                                .map(|row| {
                                    (0..columns.len())
                                        .map(|i| Self::extract_value(row, i))
                                        .collect()
                                })
                                .collect();

                            Ok(SqlResult::Query(QueryResult {
                                sql: query_string,
                                columns,
                                rows: all_rows,
                                elapsed_ms,
                                table_name: None,
                                editable: false,
                            }))
                        }
                    }
                    Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                        sql: query_string,
                        message: e.to_string(),
                    })),
                }
            } else {
                match client.execute(&query_string, &[]).await {
                    Ok(rows_affected) => {
                        let elapsed_ms = start.elapsed().as_millis();
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
        }
    }

    async fn execute_streaming(
        &self,
        script: &str,
        options: ExecOptions,
        sender: mpsc::Sender<StreamingProgress>,
    ) -> Result<(), DbError> {
        let mut guard = self.client.lock().await;
        let client = guard.as_mut()
            .ok_or_else(|| DbError::ConnectionError("Not connected".into()))?;

        let statements: Vec<String> = SqlScriptSplitter::split(script)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let total = statements.len();

        if options.transactional {
            let tx = client
                .transaction()
                .await
                .map_err(|e| DbError::QueryError(format!("Failed to begin transaction: {}", e)))?;

            let mut has_error = false;

            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;

                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if SqlStatementClassifier::is_query_statement(&sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.clone()
                    }
                } else {
                    sql.clone()
                };

                let start = Instant::now();
                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                let result = if is_query {
                    match tx.query(&modified_sql, &[]).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let all_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns,
                                    rows: all_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match tx.execute(&modified_sql, &[]).await {
                        Ok(rows_affected) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let message = SqlStatementClassifier::format_message(&sql, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.clone(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                };

                let is_error = result.is_error();
                if is_error {
                    has_error = true;
                }

                let progress = StreamingProgress {
                    current,
                    total,
                    result,
                };

                if sender.send(progress).await.is_err() {
                    break;
                }

                if is_error {
                    break;
                }
            }

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
            for (index, sql) in statements.into_iter().enumerate() {
                let current = index + 1;

                let modified_sql = if let Some(max_rows) = options.max_rows {
                    if SqlStatementClassifier::is_query_statement(&sql)
                        && !sql.to_uppercase().contains(" LIMIT ")
                    {
                        format!("{} LIMIT {}", sql, max_rows)
                    } else {
                        sql.clone()
                    }
                } else {
                    sql.clone()
                };

                let start = Instant::now();
                let is_query = SqlStatementClassifier::is_query_statement(&modified_sql);

                let result = if is_query {
                    match client.query(&modified_sql, &[]).await {
                        Ok(rows) => {
                            let elapsed_ms = start.elapsed().as_millis();

                            if rows.is_empty() {
                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns: Vec::new(),
                                    rows: Vec::new(),
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            } else {
                                let columns: Vec<String> = rows[0]
                                    .columns()
                                    .iter()
                                    .map(|col| col.name().to_string())
                                    .collect();

                                let all_rows: Vec<Vec<Option<String>>> = rows
                                    .iter()
                                    .map(|row| {
                                        (0..columns.len())
                                            .map(|i| Self::extract_value(row, i))
                                            .collect()
                                    })
                                    .collect();

                                SqlResult::Query(QueryResult {
                                    sql: sql.clone(),
                                    columns,
                                    rows: all_rows,
                                    elapsed_ms,
                                    table_name: None,
                                    editable: false,
                                })
                            }
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                } else {
                    match client.execute(&modified_sql, &[]).await {
                        Ok(rows_affected) => {
                            let elapsed_ms = start.elapsed().as_millis();
                            let message = SqlStatementClassifier::format_message(&sql, rows_affected);

                            SqlResult::Exec(ExecResult {
                                sql: sql.clone(),
                                rows_affected,
                                elapsed_ms,
                                message: Some(message),
                            })
                        }
                        Err(e) => SqlResult::Error(SqlErrorInfo {
                            sql: sql.clone(),
                            message: e.to_string(),
                        }),
                    }
                };

                let is_error = result.is_error();
                let progress = StreamingProgress {
                    current,
                    total,
                    result,
                };

                if sender.send(progress).await.is_err() {
                    break;
                }

                if is_error && options.stop_on_error {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn switch_database(&self, database: &str) -> Result<(), DbError> {
        // PostgreSQL doesn't support switching databases within a connection
        // The connection must be recreated to connect to a different database
        Err(DbError::QueryError(
            "PostgreSQL does not support switching databases within a connection. Please create a new connection.".to_string()
        ))
    }
}