use std::sync::RwLock;
use std::time::Instant;

use async_trait::async_trait;
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, SqlitePool};
use one_core::storage::DbConnectionConfig;
use crate::connection::{DbConnection, DbError};
use crate::executor::{
    ExecOptions, ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter,
    SqlStatementClassifier,
};

use crate::types::{SqlValue};

pub struct SqliteDbConnection {
    config: Option<DbConnectionConfig>,
    pool: RwLock<Option<SqlitePool>>,
}

impl SqliteDbConnection {
    pub fn new(config: DbConnectionConfig) -> Self {
        Self {
            config: Some(config),
            pool: RwLock::new(None),
        }
    }

    fn ensure_connected(&self) -> Result<SqlitePool, DbError> {
        self.pool
            .read()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| DbError::ConnectionError("Not connected to database".to_string()))
    }

    fn extract_value(row: &SqliteRow, index: usize) -> Option<String> {
        use sqlx::types::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
        use sqlx::{TypeInfo, ValueRef};

        if let Ok(val) = row.try_get_raw(index) {
            if val.is_null() {
                return None;
            }
        }

        let column = row.column(index);
        let type_name = column.type_info().name().to_uppercase();

        match type_name.as_str() {
            "DATETIME" | "TIMESTAMP" => {
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
            _ => {}
        }

        if let Ok(val) = row.try_get::<String, _>(index) {
            return Some(val);
        }

        if let Ok(val) = row.try_get::<i64, _>(index) {
            return Some(val.to_string());
        }

        if let Ok(val) = row.try_get::<f64, _>(index) {
            return Some(val.to_string());
        }

        if let Ok(val) = row.try_get::<bool, _>(index) {
            return Some(if val { "1" } else { "0" }.to_string());
        }

        if let Ok(val) = row.try_get::<Vec<u8>, _>(index) {
            if let Ok(s) = String::from_utf8(val.clone()) {
                return Some(s);
            }
            return Some(format!("0x{}", hex::encode(&val)));
        }

        Some(format!("<{}>", type_name))
    }
}

#[async_trait]
impl DbConnection for SqliteDbConnection {
    fn config(&self) -> Option<DbConnectionConfig> {
        self.config.clone()
    }

    async fn connect(&mut self) -> Result<(), DbError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| DbError::ConnectionError("No configuration provided".to_string()))?;

        let database_path = config
            .database
            .as_ref()
            .ok_or_else(|| {
                DbError::ConnectionError("Database path is required for SQLite".to_string())
            })?
            .clone();

        let url = format!("sqlite://{}", database_path);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .map_err(|e| DbError::ConnectionError(format!("Failed to connect: {}", e)))?;

        {
            let mut guard = self.pool.write().unwrap();
            *guard = Some(pool);
        }

        Ok(())
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

    async fn execute(
        &self,
        script: &str,
        options: ExecOptions,
    ) -> Result<Vec<SqlResult>, DbError> {
        let pool = self.ensure_connected()?;
        let statements = SqlScriptSplitter::split(script);
        let mut results = Vec::new();

        for sql in statements {
            let sql = sql.trim();
            if sql.is_empty() {
                continue;
            }

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
            let start = Instant::now();

            let result = if is_query {
                let pool = pool.clone();
                let sql_to_exec = modified_sql.clone();
                let original_sql = sql.to_string();

                match sqlx::raw_sql(&sql_to_exec).fetch_all(&pool).await
                {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();

                        if rows.is_empty() {
                            SqlResult::Query(QueryResult {
                                sql: original_sql,
                                columns: Vec::new(),
                                rows: Vec::new(),
                                elapsed_ms,
                            })
                        } else {
                            let columns: Vec<String> = rows[0]
                                .columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect();

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
                let pool = pool.clone();
                let sql_to_exec = modified_sql.clone();
                let original_sql = sql.to_string();

                match sqlx::raw_sql(&sql_to_exec).execute(&pool).await
                {
                    Ok(exec_result) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = exec_result.rows_affected();
                        let message =
                            SqlStatementClassifier::format_message(&original_sql, rows_affected);

                        SqlResult::Exec(ExecResult {
                            sql: original_sql,
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

        Ok(results)
    }

    async fn query(
        &self,
        query: &str,
        _params: Option<Vec<SqlValue>>,
        _options: ExecOptions,
    ) -> Result<SqlResult, DbError> {
        let pool = self.ensure_connected()?;
        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(query);

        let result = if is_query {
            let pool = pool.clone();
            let query_str = query.to_string();
            let query_str_clone = query_str.clone();

            match sqlx::raw_sql(&query_str_clone).fetch_all(&pool).await
            {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();

                    if rows.is_empty() {
                        SqlResult::Query(QueryResult {
                            sql: query_str,
                            columns: Vec::new(),
                            rows: Vec::new(),
                            elapsed_ms,
                        })
                    } else {
                        let columns: Vec<String> = rows[0]
                            .columns()
                            .iter()
                            .map(|col| col.name().to_string())
                            .collect();

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
                Err(e) => SqlResult::Error(SqlErrorInfo {
                    sql: query.to_string(),
                    message: e.to_string(),
                }),
                Err(e) => SqlResult::Error(SqlErrorInfo {
                    sql: query.to_string(),
                    message: e.to_string(),
                }),
            }
        } else {
            let pool = pool.clone();
            let query_str = query.to_string();
            let query_str_clone = query_str.clone();

            match sqlx::raw_sql(&query_str_clone).execute(&pool).await
            {
                Ok(exec_result) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = exec_result.rows_affected();
                    let message =
                        SqlStatementClassifier::format_message(&query_str, rows_affected);

                    SqlResult::Exec(ExecResult {
                        sql: query_str,
                        rows_affected,
                        elapsed_ms,
                        message: Some(message),
                    })
                }
                Err(e) => SqlResult::Error(SqlErrorInfo {
                    sql: query.to_string(),
                    message: e.to_string(),
                }),
            }
        };

        Ok(result)
    }
}
