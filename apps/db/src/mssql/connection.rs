use std::sync::RwLock;
use std::time::Instant;

use tiberius::{Client, Config, AuthMethod, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};

use crate::{
    ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter,
    SqlStatementClassifier,
};

pub struct MssqlConnection {
    config: Option<MssqlConfig>,
    client: RwLock<Option<Client<Compat<TcpStream>>>>,
    current_database: RwLock<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct MssqlConfig {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

impl MssqlConnection {
    pub fn new(config: MssqlConfig) -> Self {
        Self {
            config: Some(config),
            client: RwLock::new(None),
            current_database: RwLock::new(None),
        }
    }

    fn ensure_connected(&self) -> Result<(), String> {
        let guard = self.client.read().unwrap();
        if guard.is_none() {
            return Err("Not connected to database".to_string());
        }
        Ok(())
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        let config = self.config.as_ref().ok_or("No configuration provided")?;
        
        let mut tiberius_config = Config::new();
        tiberius_config.host(&config.host);
        tiberius_config.port(config.port);
        tiberius_config.authentication(AuthMethod::sql_server(&config.username, &config.password));
        
        if let Some(db) = &config.database {
            tiberius_config.database(db);
        }

        let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .map_err(|e| format!("Failed to connect to TCP: {}", e))?;

        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .map_err(|e| format!("Failed to connect to MSSQL: {}", e))?;

        {
            let mut guard = self.client.write().unwrap();
            *guard = Some(client);
        }
        {
            let mut db_guard = self.current_database.write().unwrap();
            *db_guard = config.database.clone();
        }

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), String> {
        let client_opt = {
            let mut guard = self.client.write().unwrap();
            guard.take()
        };
        
        if let Some(_client) = client_opt {
            // Client will be dropped automatically
        }
        
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<SqlResult>, String> {
        self.ensure_connected()?;
        
        let statements = SqlScriptSplitter::split(sql);
        let mut results = Vec::new();

        for stmt in statements {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }

            let start = Instant::now();
            let stmt_upper = stmt.to_uppercase();

            if stmt_upper.starts_with("USE ") {
                let db_name = stmt.trim_start_matches("USE ")
                    .trim_start_matches("use ")
                    .trim()
                    .trim_matches('[')
                    .trim_matches(']')
                    .trim_matches(';')
                    .to_string();

                let mut guard = self.client.write().unwrap();
                let client = guard.as_mut().ok_or("Not connected")?;

                match client.execute(stmt, &[]).await {
                    Ok(_) => {
                        let mut db_guard = self.current_database.write().unwrap();
                        *db_guard = Some(db_name.clone());
                        
                        let elapsed_ms = start.elapsed().as_millis();
                        results.push(SqlResult::Exec(ExecResult {
                            sql: stmt.to_string(),
                            rows_affected: 0,
                            elapsed_ms,
                            message: Some(format!("Database changed to '{}'", db_name)),
                        }));
                    }
                    Err(e) => {
                        results.push(SqlResult::Error(SqlErrorInfo {
                            sql: stmt.to_string(),
                            message: e.to_string(),
                        }));
                    }
                }
                continue;
            }

            let is_query = SqlStatementClassifier::is_query_statement(stmt);

            let mut guard = self.client.write().unwrap();
            let client = guard.as_mut().ok_or("Not connected")?;

            if is_query {
                match client.query(stmt, &[]).await {
                    Ok(stream) => {
                        let rows = stream.into_first_result()
                            .await
                            .map_err(|e| e.to_string())?;

                        let elapsed_ms = start.elapsed().as_millis();

                        let columns: Vec<String> = if let Some(first_row) = rows.first() {
                            first_row.columns()
                                .iter()
                                .map(|col| col.name().to_string())
                                .collect()
                        } else {
                            Vec::new()
                        };

                        let data_rows: Vec<Vec<Option<String>>> = rows.into_iter()
                            .map(|row| {
                                (0..columns.len()).map(|i| {
                                    Self::extract_value(&row, i)
                                }).collect()
                            })
                            .collect();

                        results.push(SqlResult::Query(QueryResult {
                            sql: stmt.to_string(),
                            columns,
                            rows: data_rows,
                            elapsed_ms,
                        }));
                    }
                    Err(e) => {
                        results.push(SqlResult::Error(SqlErrorInfo {
                            sql: stmt.to_string(),
                            message: e.to_string(),
                        }));
                    }
                }
            } else {
                match client.execute(stmt, &[]).await {
                    Ok(result) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let rows_affected = result.total();
                        let message = SqlStatementClassifier::format_message(stmt, rows_affected);

                        results.push(SqlResult::Exec(ExecResult {
                            sql: stmt.to_string(),
                            rows_affected,
                            elapsed_ms,
                            message: Some(message),
                        }));
                    }
                    Err(e) => {
                        results.push(SqlResult::Error(SqlErrorInfo {
                            sql: stmt.to_string(),
                            message: e.to_string(),
                        }));
                    }
                }
            }
        }

        Ok(results)
    }

    pub async fn query(&self, sql: &str) -> Result<SqlResult, String> {
        self.ensure_connected()?;

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(sql);

        let mut guard = self.client.write().unwrap();
        let client = guard.as_mut().ok_or("Not connected")?;

        if is_query {
            match client.query(sql, &[]).await {
                Ok(stream) => {
                    let rows = stream.into_first_result()
                        .await
                        .map_err(|e| e.to_string())?;

                    let elapsed_ms = start.elapsed().as_millis();

                    let columns: Vec<String> = if let Some(first_row) = rows.first() {
                        first_row.columns()
                            .iter()
                            .map(|col| col.name().to_string())
                            .collect()
                    } else {
                        Vec::new()
                    };

                    let data_rows: Vec<Vec<Option<String>>> = rows.into_iter()
                        .map(|row| {
                            (0..columns.len()).map(|i| {
                                Self::extract_value(&row, i)
                            }).collect()
                        })
                        .collect();

                    Ok(SqlResult::Query(QueryResult {
                        sql: sql.to_string(),
                        columns,
                        rows: data_rows,
                        elapsed_ms,
                    }))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql.to_string(),
                    message: e.to_string(),
                })),
            }
        } else {
            match client.execute(sql, &[]).await {
                Ok(result) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let rows_affected = result.total();
                    let message = SqlStatementClassifier::format_message(sql, rows_affected);

                    Ok(SqlResult::Exec(ExecResult {
                        sql: sql.to_string(),
                        rows_affected,
                        elapsed_ms,
                        message: Some(message),
                    }))
                }
                Err(e) => Ok(SqlResult::Error(SqlErrorInfo {
                    sql: sql.to_string(),
                    message: e.to_string(),
                })),
            }
        }
    }

    fn extract_value(row: &Row, index: usize) -> Option<String> {
        row.try_get::<&str, _>(index)
            .ok()
            .flatten()
            .map(|s| s.to_string())
            .or_else(|| {
                row.try_get::<i32, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<i64, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<f64, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
            .or_else(|| {
                row.try_get::<bool, _>(index)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            })
    }
}
