use std::sync::RwLock;
use std::time::Instant;

use oracle::Connection;

use crate::{
    ExecResult, QueryResult, SqlErrorInfo, SqlResult, SqlScriptSplitter,
    SqlStatementClassifier,
};

pub struct OracleConnection {
    config: Option<OracleConfig>,
    conn: RwLock<Option<Connection>>,
    #[allow(dead_code)]
    current_schema: RwLock<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct OracleConfig {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub service_name: Option<String>,
}

impl OracleConnection {
    pub fn new(config: OracleConfig) -> Self {
        Self {
            config: Some(config),
            conn: RwLock::new(None),
            current_schema: RwLock::new(None),
        }
    }

    fn ensure_connected(&self) -> Result<(), String> {
        let guard = self.conn.read().unwrap();
        if guard.is_none() {
            return Err("Not connected to database".to_string());
        }
        Ok(())
    }

    pub fn connect(&mut self) -> Result<(), String> {
        let config = self.config.as_ref().ok_or("No configuration provided")?;
        
        let connect_string = if let Some(service) = &config.service_name {
            format!("//{}:{}/{}", config.host, config.port, service)
        } else {
            format!("//{}:{}", config.host, config.port)
        };

        let conn = Connection::connect(&config.username, &config.password, &connect_string)
            .map_err(|e| format!("Failed to connect: {}", e))?;

        {
            let mut guard = self.conn.write().unwrap();
            *guard = Some(conn);
        }

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), String> {
        let conn_opt = {
            let mut guard = self.conn.write().unwrap();
            guard.take()
        };
        
        if let Some(conn) = conn_opt {
            let _ = conn.close();
        }
        
        Ok(())
    }

    pub fn execute(&self, sql: &str) -> Result<Vec<SqlResult>, String> {
        self.ensure_connected()?;
        
        let statements = SqlScriptSplitter::split(sql);
        let mut results = Vec::new();

        for stmt in statements {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }

            let start = Instant::now();
            let is_query = SqlStatementClassifier::is_query_statement(stmt);

            let mut guard = self.conn.write().unwrap();
            let conn = guard.as_mut().ok_or("Not connected")?;

            if is_query {
                match conn.query(stmt, &[]) {
                    Ok(rows) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        
                        let column_info = rows.column_info();
                        let columns: Vec<String> = column_info.iter()
                            .map(|col| col.name().to_string())
                            .collect();

                        let mut data_rows = Vec::new();
                        for row_result in rows {
                            match row_result {
                                Ok(row) => {
                                    let mut row_data = Vec::new();
                                    for i in 0..columns.len() {
                                        let value = row.get::<usize, Option<String>>(i)
                                            .unwrap_or(None);
                                        row_data.push(value);
                                    }
                                    data_rows.push(row_data);
                                }
                                Err(_) => continue,
                            }
                        }

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
                match conn.execute(stmt, &[]) {
                    Ok(_) => {
                        let elapsed_ms = start.elapsed().as_millis();
                        let message = SqlStatementClassifier::format_message(stmt, 0);

                        results.push(SqlResult::Exec(ExecResult {
                            sql: stmt.to_string(),
                            rows_affected: 0,
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

    pub fn query(&self, sql: &str) -> Result<SqlResult, String> {
        self.ensure_connected()?;

        let start = Instant::now();
        let is_query = SqlStatementClassifier::is_query_statement(sql);

        let mut guard = self.conn.write().unwrap();
        let conn = guard.as_mut().ok_or("Not connected")?;

        if is_query {
            match conn.query(sql, &[]) {
                Ok(rows) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    
                    let column_info = rows.column_info();
                    let columns: Vec<String> = column_info.iter()
                        .map(|col| col.name().to_string())
                        .collect();

                    let mut data_rows = Vec::new();
                    for row_result in rows {
                        match row_result {
                            Ok(row) => {
                                let mut row_data = Vec::new();
                                for i in 0..columns.len() {
                                    let value = row.get::<usize, Option<String>>(i)
                                        .unwrap_or(None);
                                    row_data.push(value);
                                }
                                data_rows.push(row_data);
                            }
                            Err(_) => continue,
                        }
                    }

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
            match conn.execute(sql, &[]) {
                Ok(_) => {
                    let elapsed_ms = start.elapsed().as_millis();
                    let message = SqlStatementClassifier::format_message(sql, 0);

                    Ok(SqlResult::Exec(ExecResult {
                        sql: sql.to_string(),
                        rows_affected: 0,
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
}
