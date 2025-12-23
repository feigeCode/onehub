use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::{analyze_query_editability, analyze_select_editability_fallback, classify_fallback, classify_stmt, is_query_statement_fallback, is_query_stmt};

/// Execution options for SQL script
#[derive(Debug, Clone)]
pub struct ExecOptions {
    /// Whether to stop execution when encountering an error
    pub stop_on_error: bool,
    /// Whether to wrap the entire script in a transaction
    pub transactional: bool,
    /// Maximum number of rows to return for query results
    pub max_rows: Option<usize>,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            stop_on_error: true,
            transactional: false,
            max_rows: Some(1000),
        }
    }
}

/// Result of a single SQL statement execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SqlResult {
    /// Query result (SELECT, SHOW, etc.)
    Query(QueryResult),
    /// Execution result (INSERT, UPDATE, DELETE, DDL, etc.)
    Exec(ExecResult),
    /// Error result
    Error(SqlErrorInfo),
}


impl SqlResult {
    
    pub fn is_error(&self) -> bool {
        matches!(self, SqlResult::Error(_))
    }
}

/// Query result with data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Original SQL statement
    pub sql: String,
    /// Column names
    pub columns: Vec<String>,
    /// Row data (each row is a vector of optional strings)
    pub rows: Vec<Vec<Option<String>>>,
    /// Execution time in milliseconds
    pub elapsed_ms: u128,
    /// Table name if this is a single-table query
    pub table_name: Option<String>,
    /// Whether this result set is editable
    pub editable: bool,
}

/// Execution result for non-query statements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecResult {
    /// Original SQL statement
    pub sql: String,
    /// Number of rows affected
    pub rows_affected: u64,
    /// Execution time in milliseconds
    pub elapsed_ms: u128,
    /// Optional message
    pub message: Option<String>,
}

/// Error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlErrorInfo {
    /// Original SQL statement
    pub sql: String,
    /// Error message
    pub message: String,
}

/// SQL statement type detector
/// Note: For dialect-specific parsing, use DbConnection methods instead
pub struct SqlStatementClassifier;

impl SqlStatementClassifier {
    /// Check if a SQL statement is a query (returns rows)
    /// Uses GenericDialect - for dialect-specific parsing, use DbConnection::is_query_statement
    pub fn is_query_statement(sql: &str) -> bool {
        let dialect = GenericDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
            if let Some(stmt) = statements.first() {
                return is_query_stmt(stmt);
            }
        }
        is_query_statement_fallback(sql)
    }

    /// Check if a SELECT query might be editable (basic heuristic)
    /// Returns None if cannot determine, Some(table_name) if looks like simple single-table query
    /// Uses GenericDialect - for dialect-specific parsing, use DbConnection::analyze_select_editability
    pub fn analyze_select_editability(sql: &str) -> Option<String> {
        let dialect = GenericDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
            if let Some(Statement::Query(query)) = statements.first() {
                return analyze_query_editability(query);
            }
        }
        analyze_select_editability_fallback(sql)
    }

    /// Determine the statement category
    /// Uses GenericDialect - for dialect-specific parsing, use DbConnection::classify_statement
    pub fn classify(sql: &str) -> StatementType {
        let dialect = GenericDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
            if let Some(stmt) = statements.first() {
                return classify_stmt(stmt);
            }
        }
        classify_fallback(sql)
    }

    /// Format execution message based on query type
    pub fn format_message(sql: &str, rows_affected: u64) -> String {
        let trimmed = sql.trim().to_uppercase();

        if trimmed.starts_with("INSERT") {
            format!("Inserted {} row(s)", rows_affected)
        } else if trimmed.starts_with("UPDATE") {
            format!("Updated {} row(s)", rows_affected)
        } else if trimmed.starts_with("DELETE") {
            format!("Deleted {} row(s)", rows_affected)
        } else if trimmed.starts_with("REPLACE") {
            format!("Replaced {} row(s)", rows_affected)
        } else if trimmed.starts_with("CREATE") {
            "Object created successfully".to_string()
        } else if trimmed.starts_with("ALTER") {
            "Object altered successfully".to_string()
        } else if trimmed.starts_with("DROP") {
            "Object dropped successfully".to_string()
        } else if trimmed.starts_with("TRUNCATE") {
            "Table truncated successfully".to_string()
        } else if trimmed.starts_with("RENAME") {
            "Object renamed successfully".to_string()
        } else if trimmed.starts_with("USE") {
            "Database changed successfully".to_string()
        } else if trimmed.starts_with("SET") {
            "Variable set successfully".to_string()
        } else if trimmed.starts_with("BEGIN") || trimmed.starts_with("START TRANSACTION") {
            "Transaction started".to_string()
        } else if trimmed.starts_with("COMMIT") {
            "Transaction committed".to_string()
        } else if trimmed.starts_with("ROLLBACK") {
            "Transaction rolled back".to_string()
        } else {
            format!("Query executed successfully, {} row(s) affected", rows_affected)
        }
    }
}

/// Statement type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    /// Query statement (SELECT, SHOW, etc.)
    Query,
    /// Data manipulation (INSERT, UPDATE, DELETE)
    Dml,
    /// Data definition (CREATE, ALTER, DROP)
    Ddl,
    /// Transaction control (BEGIN, COMMIT, ROLLBACK)
    Transaction,
    /// Database commands (USE, SET)
    Command,
    /// Other execution statements
    Exec,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_query_statements() {
        assert!(SqlStatementClassifier::is_query_statement("SELECT * FROM users"));
        assert!(SqlStatementClassifier::is_query_statement("select id from table"));
        assert!(SqlStatementClassifier::is_query_statement("SHOW TABLES"));
        assert!(SqlStatementClassifier::is_query_statement("DESCRIBE users"));
        assert!(SqlStatementClassifier::is_query_statement("EXPLAIN SELECT * FROM users"));
        assert!(SqlStatementClassifier::is_query_statement("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    }

    #[test]
    fn test_classify_exec_statements() {
        assert!(!SqlStatementClassifier::is_query_statement("INSERT INTO users VALUES (1)"));
        assert!(!SqlStatementClassifier::is_query_statement("UPDATE users SET name = 'test'"));
        assert!(!SqlStatementClassifier::is_query_statement("DELETE FROM users"));
        assert!(!SqlStatementClassifier::is_query_statement("CREATE TABLE test (id INT)"));
        assert!(!SqlStatementClassifier::is_query_statement("DROP TABLE test"));
    }

    #[test]
    fn test_statement_types() {
        assert_eq!(SqlStatementClassifier::classify("SELECT * FROM users"), StatementType::Query);
        assert_eq!(SqlStatementClassifier::classify("INSERT INTO users VALUES (1)"), StatementType::Dml);
        assert_eq!(SqlStatementClassifier::classify("CREATE TABLE test (id INT)"), StatementType::Ddl);
        assert_eq!(SqlStatementClassifier::classify("BEGIN"), StatementType::Transaction);
        assert_eq!(SqlStatementClassifier::classify("USE mydb"), StatementType::Command);
    }

    #[test]
    fn test_analyze_select_editability() {
        // Simple single-table queries should be editable (return Some)
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT * FROM users").is_some());
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT id, name FROM users WHERE id > 10").is_some());

        // Aggregate functions should NOT be editable (return None)
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT COUNT(*) FROM login_user").is_none());
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT SUM(amount) FROM orders").is_none());
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT AVG(price) FROM products").is_none());
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT MAX(score), MIN(score) FROM results").is_none());

        // JOIN queries should NOT be editable
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT u.*, o.* FROM users u JOIN orders o").is_none());

        // GROUP BY should NOT be editable
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT city, COUNT(*) FROM users GROUP BY city").is_none());

        // DISTINCT should NOT be editable
        assert!(SqlStatementClassifier::analyze_select_editability("SELECT DISTINCT city FROM users").is_none());
    }
}
