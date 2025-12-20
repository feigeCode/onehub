use serde::{Deserialize, Serialize};

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

/// SQL script splitter
pub struct SqlScriptSplitter;

impl SqlScriptSplitter {
    /// Split SQL script into individual statements
    /// Handles string literals, comments, and multi-line statements
    pub fn split(script: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut chars = script.chars().peekable();

        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while let Some(ch) = chars.next() {
            // Handle line comments (-- or #)
            if !in_single_quote && !in_double_quote && !in_backtick && !in_block_comment {
                if ch == '-' {
                    if let Some(&next_ch) = chars.peek() {
                        if next_ch == '-' {
                            chars.next(); // consume the second '-'
                            in_line_comment = true;
                            continue;
                        }
                    }
                } else if ch == '#' {
                    in_line_comment = true;
                    continue;
                }
            }

            // Handle end of line comment
            if in_line_comment {
                if ch == '\n' {
                    in_line_comment = false;
                    current_statement.push(ch);
                }
                continue;
            }

            // Handle block comments (/* ... */)
            if !in_single_quote && !in_double_quote && !in_backtick
                && ch == '/' {
                    if let Some(&next_ch) = chars.peek() {
                        if next_ch == '*' {
                            chars.next(); // consume the '*'
                            in_block_comment = true;
                            continue;
                        }
                    }
                }

            if in_block_comment {
                if ch == '*' {
                    if let Some(&next_ch) = chars.peek() {
                        if next_ch == '/' {
                            chars.next(); // consume the '/'
                            in_block_comment = false;
                            continue;
                        }
                    }
                }
                continue;
            }

            // Handle string literals
            if ch == '\'' && !in_double_quote && !in_backtick {
                in_single_quote = !in_single_quote;
                current_statement.push(ch);
                continue;
            }

            if ch == '"' && !in_single_quote && !in_backtick {
                in_double_quote = !in_double_quote;
                current_statement.push(ch);
                continue;
            }

            if ch == '`' && !in_single_quote && !in_double_quote {
                in_backtick = !in_backtick;
                current_statement.push(ch);
                continue;
            }

            // Handle semicolon (statement separator)
            if ch == ';' && !in_single_quote && !in_double_quote && !in_backtick {
                let trimmed = current_statement.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current_statement.clear();
                continue;
            }

            current_statement.push(ch);
        }

        // Add the last statement if it's not empty
        let trimmed = current_statement.trim();
        if !trimmed.is_empty() {
            statements.push(trimmed.to_string());
        }

        statements
    }
}

/// SQL statement type detector
pub struct SqlStatementClassifier;

impl SqlStatementClassifier {
    /// Check if a SQL statement is a query (returns rows)
    pub fn is_query_statement(sql: &str) -> bool {
        let trimmed = sql.trim().to_uppercase();

        // Query statements that return rows
        trimmed.starts_with("SELECT")
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("DESC")
            || trimmed.starts_with("DESCRIBE")
            || trimmed.starts_with("EXPLAIN")
            || trimmed.starts_with("WITH") // CTE
            || trimmed.starts_with("TABLE") // PostgreSQL TABLE command
            || trimmed.starts_with("PRAGMA") // SQLite PRAGMA (table_info, index_list, etc.)
    }

    /// Check if a SELECT query might be editable (basic heuristic)
    /// Returns None if cannot determine, Some(table_name) if looks like simple single-table query
    pub fn analyze_select_editability(sql: &str) -> Option<String> {
        let upper = sql.trim().to_uppercase();

        // Must be a SELECT statement
        if !upper.starts_with("SELECT") {
            return None;
        }

        // Cannot have these keywords (indicate complex queries)
        let complex_keywords = [
            " JOIN ", " INNER JOIN ", " LEFT JOIN ", " RIGHT JOIN ", " OUTER JOIN ",
            " CROSS JOIN ", " FULL JOIN ",
            " UNION ", " INTERSECT ", " EXCEPT ",
            " GROUP BY ", " HAVING ",
            "DISTINCT", " DISTINCT ",
        ];

        for keyword in &complex_keywords {
            if upper.contains(keyword) {
                return None;
            }
        }

        // Check for aggregate functions in SELECT clause
        let aggregate_functions = [
            "COUNT(", "SUM(", "AVG(", "MAX(", "MIN(",
            "GROUP_CONCAT(", "STRING_AGG(",
        ];

        for func in &aggregate_functions {
            if upper.contains(func) {
                return None;
            }
        }

        // Try to extract table name from "FROM table_name"
        // Simple regex-like parsing
        if let Some(from_pos) = upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..].trim();

            // Extract table name (stop at WHERE, ORDER, LIMIT, semicolon, or whitespace)
            let table_name = after_from
                .split_whitespace()
                .next()?
                .trim_end_matches(';')
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();

            // Check if table name contains subquery or complex syntax
            if table_name.contains('(') || table_name.contains(',') {
                return None;
            }

            return Some(table_name);
        }

        None
    }

    /// Determine the statement category
    pub fn classify(sql: &str) -> StatementType {
        let trimmed = sql.trim().to_uppercase();

        if Self::is_query_statement(sql) {
            return StatementType::Query;
        }

        // DML statements
        if trimmed.starts_with("INSERT")
            || trimmed.starts_with("UPDATE")
            || trimmed.starts_with("DELETE")
            || trimmed.starts_with("REPLACE")
        {
            return StatementType::Dml;
        }

        // DDL statements
        if trimmed.starts_with("CREATE")
            || trimmed.starts_with("ALTER")
            || trimmed.starts_with("DROP")
            || trimmed.starts_with("TRUNCATE")
            || trimmed.starts_with("RENAME")
        {
            return StatementType::Ddl;
        }

        // Transaction control
        if trimmed.starts_with("BEGIN")
            || trimmed.starts_with("COMMIT")
            || trimmed.starts_with("ROLLBACK")
            || trimmed.starts_with("START TRANSACTION")
        {
            return StatementType::Transaction;
        }

        // Database/connection commands
        if trimmed.starts_with("USE") || trimmed.starts_with("SET") {
            return StatementType::Command;
        }

        // Default to execution
        StatementType::Exec
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
    fn test_split_simple_statements() {
        let script = "SELECT * FROM users; INSERT INTO users VALUES (1); DELETE FROM users;";
        let statements = SqlScriptSplitter::split(script);

        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0], "SELECT * FROM users");
        assert_eq!(statements[1], "INSERT INTO users VALUES (1)");
        assert_eq!(statements[2], "DELETE FROM users");
    }

    #[test]
    fn test_split_with_string_literals() {
        let script = r#"INSERT INTO users VALUES ('John; Doe'); SELECT * FROM users WHERE name = 'test;';"#;
        let statements = SqlScriptSplitter::split(script);

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("John; Doe"));
        assert!(statements[1].contains("test;"));
    }

    #[test]
    fn test_split_with_comments() {
        let script = r#"
            -- This is a comment
            SELECT * FROM users; -- inline comment
            # Another comment style
            INSERT INTO users VALUES (1);
            /* Block comment
               spanning multiple lines */
            DELETE FROM users;
        "#;
        let statements = SqlScriptSplitter::split(script);

        assert_eq!(statements.len(), 3);
        assert!(statements[0].contains("SELECT"));
        assert!(statements[1].contains("INSERT"));
        assert!(statements[2].contains("DELETE"));
    }

    #[test]
    fn test_split_multiline_statement() {
        let script = r#"
            SELECT
                id,
                name,
                email
            FROM users
            WHERE active = 1;
        "#;
        let statements = SqlScriptSplitter::split(script);

        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("SELECT"));
        assert!(statements[0].contains("FROM users"));
    }

    #[test]
    fn test_split_no_trailing_semicolon() {
        let script = "SELECT * FROM users";
        let statements = SqlScriptSplitter::split(script);

        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users");
    }

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
