use crate::connection::{
    DbConnection, DbError
};
use crate::executor::{ExecOptions, SqlResult, StatementType};
use crate::types::*;
use anyhow::{Error, Result};
use async_trait::async_trait;
use one_core::storage::query_repository::QueryRepository;
use one_core::storage::{DatabaseType, DbConnectionConfig, GlobalStorageState};
use sqlparser::ast;
use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor};
use sqlparser::dialect::{
    ClickHouseDialect, Dialect, MsSqlDialect, MySqlDialect,
    OracleDialect, PostgreSqlDialect, SQLiteDialect
};
use sqlparser::parser::Parser;
use std::collections::HashMap;

/// Standard SQL functions common to most databases
pub const STANDARD_SQL_FUNCTIONS: &[(&str, &str)] = &[
    // String functions
    ("CONCAT(str1, str2, ...)", "Concatenate strings"),
    ("SUBSTRING(str, pos, len)", "Extract substring"),
    ("LENGTH(str)", "String length"),
    ("UPPER(str)", "Convert to uppercase"),
    ("LOWER(str)", "Convert to lowercase"),
    ("TRIM(str)", "Remove leading/trailing spaces"),
    ("LTRIM(str)", "Remove leading spaces"),
    ("RTRIM(str)", "Remove trailing spaces"),
    ("REPLACE(str, from, to)", "Replace occurrences"),
    ("REVERSE(str)", "Reverse string"),
    ("LEFT(str, len)", "Left substring"),
    ("RIGHT(str, len)", "Right substring"),
    // Numeric functions
    ("ABS(x)", "Absolute value"),
    ("CEIL(x)", "Round up"),
    ("FLOOR(x)", "Round down"),
    ("ROUND(x, d)", "Round to decimal places"),
    ("MOD(x, y)", "Modulo operation"),
    ("POWER(x, y)", "Power function"),
    ("SQRT(x)", "Square root"),
    ("SIGN(x)", "Sign of number (-1, 0, 1)"),
    // Date/Time functions
    ("NOW()", "Current date and time"),
    ("CURRENT_DATE", "Current date"),
    ("CURRENT_TIME", "Current time"),
    ("CURRENT_TIMESTAMP", "Current timestamp"),
    // Aggregate functions
    ("COUNT(*)", "Count rows"),
    ("COUNT(DISTINCT col)", "Count distinct values"),
    ("SUM(col)", "Sum of values"),
    ("AVG(col)", "Average value"),
    ("MIN(col)", "Minimum value"),
    ("MAX(col)", "Maximum value"),
    // Control flow
    ("COALESCE(val1, val2, ...)", "First non-NULL value"),
    ("NULLIF(val1, val2)", "Return NULL if equal"),
    ("CASE WHEN ... THEN ... END", "Case expression"),
    // Type conversion
    ("CAST(expr AS type)", "Type conversion"),
];

/// Standard SQL keywords common to most databases
pub const STANDARD_SQL_KEYWORDS: &[(&str, &str)] = &[
    ("IF EXISTS", "Conditional existence check"),
    ("IF NOT EXISTS", "Conditional non-existence check"),
];

/// SQL completion information for a specific database type
#[derive(Clone, Default)]
pub struct SqlCompletionInfo {
    /// Database-specific keywords (e.g., LIMIT for MySQL, FETCH for PostgreSQL)
    pub keywords: Vec<(&'static str, &'static str)>,
    /// Database-specific functions with documentation
    pub functions: Vec<(&'static str, &'static str)>,
    /// Database-specific operators
    pub operators: Vec<(&'static str, &'static str)>,
    /// Database-specific data types for CREATE TABLE etc.
    pub data_types: Vec<(&'static str, &'static str)>,
    /// Database-specific snippets (e.g., common query patterns)
    pub snippets: Vec<(&'static str, &'static str, &'static str)>, // (label, insert_text, doc)
}

/// Database operation request
#[derive(Clone, Debug)]
pub struct DatabaseOperationRequest {
    pub database_name: String,
    pub field_values: HashMap<String, String>,
}

impl SqlCompletionInfo {
    /// Create completion info with standard SQL functions and keywords included
    pub fn with_standard_sql(mut self) -> Self {
        // Prepend standard functions
        let mut all_functions = STANDARD_SQL_FUNCTIONS.to_vec();
        all_functions.extend(self.functions);
        self.functions = all_functions;

        // Prepend standard keywords
        let mut all_keywords = STANDARD_SQL_KEYWORDS.to_vec();
        all_keywords.extend(self.keywords);
        self.keywords = all_keywords;

        self
    }
}

/// Database plugin trait for supporting multiple database types
#[async_trait]
pub trait DatabasePlugin: Send + Sync {
    fn name(&self) -> DatabaseType;

    fn identifier_quote(&self) -> &str {
        match self.name() {
            DatabaseType::MySQL => "`",
            DatabaseType::PostgreSQL => "\"",
            DatabaseType::SQLite => "\"",
            DatabaseType::MSSQL => "[",
            DatabaseType::Oracle => "\"",
            DatabaseType::ClickHouse => "`",
        }
    }

    /// Get database-specific SQL completion information
    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo::default()
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        match self.name() {
            DatabaseType::MSSQL => format!("[{}]", identifier.replace("]", "]]")),
            _ => {
                let quote = self.identifier_quote();
                format!("{}{}{}", quote, identifier, quote)
            }
        }
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError>;

    // === Database/Schema Level Operations ===
    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>>;

    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView>;
    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>>;

    /// Whether this database supports schemas (e.g., PostgreSQL, MSSQL)
    fn supports_schema(&self) -> bool {
        false
    }

    /// Whether this database supports sequences (e.g., PostgreSQL, Oracle, MSSQL)
    fn supports_sequences(&self) -> bool {
        false
    }

    /// Get the SQL dialect for this database type
    fn sql_dialect(&self) -> Box<dyn Dialect> {
        match self.name() {
            DatabaseType::MySQL => Box::new(MySqlDialect {}),
            DatabaseType::PostgreSQL => Box::new(PostgreSqlDialect {}),
            DatabaseType::MSSQL => Box::new(MsSqlDialect {}),
            DatabaseType::SQLite => Box::new(SQLiteDialect {}),
            DatabaseType::ClickHouse => Box::new(ClickHouseDialect {}),
            DatabaseType::Oracle => Box::new(OracleDialect {})
        }
    }

    /// Split a SQL script into individual statements using this database's dialect
    fn split_statements(&self, script: &str) -> Vec<String> {
        match Parser::parse_sql(self.sql_dialect().as_ref(), script) {
            Ok(statements) => statements.iter().map(|stmt| stmt.to_string()).collect(),
            Err(_) => fallback_split(script),
        }
    }

    /// Check if a SQL statement is a query (returns rows)
    fn is_query_statement(&self, sql: &str) -> bool {
        if let Ok(statements) = Parser::parse_sql(self.sql_dialect().as_ref(), sql) {
            if let Some(stmt) = statements.first() {
                return is_query_stmt(stmt);
            }
        }
        is_query_statement_fallback(sql)
    }

    /// Determine the statement category
    fn classify_statement(&self, sql: &str) -> StatementType {
        if let Ok(statements) = Parser::parse_sql(self.sql_dialect().as_ref(), sql) {
            if let Some(stmt) = statements.first() {
                return classify_stmt(stmt);
            }
        }
        classify_fallback(sql)
    }

    /// Check if a SELECT query might be editable
    /// Returns None if cannot determine, Some(table_name) if looks like simple single-table query
    fn analyze_select_editability(&self, sql: &str) -> Option<String> {
        if let Ok(statements) = Parser::parse_sql(self.sql_dialect().as_ref(), sql) {
            if let Some(Statement::Query(query)) = statements.first() {
                return analyze_query_editability(query);
            }
        }
        analyze_select_editability_fallback(sql)
    }

    /// List schemas in a database (for databases that support schemas)
    async fn list_schemas(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// List schemas view (for databases that support schemas)
    async fn list_schemas_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        Ok(ObjectView::default())
    }

    // === Table Operations ===
    async fn list_tables(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TableInfo>>;

    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;
    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView>;
    async fn list_indexes(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<IndexInfo>>;

    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView>;

    /// List foreign keys for a table
    async fn list_foreign_keys(&self, _connection: &dyn DbConnection, _database: &str, _table: &str) -> Result<Vec<ForeignKeyDefinition>> {
        Ok(Vec::new())
    }

    /// List triggers for a specific table
    async fn list_table_triggers(&self, _connection: &dyn DbConnection, _database: &str, _table: &str) -> Result<Vec<TriggerInfo>> {
        Ok(Vec::new())
    }

    // === View Operations ===
    async fn list_views(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<ViewInfo>>;
    
    async fn list_views_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Function Operations ===
    async fn list_functions(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>>;
    
    async fn list_functions_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Procedure Operations ===
    async fn list_procedures(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>>;
    
    async fn list_procedures_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Trigger Operations ===
    async fn list_triggers(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TriggerInfo>>;

    async fn list_triggers_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Sequence Operations ===
    async fn list_sequences(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<SequenceInfo>>;
    
    async fn list_sequences_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Helper Methods ===
    fn build_column_definition(&self, column: &ColumnInfo, include_name: bool) -> String {
        let mut def = String::new();

        if include_name {
            def.push_str(&self.quote_identifier(&column.name));
            def.push(' ');
        }

        def.push_str(&column.data_type);

        if !column.is_nullable {
            def.push_str(" NOT NULL");
        }

        if let Some(default) = &column.default_value {
            def.push_str(&format!(" DEFAULT {}", default));
        }

        if column.is_primary_key {
            def.push_str(" PRIMARY KEY");
        }

        if let Some(comment) = &column.comment {
            if self.name() == DatabaseType::MySQL {
                def.push_str(&format!(" COMMENT '{}'", comment.replace("'", "''")));
            }
        }

        def
    }

    // === Database Management Operations ===
    /// Build SQL for creating a new database
    fn build_create_database_sql(&self, request: &DatabaseOperationRequest) -> String;

    /// Build SQL for modifying an existing database
    fn build_modify_database_sql(&self, request: &DatabaseOperationRequest) -> String;

    /// Build SQL for dropping a database
    fn build_drop_database_sql(&self, database_name: &str) -> String;

    // === Schema Management Operations ===
    /// Build SQL for creating a new schema
    fn build_create_schema_sql(&self, schema_name: &str) -> String {
        format!("CREATE SCHEMA {}", self.quote_identifier(schema_name))
    }

    /// Build SQL for dropping a schema
    fn build_drop_schema_sql(&self, schema_name: &str) -> String {
        format!("DROP SCHEMA {}", self.quote_identifier(schema_name))
    }

    /// Build SQL for adding/updating schema comment
    /// Returns None if the database doesn't support schema comments
    fn build_comment_schema_sql(&self, _schema_name: &str, _comment: &str) -> Option<String> {
        None
    }

    // === Tree Building ===
    async fn build_database_tree(&self, connection: &dyn DbConnection, node: &DbNode, global_storage_state: &GlobalStorageState) -> Result<Vec<DbNode>> {
        let database = &node.name;
        let id = &node.id;

        if self.supports_schema() {
            let schemas = self.list_schemas(connection, database).await?;
            let mut nodes = Vec::new();

            for schema in schemas {
                let mut metadata: HashMap<String, String> = HashMap::new();
                metadata.insert("database".to_string(), database.to_string());
                metadata.insert("schema".to_string(), schema.clone());

                let schema_node = DbNode::new(
                    format!("{}:schema:{}", id, schema),
                    schema.clone(),
                    DbNodeType::Schema,
                    node.connection_id.clone(),
                    node.database_type
                )
                .with_parent_context(id)
                .with_metadata(metadata);

                nodes.push(schema_node);
            }

            Ok(nodes)
        } else {
            self.build_schema_tree(connection, node, None, global_storage_state).await
        }
    }

    async fn build_schema_tree(&self, connection: &dyn DbConnection, node: &DbNode, schema: Option<&str>, global_storage_state: &GlobalStorageState) -> Result<Vec<DbNode>> {
        let mut nodes = Vec::new();
        let database = node.metadata.as_ref()
            .and_then(|m| m.get("database"))
            .map(|s| s.as_str())
            .unwrap_or(&node.name);
        let id = &node.id;
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("database".to_string(), database.to_string());
        if let Some(s) = schema {
            metadata.insert("schema".to_string(), s.to_string());
        }

        let tables = self.list_tables(connection, database).await?;
        let filtered_tables: Vec<_> = if let Some(s) = schema {
            tables.into_iter().filter(|t| t.schema.as_deref() == Some(s)).collect()
        } else {
            tables
        };
        let table_count = filtered_tables.len();
        let mut table_folder = DbNode::new(
            format!("{}:table_folder", id),
            format!("Tables ({})", table_count),
            DbNodeType::TablesFolder,
            node.connection_id.clone(),
            node.database_type
        ).with_parent_context(id).with_metadata(metadata.clone());
        if table_count > 0 {
            let children: Vec<DbNode> = filtered_tables
                .into_iter()
                .map(|table_info| {
                    let mut meta: HashMap<String, String> = metadata.clone();
                    if let Some(comment) = &table_info.comment {
                        if !comment.is_empty() {
                            meta.insert("comment".to_string(), comment.clone());
                        }
                    }

                    DbNode::new(
                        format!("{}:table_folder:{}", id, table_info.name),
                        table_info.name.clone(),
                        DbNodeType::Table,
                        node.connection_id.clone(),
                        node.database_type
                    )
                    .with_parent_context(format!("{}:table_folder", id))
                    .with_metadata(meta)
                })
                .collect();
            table_folder.set_children(children)
        }
        nodes.push(table_folder);

        let views = self.list_views(connection, database).await?;
        let filtered_views: Vec<_> = if let Some(s) = schema {
            views.into_iter().filter(|v| v.schema.as_deref() == Some(s)).collect()
        } else {
            views
        };
        let view_count = filtered_views.len();
        let mut views_folder = DbNode::new(
            format!("{}:views_folder", id),
            format!("Views ({})", view_count),
            DbNodeType::ViewsFolder,
            node.connection_id.clone(),
            node.database_type
        ).with_parent_context(id).with_metadata(metadata.clone());
        if view_count > 0 {
            let children: Vec<DbNode> = filtered_views
                .into_iter()
                .map(|view| {
                    let mut meta: HashMap<String, String> = metadata.clone();
                    if let Some(comment) = view.comment {
                        meta.insert("comment".to_string(), comment);
                    }

                    let mut vnode = DbNode::new(
                        format!("{}:views_folder:{}", id, view.name),
                        view.name.clone(),
                        DbNodeType::View,
                        node.connection_id.clone(),
                        node.database_type
                    ).with_parent_context(format!("{}:views_folder", id));

                    if !meta.is_empty() {
                        vnode = vnode.with_metadata(meta);
                    }
                    vnode
                })
                .collect();
            views_folder.set_children( children);

        }
        nodes.push(views_folder);

        // Functions folder
        let functions = self.list_functions(connection, database).await.unwrap_or_default();
        let function_count = functions.len();
        let mut functions_folder = DbNode::new(
            format!("{}:functions_folder", id),
            format!("Functions ({})", function_count),
            DbNodeType::FunctionsFolder,
            node.connection_id.clone(),
            node.database_type
        ).with_parent_context(id).with_metadata(metadata.clone());
        if function_count > 0 {
            let children: Vec<DbNode> = functions
                .into_iter()
                .map(|func| {
                    DbNode::new(
                        format!("{}:functions_folder:{}", id, func.name),
                        func.name.clone(),
                        DbNodeType::Function,
                        node.connection_id.clone(),
                        node.database_type
                    )
                    .with_parent_context(format!("{}:functions_folder", id))
                    .with_metadata(metadata.clone())
                })
                .collect();
            functions_folder.set_children(children);
        }
        nodes.push(functions_folder);

        // Procedures folder
        let procedures = self.list_procedures(connection, database).await.unwrap_or_default();
        let procedure_count = procedures.len();
        let mut procedures_folder = DbNode::new(
            format!("{}:procedures_folder", id),
            format!("Procedures ({})", procedure_count),
            DbNodeType::ProceduresFolder,
            node.connection_id.clone(),
            node.database_type
        ).with_parent_context(id).with_metadata(metadata.clone());
        if procedure_count > 0 {
            let children: Vec<DbNode> = procedures
                .into_iter()
                .map(|proc| {
                    DbNode::new(
                        format!("{}:procedures_folder:{}", id, proc.name),
                        proc.name.clone(),
                        DbNodeType::Procedure,
                        node.connection_id.clone(),
                        node.database_type
                    )
                    .with_parent_context(format!("{}:procedures_folder", id))
                    .with_metadata(metadata.clone())
                })
                .collect();
            procedures_folder.set_children(children);
        }
        nodes.push(procedures_folder);

        // Sequences folder (only for databases that support sequences)
        if self.supports_sequences() {
            let sequences = self.list_sequences(connection, database).await.unwrap_or_default();
            let filtered_sequences: Vec<_> = if let Some(s) = schema {
                sequences.into_iter().filter(|seq| {
                    seq.name.starts_with(&format!("{}.", s))
                }).collect()
            } else {
                sequences
            };
            let sequence_count = filtered_sequences.len();
            let mut sequences_folder = DbNode::new(
                format!("{}:sequences_folder", id),
                format!("Sequences ({})", sequence_count),
                DbNodeType::SequencesFolder,
                node.connection_id.clone(),
                node.database_type
            ).with_parent_context(id).with_metadata(metadata.clone());
            if sequence_count > 0 {
                let children: Vec<DbNode> = filtered_sequences
                    .into_iter()
                    .map(|seq| {
                        let mut seq_meta: HashMap<String, String> = metadata.clone();
                        if let Some(start) = seq.start_value {
                            seq_meta.insert("start_value".to_string(), start.to_string());
                        }
                        if let Some(inc) = seq.increment {
                            seq_meta.insert("increment".to_string(), inc.to_string());
                        }
                        if let Some(min) = seq.min_value {
                            seq_meta.insert("min_value".to_string(), min.to_string());
                        }
                        if let Some(max) = seq.max_value {
                            seq_meta.insert("max_value".to_string(), max.to_string());
                        }
                        DbNode::new(
                            format!("{}:sequences_folder:{}", id, seq.name),
                            seq.name.clone(),
                            DbNodeType::Sequence,
                            node.connection_id.clone(),
                            node.database_type
                        )
                        .with_parent_context(format!("{}:sequences_folder", id))
                        .with_metadata(seq_meta)
                    })
                    .collect();
                sequences_folder.set_children(children);
            }
            nodes.push(sequences_folder);
        }

        let queries_folder = self.load_queries(node, metadata.clone(), global_storage_state).await?;
        nodes.push(queries_folder);
        Ok(nodes)
    }

    async fn load_queries(&self, node: &DbNode, metadata: HashMap<String, String>, global_storage_state: &GlobalStorageState) -> std::result::Result<DbNode, Error> {
        let node_id_for_queries = node.id.clone();
        let connection_id_for_queries = node.connection_id.clone();
        let database_name = node.name.clone();  // Database node's name is the database name

        // 获取当前连接的信息
        let conn_repo_arc = global_storage_state.storage.get::<QueryRepository>().await;
        if let Some(conn_repo) = conn_repo_arc {
            let query_repo = (*conn_repo).clone();
            let queries = query_repo.list_by_connection(&connection_id_for_queries).await.unwrap_or_default();
            // Create QueriesFolder node
            let query_count = queries.len();

            // Add database name to metadata
            let mut queries_folder_node = DbNode::new(
                format!("{}:queries_folder", &node_id_for_queries),
                format!("Queries ({})", query_count),
                DbNodeType::QueriesFolder,
                connection_id_for_queries.clone(),
                node.database_type
            )
                .with_parent_context(node_id_for_queries.clone())
                .with_metadata(metadata.clone());

            return if !queries.is_empty() {
                // Add NamedQuery children
                let mut query_nodes = Vec::new();
                for query in queries {
                    let mut query_metadata: HashMap<String, String> = HashMap::new();
                    metadata.iter().for_each(|(k, v)| {
                        query_metadata.insert(k.clone(), v.clone());
                    });
                    // Add query_id to metadata
                    if let Some(qid) = query.id {
                        query_metadata.insert("query_id".to_string(), qid.to_string());
                    }

                    let query_node = DbNode::new(
                        format!("{}:queries_folder:{}", &node_id_for_queries, query.id.unwrap_or(0)),
                        query.name.clone(),
                        DbNodeType::NamedQuery,
                        connection_id_for_queries.clone(),
                        node.database_type
                    )
                        .with_parent_context(format!("{}:queries_folder", &node_id_for_queries))
                        .with_metadata(query_metadata);

                    query_nodes.push(query_node);
                }

                queries_folder_node.set_children(query_nodes);
                Ok(queries_folder_node)
            } else {
                // Add empty QueriesFolder node
                Ok(queries_folder_node)
            }
        }

        // Add database name to metadata
        let mut metadata = HashMap::new();
        metadata.insert("database".to_string(), database_name.clone());

        let queries_folder_node = DbNode::new(
            format!("{}:queries_folder", &node_id_for_queries),
            format!("Queries ({})", 0),
            DbNodeType::QueriesFolder,
            connection_id_for_queries.clone(),
            node.database_type
        )
            .with_parent_context(node_id_for_queries.clone())
            .with_metadata(metadata);
        Ok(queries_folder_node)
    }

    async fn load_node_children(&self, connection: &dyn DbConnection, node: &DbNode, global_storage_state: &GlobalStorageState) -> Result<Vec<DbNode>> {
        let id = &node.id;
        match node.node_type {
            DbNodeType::Connection => {
                let databases = self.list_databases(connection).await?;
                Ok(databases
                    .into_iter()
                    .map(|db| {
                        DbNode::new(format!("{}:{}", &node.id, db), db.clone(), DbNodeType::Database, node.id.clone(), node.database_type)
                            .with_parent_context(id)
                    })
                    .collect())
            }
            DbNodeType::Database => {
                self.build_database_tree(connection, node, global_storage_state).await
            }
            DbNodeType::Schema => {
                let schema_name = node.metadata.as_ref()
                    .and_then(|m| m.get("schema"))
                    .map(|s| s.as_str());
                self.build_schema_tree(connection, node, schema_name, global_storage_state).await
            }
            DbNodeType::TablesFolder | DbNodeType::ViewsFolder |
            DbNodeType::FunctionsFolder | DbNodeType::ProceduresFolder |
            DbNodeType::SequencesFolder | DbNodeType::QueriesFolder => {
                if node.children_loaded {
                    Ok(node.children.clone())
                } else {
                    Ok(Vec::new())
                }
            }
            DbNodeType::Table => {
                let Some(ref metadata) = node.metadata else {
                    return Err(anyhow::anyhow!("表节点缺少 metadata"));
                };
                let Some(db) = metadata.get("database") else {
                    return Err(anyhow::anyhow!("表节点缺少 database 字段"));
                };
                let table = &node.name;
                let mut folder_metadata = HashMap::new();
                folder_metadata.insert("table".to_string(), table.clone());
                metadata.iter().for_each(|(k, v)| {
                    folder_metadata.insert(k.clone(), v.clone());
                });
                let mut children = Vec::new();

                // Columns folder
                let columns = self.list_columns(connection, db, table).await?;
                let column_count = columns.len();
                let mut columns_folder = DbNode::new(
                    format!("{}:columns_folder", id),
                    format!("Columns ({})", column_count),
                    DbNodeType::ColumnsFolder,
                    node.connection_id.clone(),
                    node.database_type
                ).with_parent_context(id)
                    .with_metadata(folder_metadata.clone());

                if column_count > 0 {
                    let column_nodes: Vec<DbNode> = columns
                        .into_iter()
                        .map(|col| {
                            let mut column_metadata = HashMap::new();
                            folder_metadata.iter().for_each(|(k, v)| {
                                column_metadata.insert(k.clone(), v.clone());
                            });
                            column_metadata.insert("type".to_string(),col.data_type);
                            column_metadata.insert("is_nullable".to_string(), col.is_nullable.to_string());
                            column_metadata.insert("is_primary_key".to_string(), col.is_primary_key.to_string());
                            DbNode::new(
                                format!("{}:columns_folder:{}", id, col.name),
                                col.name,
                                DbNodeType::Column,
                                node.connection_id.clone(),
                                node.database_type
                            )
                            .with_metadata(column_metadata)
                            .with_parent_context(format!("{}:columns_folder", id))
                        })
                        .collect();
                    columns_folder.set_children(column_nodes);
                }
                children.push(columns_folder);

                // Indexes folder (excluding primary key index)
                let indexes = self.list_indexes(connection, db, table).await?;
                let non_primary_indexes: Vec<_> = indexes
                    .into_iter()
                    .filter(|idx| idx.name.to_uppercase() != "PRIMARY")
                    .collect();
                let index_count = non_primary_indexes.len();
                let mut indexes_folder = DbNode::new(
                    format!("{}:indexes_folder", id),
                    format!("Indexes ({})", index_count),
                    DbNodeType::IndexesFolder,
                    node.connection_id.clone(),
                    node.database_type
                ).with_parent_context(id)
                .with_metadata(folder_metadata.clone());

                if index_count > 0 {
                    let index_nodes: Vec<DbNode> = non_primary_indexes
                        .into_iter()
                        .map(|idx| {
                            let mut metadata = HashMap::new();
                            folder_metadata.iter().for_each(|(k, v)| {
                                metadata.insert(k.clone(), v.clone());
                            });
                            metadata.insert("unique".to_string(), idx.is_unique.to_string());
                            metadata.insert("columns".to_string(), idx.columns.join(", "));
                            DbNode::new(
                                format!("{}:indexes_folder:{}", id, idx.name),
                                idx.name,
                                DbNodeType::Index,
                                node.connection_id.clone(),
                                node.database_type
                            )
                            .with_metadata(metadata)
                            .with_parent_context(format!("{}:indexes_folder", id))
                        })
                        .collect();
                    indexes_folder.set_children(index_nodes);
                }
                children.push(indexes_folder);

                // Foreign Keys folder
                let foreign_keys = self.list_foreign_keys(connection, db, table).await.unwrap_or_default();
                let fk_count = foreign_keys.len();
                let mut fk_folder = DbNode::new(
                    format!("{}:foreign_keys_folder", id),
                    format!("Foreign Keys ({})", fk_count),
                    DbNodeType::ForeignKeysFolder,
                    node.connection_id.clone(),
                    node.database_type
                ).with_parent_context(id)
                .with_metadata(folder_metadata.clone());

                if fk_count > 0 {
                    let fk_nodes: Vec<DbNode> = foreign_keys
                        .into_iter()
                        .map(|fk| {
                            let mut metadata = HashMap::new();
                            folder_metadata.iter().for_each(|(k, v)| {
                                metadata.insert(k.clone(), v.clone());
                            });
                            metadata.insert("columns".to_string(), fk.columns.join(", "));
                            metadata.insert("ref_table".to_string(), fk.ref_table.clone());
                            metadata.insert("ref_columns".to_string(), fk.ref_columns.join(", "));
                            DbNode::new(
                                format!("{}:foreign_keys_folder:{}", id, fk.name),
                                fk.name,
                                DbNodeType::ForeignKey,
                                node.connection_id.clone(),
                                node.database_type
                            )
                            .with_metadata(metadata)
                            .with_parent_context(format!("{}:foreign_keys_folder", id))
                        })
                        .collect();
                    fk_folder.set_children(fk_nodes);
                }
                children.push(fk_folder);

                // Triggers folder
                let triggers = self.list_table_triggers(connection, db, table).await.unwrap_or_default();
                let trigger_count = triggers.len();
                let mut triggers_folder = DbNode::new(
                    format!("{}:triggers_folder", id),
                    format!("Triggers ({})", trigger_count),
                    DbNodeType::TriggersFolder,
                    node.connection_id.clone(),
                    node.database_type
                ).with_parent_context(id)
                .with_metadata(folder_metadata.clone());

                if trigger_count > 0 {
                    let trigger_nodes: Vec<DbNode> = triggers
                        .into_iter()
                        .map(|trigger| {
                            let mut metadata = HashMap::new();
                            folder_metadata.iter().for_each(|(k, v)| {
                                metadata.insert(k.clone(), v.clone());
                            });
                            metadata.insert("event".to_string(), trigger.event.clone());
                            metadata.insert("timing".to_string(), trigger.timing.clone());
                            DbNode::new(
                                format!("{}:triggers_folder:{}", id, trigger.name),
                                trigger.name,
                                DbNodeType::Trigger,
                                node.connection_id.clone(),
                                node.database_type
                            )
                            .with_metadata(metadata)
                            .with_parent_context(format!("{}:triggers_folder", id))
                        })
                        .collect();
                    triggers_folder.set_children(trigger_nodes);
                }
                children.push(triggers_folder);

                Ok(children)
            }
            DbNodeType::ColumnsFolder | DbNodeType::IndexesFolder |
            DbNodeType::ForeignKeysFolder | DbNodeType::TriggersFolder => {
                if node.children_loaded {
                    Ok(node.children.clone())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Ok(Vec::new()),
        }
    }

 

    // === Table Data Operations ===
    /// Query table data with pagination, filtering and sorting
    async fn query_table_data(
        &self,
        connection: &dyn DbConnection,
        request: &TableDataRequest,
    ) -> Result<TableDataResponse> {
        let start_time = std::time::Instant::now();
        let quote = self.identifier_quote();

        // Get column metadata
        let columns_info = self.list_columns(connection, &request.database, &request.table).await?;
        let columns: Vec<TableColumnMeta> = columns_info
            .iter()
            .enumerate()
            .map(|(i, c)| TableColumnMeta {
                name: c.name.clone(),
                db_type: c.data_type.clone(),
                field_type: FieldType::from_db_type(&c.data_type),
                nullable: c.is_nullable,
                is_primary_key: c.is_primary_key,
                index: i,
            })
            .collect();

        let primary_key_indices: Vec<usize> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.index)
            .collect();

        // Get unique key indices from indexes
        let unique_key_indices = if primary_key_indices.is_empty() {
            let indexes = self.list_indexes(connection, &request.database, &request.table).await.unwrap_or_default();
            // Find first unique index and get its column indices
            indexes
                .iter()
                .find(|idx| idx.is_unique)
                .map(|idx| {
                    idx.columns
                        .iter()
                        .filter_map(|col_name| {
                            columns.iter().find(|c| &c.name == col_name).map(|c| c.index)
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Build WHERE clause: raw clause takes priority, then structured filters
        let where_clause = if let Some(raw_where) = &request.where_clause {
            if raw_where.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", raw_where)
            }
        } else if request.filters.is_empty() {
            String::new()
        } else {
            let conditions: Vec<String> = request
                .filters
                .iter()
                .map(|f| {
                    let col = format!("{}{}{}", quote, f.column, quote);
                    match f.operator {
                        FilterOperator::IsNull => format!("{} IS NULL", col),
                        FilterOperator::IsNotNull => format!("{} IS NOT NULL", col),
                        FilterOperator::In | FilterOperator::NotIn => {
                            format!("{} {} ({})", col, f.operator.to_sql(), f.value)
                        }
                        FilterOperator::Like | FilterOperator::NotLike => {
                            format!("{} {} '{}'", col, f.operator.to_sql(), f.value.replace('\'', "''"))
                        }
                        _ => format!("{} {} '{}'", col, f.operator.to_sql(), f.value.replace('\'', "''"))
                    }
                })
                .collect();
            format!(" WHERE {}", conditions.join(" AND "))
        };

        // Build ORDER BY clause: raw clause takes priority, then structured sorts
        let order_clause = if let Some(raw_order) = &request.order_by_clause {
            if raw_order.is_empty() {
                String::new()
            } else {
                format!(" ORDER BY {}", raw_order)
            }
        } else if request.sorts.is_empty() {
            String::new()
        } else {
            let sorts: Vec<String> = request
                .sorts
                .iter()
                .map(|s| {
                    let dir = match s.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    format!("{}{}{} {}", quote, s.column, quote, dir)
                })
                .collect();
            format!(" ORDER BY {}", sorts.join(", "))
        };

        // Calculate offset
        let offset = (request.page.saturating_sub(1)) * request.page_size;

        // Build count query
        let count_sql = format!(
            "SELECT COUNT(*) FROM {}{}{}.{}{}{}{}",
            quote, request.database, quote,
            quote, request.table, quote,
            where_clause
        );

        // Get total count
        let total_count = match connection.query(&count_sql, None, ExecOptions::default()).await? {
            SqlResult::Query(result) => {
                result.rows.first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_ref())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0)
            }
            _ => 0,
        };

        // Build data query with pagination
        let data_sql = if request.page_size == 0 {
            // Query all records without pagination
            format!(
                "SELECT * FROM {}{}{}.{}{}{}{}{}",
                quote, request.database, quote,
                quote, request.table, quote,
                where_clause,
                order_clause
            )
        } else {
            // Query with pagination
            format!(
                "SELECT * FROM {}{}{}.{}{}{}{}{} LIMIT {} OFFSET {}",
                quote, request.database, quote,
                quote, request.table, quote,
                where_clause,
                order_clause,
                request.page_size,
                offset
            )
        };

        // Execute data query
        let rows = match connection.query(&data_sql, None, ExecOptions::default()).await? {
            SqlResult::Query(result) => result.rows,
            _ => Vec::new(),
        };

        let duration = start_time.elapsed().as_millis();

        Ok(TableDataResponse {
            columns,
            rows,
            total_count,
            page: request.page,
            page_size: request.page_size,
            primary_key_indices,
            unique_key_indices,
            executed_sql: data_sql,
            duration,
        })
    }

    /// Generate SQL preview for table changes without executing them
    fn generate_table_changes_sql(&self, request: &TableSaveRequest) -> String {
        let mut sql_statements = Vec::new();
        
        for change in &request.changes {
            if let Some(sql) = self.build_table_change_sql(request, change) {
                sql_statements.push(sql);
            }
        }

        if sql_statements.is_empty() {
            "-- 没有变更数据".to_string()
        } else {
            sql_statements.join(";\n\n") + ";"
        }
    }

    fn build_table_change_sql(
        &self,
        request: &TableSaveRequest,
        change: &TableRowChange,
    ) -> Option<String> {
        let quote = self.identifier_quote();
        let table_ident = format!(
            "{}{}{}.{}{}{}",
            quote, request.database, quote, quote, request.table, quote
        );

        match change {
            TableRowChange::Added { data } => {
                if data.is_empty() {
                    return None;
                }
                let columns: Vec<String> = request
                    .column_names
                    .iter()
                    .map(|name| self.quote_identifier(name))
                    .collect();
                let values: Vec<String> = data
                    .iter()
                    .map(|value| {
                        if value == "NULL" || value.is_empty() {
                            "NULL".to_string()
                        } else {
                            format!("'{}'", value.replace('\'', "''"))
                        }
                    })
                    .collect();

                Some(format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table_ident,
                    columns.join(", "),
                    values.join(", ")
                ))
            }
            TableRowChange::Updated {
                original_data,
                changes,
            } => {
                if changes.is_empty() {
                    return None;
                }

                let set_clause: Vec<String> = changes
                    .iter()
                    .map(|change| {
                        let column_name = if change.column_name.is_empty() {
                            request
                                .column_names
                                .get(change.column_index)
                                .cloned()
                                .unwrap_or_default()
                        } else {
                            change.column_name.clone()
                        };
                        let ident = self.quote_identifier(&column_name);
                        let value = if change.new_value == "NULL" {
                            "NULL".to_string()
                        } else {
                            format!("'{}'", change.new_value.replace('\'', "''"))
                        };
                        format!("{} = {}", ident, value)
                    })
                    .collect();

                let (where_clause, limit_clause) = self.build_where_and_limit_clause(request, original_data);

                // Handle SQLite rowid subquery for tables without unique key
                if limit_clause == " __SQLITE_ROWID_LIMIT__" {
                    let simple_table = format!("{}{}{}", quote, request.table, quote);
                    Some(format!(
                        "UPDATE {} SET {} WHERE rowid IN (SELECT rowid FROM {} WHERE {} LIMIT 1)",
                        table_ident,
                        set_clause.join(", "),
                        simple_table,
                        where_clause
                    ))
                } else {
                    Some(format!(
                        "UPDATE {} SET {}{}{}{}",
                        table_ident,
                        set_clause.join(", "),
                        if where_clause.is_empty() { "" } else { " WHERE " },
                        where_clause,
                        limit_clause
                    ))
                }
            }
            TableRowChange::Deleted { original_data } => {
                let (where_clause, limit_clause) = self.build_where_and_limit_clause(request, original_data);

                // Handle SQLite rowid subquery for tables without unique key
                if limit_clause == " __SQLITE_ROWID_LIMIT__" {
                    let simple_table = format!("{}{}{}", quote, request.table, quote);
                    Some(format!(
                        "DELETE FROM {} WHERE rowid IN (SELECT rowid FROM {} WHERE {} LIMIT 1)",
                        table_ident,
                        simple_table,
                        where_clause
                    ))
                } else {
                    Some(format!(
                        "DELETE FROM {}{}{}{}",
                        table_ident,
                        if where_clause.is_empty() { "" } else { " WHERE " },
                        where_clause,
                        limit_clause
                    ))
                }
            }
        }
    }

    fn build_limit_clause(&self) -> String {
        match self.name() {
            DatabaseType::MySQL => " LIMIT 1".to_string(),
            DatabaseType::PostgreSQL => " LIMIT 1".to_string(),
            DatabaseType::SQLite => String::new(), // SQLite UPDATE/DELETE does not support LIMIT by default
            DatabaseType::MSSQL => " FETCH FIRST 1 ROWS ONLY".to_string(),
            DatabaseType::Oracle => String::new(),
            DatabaseType::ClickHouse => " LIMIT 1".to_string(),
        }
    }

    fn build_where_and_limit_clause(
        &self,
        request: &TableSaveRequest,
        original_data: &[String],
    ) -> (String, String) {
        let where_clause = self.build_table_change_where_clause(request, original_data);
        let has_unique_key = !request.primary_key_indices.is_empty() || !request.unique_key_indices.is_empty();

        // Add LIMIT/ROWNUM constraint based on database type
        match self.name() {
            DatabaseType::Oracle => {
                // Oracle uses ROWNUM in WHERE clause
                let mut oracle_where = where_clause;
                if oracle_where.is_empty() {
                    oracle_where = "ROWNUM <= 1".to_string();
                } else {
                    oracle_where = format!("{} AND ROWNUM <= 1", oracle_where);
                }
                (oracle_where, String::new())
            }
            DatabaseType::SQLite => {
                // SQLite doesn't support LIMIT in UPDATE/DELETE by default
                // Use rowid subquery when no unique key exists
                if has_unique_key {
                    (where_clause, String::new())
                } else {
                    // Will be handled specially in build_table_change_sql
                    (where_clause, " __SQLITE_ROWID_LIMIT__".to_string())
                }
            }
            _ => {
                // MySQL, PostgreSQL, MSSQL use LIMIT clause
                (where_clause, self.build_limit_clause())
            }
        }
    }

    fn build_table_change_where_clause(
        &self,
        request: &TableSaveRequest,
        original_data: &[String],
    ) -> String {
        // Priority: use primary key > unique key > all columns
        let indices: Vec<usize> = if !request.primary_key_indices.is_empty() {
            request.primary_key_indices.clone()
        } else if !request.unique_key_indices.is_empty() {
            request.unique_key_indices.clone()
        } else {
            // Fallback: use all columns when no primary key or unique key
            (0..request.column_names.len()).collect()
        };

        let mut parts = Vec::new();
        for index in indices {
            if let (Some(column), Some(value)) = (
                request.column_names.get(index),
                original_data.get(index),
            ) {
                let ident = self.quote_identifier(column);
                if value == "NULL" {
                    parts.push(format!("{} IS NULL", ident));
                } else {
                    parts.push(format!(
                        "{} = '{}'",
                        ident,
                        value.replace('\'', "''")
                    ));
                }
            }
        }

        parts.join(" AND ")
    }

    // === Charset and Collation ===
    /// Get list of available character sets for this database
    fn get_charsets(&self) -> Vec<CharsetInfo> {
        vec![]
    }

    /// Get collations for a specific charset
    fn get_collations(&self, _charset: &str) -> Vec<CollationInfo> {
        vec![]
    }

    // === Data Types ===
    /// Get list of available data types for this database
    fn get_data_types(&self) -> Vec<DataTypeInfo> {
        // Default implementation with common types
        vec![
            DataTypeInfo::new("INT", "Integer number"),
            DataTypeInfo::new("VARCHAR(255)", "Variable-length string"),
            DataTypeInfo::new("TEXT", "Long text"),
            DataTypeInfo::new("DATE", "Date"),
            DataTypeInfo::new("DATETIME", "Date and time"),
            DataTypeInfo::new("BOOLEAN", "True/False"),
            DataTypeInfo::new("DECIMAL(10,2)", "Decimal number"),
        ]
    }

    // === DDL Operations ===
    /// Drop database
    fn drop_database(&self, database: &str) -> String {
         format!("DROP DATABASE IF EXISTS {}", self.quote_identifier(database))
    }

    /// Drop table
    fn drop_table(&self, database: &str, table: &str) -> String {
        format!("DROP TABLE IF EXISTS {}.{}", self.quote_identifier( database) , self.quote_identifier(table))
    }

    /// Truncate table
    fn truncate_table(&self, _database: &str, table: &str) -> String {
        format!("TRUNCATE TABLE {}", self.quote_identifier(table))
    }

    /// Rename table
    fn rename_table(&self, _database: &str, old_name: &str, new_name: &str) -> String {
        match self.name() {
            DatabaseType::MySQL | DatabaseType::ClickHouse => format!(
                "RENAME TABLE {} TO {}",
                self.quote_identifier(old_name),
                self.quote_identifier(new_name)
            ),
            DatabaseType::PostgreSQL | DatabaseType::SQLite => format!(
                "ALTER TABLE {} RENAME TO {}",
                self.quote_identifier(old_name),
                self.quote_identifier(new_name)
            ),
            DatabaseType::MSSQL | DatabaseType::Oracle => todo!(),
        }
    }

    /// Drop view
    fn drop_view(&self, _database: &str, view: &str) -> String {
        format!("DROP VIEW IF EXISTS {}", self.quote_identifier(view))
    }

    /// Build column definition from ColumnDefinition (for table designer)
    fn build_column_def(&self, col: &ColumnDefinition) -> String {
        let mut def = String::new();
        def.push_str(&self.quote_identifier(&col.name));
        def.push(' ');

        let mut type_str = col.data_type.clone();
        if let Some(len) = col.length {
            if let Some(scale) = col.scale {
                type_str = format!("{}({},{})", col.data_type, len, scale);
            } else {
                type_str = format!("{}({})", col.data_type, len);
            }
        }
        def.push_str(&type_str);

        if col.is_unsigned && self.name() == DatabaseType::MySQL {
            def.push_str(" UNSIGNED");
        }

        if !col.is_nullable {
            def.push_str(" NOT NULL");
        }

        if col.is_auto_increment {
            match self.name() {
                DatabaseType::MySQL => def.push_str(" AUTO_INCREMENT"),
                DatabaseType::PostgreSQL => {},
                _ => {}
            }
        }

        if let Some(default) = &col.default_value {
            if !default.is_empty() {
                def.push_str(&format!(" DEFAULT {}", default));
            }
        }

        if !col.comment.is_empty() && self.name() == DatabaseType::MySQL {
            def.push_str(&format!(" COMMENT '{}'", col.comment.replace("'", "''")));
        }

        def
    }

    /// Build CREATE TABLE SQL from TableDesign
    fn build_create_table_sql(&self, design: &TableDesign) -> String {
        let mut sql = String::new();
        sql.push_str("CREATE TABLE ");
        sql.push_str(&self.quote_identifier(&design.table_name));
        sql.push_str(" (\n");

        let mut definitions: Vec<String> = Vec::new();

        for col in &design.columns {
            definitions.push(format!("  {}", self.build_column_def(col)));
        }

        let pk_columns: Vec<&str> = design.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect();
        if !pk_columns.is_empty() {
            let pk_cols: Vec<String> = pk_columns.iter()
                .map(|c| self.quote_identifier(c))
                .collect();
            definitions.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
        }

        for idx in &design.indexes {
            if idx.is_primary {
                continue;
            }
            let idx_cols: Vec<String> = idx.columns.iter()
                .map(|c| self.quote_identifier(c))
                .collect();
            let idx_type = if idx.is_unique { "UNIQUE INDEX" } else { "INDEX" };
            definitions.push(format!("  {} {} ({})",
                idx_type,
                self.quote_identifier(&idx.name),
                idx_cols.join(", ")
            ));
        }

        sql.push_str(&definitions.join(",\n"));
        sql.push_str("\n)");

        if self.name() == DatabaseType::MySQL {
            if let Some(engine) = &design.options.engine {
                sql.push_str(&format!(" ENGINE={}", engine));
            }
            if let Some(charset) = &design.options.charset {
                sql.push_str(&format!(" DEFAULT CHARSET={}", charset));
            }
            if let Some(collation) = &design.options.collation {
                sql.push_str(&format!(" COLLATE={}", collation));
            }
            if !design.options.comment.is_empty() {
                sql.push_str(&format!(" COMMENT='{}'", design.options.comment.replace("'", "''")));
            }
        }

        sql.push(';');
        sql
    }

    /// Build ALTER TABLE SQL from original and new TableDesign
    /// Returns a series of ALTER TABLE statements for the differences
    fn build_alter_table_sql(&self, original: &TableDesign, new: &TableDesign) -> String {
        let mut statements: Vec<String> = Vec::new();
        let table_name = self.quote_identifier(&new.table_name);

        // Compare columns
        let original_cols: HashMap<&str, &ColumnDefinition> = original.columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        let new_cols: HashMap<&str, &ColumnDefinition> = new.columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        // Find dropped columns
        for name in original_cols.keys() {
            if !new_cols.contains_key(name) {
                statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {};",
                    table_name,
                    self.quote_identifier(name)
                ));
            }
        }

        // Find added or modified columns
        for (idx, col) in new.columns.iter().enumerate() {
            if let Some(orig_col) = original_cols.get(col.name.as_str()) {
                // Check if column was modified
                if self.column_changed(orig_col, col) {
                    let col_def = self.build_column_def(col);
                    match self.name() {
                        DatabaseType::MySQL => {
                            statements.push(format!(
                                "ALTER TABLE {} MODIFY COLUMN {};",
                                table_name, col_def
                            ));
                        }
                        DatabaseType::PostgreSQL => {
                            // PostgreSQL requires separate ALTER statements for each change
                            let col_name = self.quote_identifier(&col.name);

                            // Change data type
                            if orig_col.data_type != col.data_type || orig_col.length != col.length {
                                let type_str = self.build_type_string(col);
                                statements.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                                    table_name, col_name, type_str
                                ));
                            }

                            // Change nullability
                            if orig_col.is_nullable != col.is_nullable {
                                if col.is_nullable {
                                    statements.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                                        table_name, col_name
                                    ));
                                } else {
                                    statements.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                                        table_name, col_name
                                    ));
                                }
                            }

                            // Change default
                            if orig_col.default_value != col.default_value {
                                if let Some(default) = &col.default_value {
                                    statements.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                                        table_name, col_name, default
                                    ));
                                } else {
                                    statements.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                                        table_name, col_name
                                    ));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                // New column
                let col_def = self.build_column_def(col);
                let position = if idx == 0 {
                    " FIRST".to_string()
                } else if self.name() == DatabaseType::MySQL {
                    format!(" AFTER {}", self.quote_identifier(&new.columns[idx - 1].name))
                } else {
                    String::new()
                };

                statements.push(format!(
                    "ALTER TABLE {} ADD COLUMN {}{};",
                    table_name, col_def, position
                ));
            }
        }

        // Compare indexes
        let original_indexes: HashMap<&str, &IndexDefinition> = original.indexes
            .iter()
            .map(|i| (i.name.as_str(), i))
            .collect();
        let new_indexes: HashMap<&str, &IndexDefinition> = new.indexes
            .iter()
            .map(|i| (i.name.as_str(), i))
            .collect();

        // Find dropped indexes
        for (name, idx) in &original_indexes {
            if !new_indexes.contains_key(name) {
                if idx.is_primary {
                    statements.push(format!(
                        "ALTER TABLE {} DROP PRIMARY KEY;",
                        table_name
                    ));
                } else {
                    match self.name() {
                        DatabaseType::MySQL => {
                            statements.push(format!(
                                "ALTER TABLE {} DROP INDEX {};",
                                table_name,
                                self.quote_identifier(name)
                            ));
                        }
                        DatabaseType::PostgreSQL => {
                            statements.push(format!(
                                "DROP INDEX {};",
                                self.quote_identifier(name)
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }

        // Find added indexes
        for (name, idx) in &new_indexes {
            if !original_indexes.contains_key(name) {
                let idx_cols: Vec<String> = idx.columns.iter()
                    .map(|c| self.quote_identifier(c))
                    .collect();

                if idx.is_primary {
                    statements.push(format!(
                        "ALTER TABLE {} ADD PRIMARY KEY ({});",
                        table_name,
                        idx_cols.join(", ")
                    ));
                } else {
                    let idx_type = if idx.is_unique { "UNIQUE INDEX" } else { "INDEX" };
                    match self.name() {
                        DatabaseType::MySQL => {
                            statements.push(format!(
                                "ALTER TABLE {} ADD {} {} ({});",
                                table_name,
                                idx_type,
                                self.quote_identifier(name),
                                idx_cols.join(", ")
                            ));
                        }
                        DatabaseType::PostgreSQL => {
                            let unique_str = if idx.is_unique { "UNIQUE " } else { "" };
                            statements.push(format!(
                                "CREATE {}INDEX {} ON {} ({});",
                                unique_str,
                                self.quote_identifier(name),
                                table_name,
                                idx_cols.join(", ")
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }

        // Compare table options (MySQL specific)
        if self.name() == DatabaseType::MySQL {
            let mut options_changed = false;
            let mut option_parts: Vec<String> = Vec::new();

            if original.options.engine != new.options.engine
                && original.options.engine.is_some()
                && new.options.engine.is_some()
            {
                if let Some(engine) = &new.options.engine {
                    option_parts.push(format!("ENGINE={}", engine));
                    options_changed = true;
                }
            }

            if original.options.charset != new.options.charset
                && original.options.charset.is_some()
                && new.options.charset.is_some()
            {
                if let Some(charset) = &new.options.charset {
                    option_parts.push(format!("DEFAULT CHARSET={}", charset));
                    options_changed = true;
                }
            }

            if original.options.collation != new.options.collation
                && original.options.collation.is_some()
                && new.options.collation.is_some()
            {
                if let Some(collation) = &new.options.collation {
                    option_parts.push(format!("COLLATE={}", collation));
                    options_changed = true;
                }
            }

            if original.options.comment != new.options.comment
                && !original.options.comment.is_empty()
                && !new.options.comment.is_empty()
            {
                option_parts.push(format!("COMMENT='{}'", new.options.comment.replace("'", "''")));
                options_changed = true;
            }

            if options_changed && !option_parts.is_empty() {
                statements.push(format!(
                    "ALTER TABLE {} {};",
                    table_name,
                    option_parts.join(" ")
                ));
            }
        }

        if statements.is_empty() {
            "-- No changes detected".to_string()
        } else {
            statements.join("\n")
        }
    }

    /// Check if a column definition has changed
    fn column_changed(&self, original: &ColumnDefinition, new: &ColumnDefinition) -> bool {
        original.data_type.to_uppercase() != new.data_type.to_uppercase()
            || original.length != new.length
            || original.precision != new.precision
            || original.scale != new.scale
            || original.is_nullable != new.is_nullable
            || original.is_auto_increment != new.is_auto_increment
            || original.is_unsigned != new.is_unsigned
            || original.default_value != new.default_value
            || original.comment != new.comment
            || original.charset != new.charset
            || original.collation != new.collation
    }

    /// Build type string for a column (used in ALTER statements)
    fn build_type_string(&self, col: &ColumnDefinition) -> String {
        let mut type_str = col.data_type.clone();
        if let Some(len) = col.length {
            if let Some(scale) = col.scale {
                type_str = format!("{}({},{})", type_str, len, scale);
            } else {
                type_str = format!("{}({})", type_str, len);
            }
        }
        type_str
    }

}


/// Split SQL statements using sqlparser's parser with the given dialect
pub fn split_statements_with_dialect(script: &str, dialect: &dyn Dialect) -> Vec<String> {
    match Parser::parse_sql(dialect, script) {
        Ok(statements) => {
            statements.iter().map(|stmt| stmt.to_string()).collect()
        }
        Err(_) => fallback_split(script),
    }
}

pub fn fallback_split(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut paren_depth = 0i32;
    let mut chars = script.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_string {
            current.push(ch);
            if ch == string_char {
                if chars.peek() == Some(&string_char) {
                    current.push(chars.next().unwrap());
                } else {
                    in_string = false;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => {
                in_string = true;
                string_char = ch;
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth = (paren_depth - 1).max(0);
                current.push(ch);
            }
            '-' if chars.peek() == Some(&'-') => {
                current.push(ch);
                while let Some(c) = chars.next() {
                    current.push(c);
                    if c == '\n' { break; }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                current.push(ch);
                current.push(chars.next().unwrap());
                while let Some(c) = chars.next() {
                    current.push(c);
                    if c == '*' && chars.peek() == Some(&'/') {
                        current.push(chars.next().unwrap());
                        break;
                    }
                }
            }
            ';' if paren_depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}


pub fn is_query_stmt(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Query(_)
            | Statement::ShowTables { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowFunctions { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCollation { .. }
            | Statement::ExplainTable { .. }
            | Statement::Explain { .. }
    )
}

pub fn is_query_statement_fallback(sql: &str) -> bool {
    let trimmed = sql.trim().to_uppercase();
    trimmed.starts_with("SELECT")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESC")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("TABLE")
        || trimmed.starts_with("PRAGMA")
}

pub fn classify_stmt(stmt: &Statement) -> StatementType {
    if is_query_stmt(stmt) {
        return StatementType::Query;
    }

    match stmt {
        Statement::Insert(_)
        | Statement::Update { .. }
        | Statement::Delete(_)
        | Statement::Merge { .. } => StatementType::Dml,

        Statement::CreateTable { .. }
        | Statement::CreateView { .. }
        | Statement::CreateIndex(_)
        | Statement::CreateFunction { .. }
        | Statement::CreateProcedure { .. }
        | Statement::CreateTrigger { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateDatabase { .. }
        | Statement::CreateSequence { .. }
        | Statement::AlterTable { .. }
        | Statement::AlterView { .. }
        | Statement::AlterIndex { .. }
        | Statement::Drop { .. }
        | Statement::DropFunction { .. }
        | Statement::DropProcedure { .. }
        | Statement::DropTrigger { .. }
        | Statement::DropSecret { .. }
        | Statement::Truncate { .. }
        | Statement::RenameTable { .. } => StatementType::Ddl,

        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. } => StatementType::Transaction,

        Statement::Use(_)
        | Statement::Set(_) => StatementType::Command,

        _ => StatementType::Exec,
    }
}

pub fn classify_fallback(sql: &str) -> StatementType {
    let trimmed = sql.trim().to_uppercase();

    if is_query_statement_fallback(sql) {
        return StatementType::Query;
    }

    if trimmed.starts_with("INSERT")
        || trimmed.starts_with("UPDATE")
        || trimmed.starts_with("DELETE")
        || trimmed.starts_with("REPLACE")
    {
        return StatementType::Dml;
    }

    if trimmed.starts_with("CREATE")
        || trimmed.starts_with("ALTER")
        || trimmed.starts_with("DROP")
        || trimmed.starts_with("TRUNCATE")
        || trimmed.starts_with("RENAME")
    {
        return StatementType::Ddl;
    }

    if trimmed.starts_with("BEGIN")
        || trimmed.starts_with("COMMIT")
        || trimmed.starts_with("ROLLBACK")
        || trimmed.starts_with("START TRANSACTION")
    {
        return StatementType::Transaction;
    }

    if trimmed.starts_with("USE") || trimmed.starts_with("SET") {
        return StatementType::Command;
    }

    StatementType::Exec
}

pub fn analyze_query_editability(query: &Box<ast::Query>) -> Option<String> {
    let body = &query.body;

    let select = match body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    if select.distinct.is_some() {
        return None;
    }

    let has_group_by = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if has_group_by {
        return None;
    }

    if select.having.is_some() {
        return None;
    }

    for item in &select.projection {
        if has_aggregate_function_in_select_item(item) {
            return None;
        }
    }

    if select.from.len() != 1 {
        return None;
    }

    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return None;
    }

    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => {
            let table_name = name.to_string();
            Some(table_name)
        }
        _ => None,
    }
}

fn has_aggregate_function_in_select_item(item: &ast::SelectItem) -> bool {
    match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            has_aggregate_function(expr)
        }
        _ => false,
    }
}

fn has_aggregate_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MAX" | "MIN" | "GROUP_CONCAT" | "STRING_AGG")
        }
        Expr::BinaryOp { left, right, .. } => {
            has_aggregate_function(left) || has_aggregate_function(right)
        }
        Expr::UnaryOp { expr, .. } => has_aggregate_function(expr),
        Expr::Nested(inner) => has_aggregate_function(inner),
        _ => false,
    }
}

pub fn analyze_select_editability_fallback(sql: &str) -> Option<String> {
    let upper = sql.trim().to_uppercase();

    if !upper.starts_with("SELECT") {
        return None;
    }

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

    let aggregate_functions = [
        "COUNT(", "SUM(", "AVG(", "MAX(", "MIN(",
        "GROUP_CONCAT(", "STRING_AGG(",
    ];

    for func in &aggregate_functions {
        if upper.contains(func) {
            return None;
        }
    }

    if let Some(from_pos) = upper.find(" FROM ") {
        let after_from = &sql[from_pos + 6..].trim();
        let table_name = after_from
            .split_whitespace()
            .next()?
            .trim_end_matches(';')
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();

        if table_name.contains('(') || table_name.contains(',') {
            return None;
        }

        return Some(table_name);
    }

    None
}