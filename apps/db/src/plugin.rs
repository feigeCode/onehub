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
    Dialect, MsSqlDialect,
    OracleDialect, PostgreSqlDialect, SQLiteDialect
};
use sqlparser::parser::Parser;
use std::collections::HashMap;
use tracing::log::error;

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

    /// Quote an identifier (table name, column name, etc.) according to database syntax
    fn quote_identifier(&self, identifier: &str) -> String;

    /// Get database-specific SQL completion information
    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo::default()
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
    fn sql_dialect(&self) -> Box<dyn Dialect>;

    /// Split a SQL script into individual statements using this database's dialect
    fn split_statements(&self, script: &str) -> Vec<String> {
        split_statements_for_database(script, self.name(), self.sql_dialect())
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
    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, schema: Option<&str>, table: &str) -> Result<ObjectView>;
    async fn list_indexes(&self, connection: &dyn DbConnection, database: &str, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>>;

    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, schema: Option<&str>, table: &str) -> Result<ObjectView>;

    /// List foreign keys for a table
    async fn list_foreign_keys(&self, _connection: &dyn DbConnection, _database: &str, _schema: Option<&str>, _table: &str) -> Result<Vec<ForeignKeyDefinition>> {
        Ok(Vec::new())
    }

    /// List triggers for a specific table
    async fn list_table_triggers(&self, _connection: &dyn DbConnection, _database: &str, _schema: Option<&str>, _table: &str) -> Result<Vec<TriggerInfo>> {
        Ok(Vec::new())
    }

    /// List check constraints for a specific table
    async fn list_table_checks(&self, _connection: &dyn DbConnection, _database: &str, _schema: Option<&str>, _table: &str) -> Result<Vec<CheckInfo>> {
        Ok(Vec::new())
    }

    // === View Operations ===
    async fn list_views(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<ViewInfo>>;
    
    async fn list_views_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    // === Function Operations ===

    fn supports_functions(&self) -> bool {
        true
    }

    async fn list_functions(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>>;
    
    async fn list_functions_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;

    fn supports_procedures(&self) -> bool {
        true
    }
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
    fn build_column_definition(&self, column: &ColumnInfo, include_name: bool) -> String;

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
        if self.supports_functions() {
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
        }

        // Procedures folder
        if self.supports_procedures() {
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
        }

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
                let schema = metadata.get("schema").map(|s| s.as_str());
                let table = &node.name;
                let mut folder_metadata = HashMap::new();
                folder_metadata.insert("table".to_string(), table.clone());
                metadata.iter().for_each(|(k, v)| {
                    folder_metadata.insert(k.clone(), v.clone());
                });
                let mut children = Vec::new();

                // Columns folder
                let columns = self.list_columns(connection, db, schema, table).await?;
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
                let indexes = self.list_indexes(connection, db, schema, table).await?;
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
                let foreign_keys = self.list_foreign_keys(connection, db, schema, table).await.unwrap_or_default();
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
                let triggers = self.list_table_triggers(connection, db, schema, table).await.unwrap_or_default();
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

                // Checks folder
                let checks = self.list_table_checks(connection, db, schema, table).await.unwrap_or_default();
                let check_count = checks.len();
                let mut checks_folder = DbNode::new(
                    format!("{}:checks_folder", id),
                    format!("Checks ({})", check_count),
                    DbNodeType::ChecksFolder,
                    node.connection_id.clone(),
                    node.database_type
                ).with_parent_context(id)
                .with_metadata(folder_metadata.clone());

                if check_count > 0 {
                    let check_nodes: Vec<DbNode> = checks
                        .into_iter()
                        .map(|check| {
                            let mut metadata = HashMap::new();
                            folder_metadata.iter().for_each(|(k, v)| {
                                metadata.insert(k.clone(), v.clone());
                            });
                            if let Some(def) = &check.definition {
                                metadata.insert("definition".to_string(), def.clone());
                            }
                            DbNode::new(
                                format!("{}:checks_folder:{}", id, check.name),
                                check.name,
                                DbNodeType::Check,
                                node.connection_id.clone(),
                                node.database_type
                            )
                            .with_metadata(metadata)
                            .with_parent_context(format!("{}:checks_folder", id))
                        })
                        .collect();
                    checks_folder.set_children(check_nodes);
                }
                children.push(checks_folder);

                Ok(children)
            }
            DbNodeType::ColumnsFolder | DbNodeType::IndexesFolder |
            DbNodeType::ForeignKeysFolder | DbNodeType::TriggersFolder |
            DbNodeType::ChecksFolder => {
                if node.children_loaded {
                    Ok(node.children.clone())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Ok(Vec::new()),
        }
    }



    /// Format pagination SQL clause. Override for databases with different syntax.
    fn format_pagination(&self, limit: usize, offset: usize, _order_clause: &str) -> String {
        format!(" LIMIT {} OFFSET {}", limit, offset)
    }

    /// Format table reference for queries. Override for databases with different syntax.
    /// - MySQL: `database`.`table`
    /// - PostgreSQL: "schema"."table" (uses schema, ignores database since connection is db-specific)
    /// - MSSQL: [database]..[table] or [database].[schema].[table]
    fn format_table_reference(&self, database: &str, _schema: Option<&str>, table: &str) -> String {
        format!(
            "{}.{}",
            self.quote_identifier(database),
            self.quote_identifier(table)
        )
    }

    // === Table Data Operations ===
    /// Query table data with pagination, filtering and sorting
    async fn query_table_data(
        &self,
        connection: &dyn DbConnection,
        request: &TableDataRequest,
    ) -> Result<TableDataResponse> {
        let start_time = std::time::Instant::now();

        // Get column metadata
        let columns_info = self.list_columns(connection, &request.database, request.schema.as_deref(), &request.table).await?;
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
            let indexes = self.list_indexes(connection, &request.database, request.schema.as_deref(), &request.table).await.unwrap_or_default();
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
                    let col = self.quote_identifier(&f.column);
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
                    format!("{} {}", self.quote_identifier(&s.column), dir)
                })
                .collect();
            format!(" ORDER BY {}", sorts.join(", "))
        };

        // Calculate offset
        let offset = (request.page.saturating_sub(1)) * request.page_size;

        // Build table reference
        let table_ref = self.format_table_reference(
            &request.database,
            request.schema.as_deref(),
            &request.table
        );

        // Build count query
        let count_sql = format!(
            "SELECT COUNT(*) FROM {}{}",
            table_ref,
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
                "SELECT * FROM {}{}{}",
                table_ref,
                where_clause,
                order_clause
            )
        } else {
            // Query with pagination
            let pagination = self.format_pagination(request.page_size, offset, &order_clause);
            format!(
                "SELECT * FROM {}{}{}{}",
                table_ref,
                where_clause,
                order_clause,
                pagination
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
        let table_ident = self.format_table_reference(
            &request.database,
            request.schema.as_deref(),
            &request.table
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
                    let simple_table = self.quote_identifier(&request.table);
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
                    let simple_table = self.quote_identifier(&request.table);
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

    fn build_limit_clause(&self) -> String;

    fn build_where_and_limit_clause(
        &self,
        request: &TableSaveRequest,
        original_data: &[String],
    ) -> (String, String);

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

    // === Export Operations ===
    /// Export table CREATE statement
    async fn export_table_create_sql(
        &self,
        connection: &dyn DbConnection,
        database: &str,
        table: &str,
    ) -> Result<String> {
        let columns = self.list_columns(connection, database, None, table).await?;
        if columns.is_empty() {
            return Ok(String::new());
        }

        let mut sql = format!("CREATE TABLE {} (\n", self.quote_identifier(table));
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str("    ");
            sql.push_str(&self.build_column_definition(col, true));
        }
        sql.push_str("\n)");
        Ok(sql)
    }

    /// Export table data as INSERT statements
    async fn export_table_data_sql(
        &self,
        connection: &dyn DbConnection,
        database: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<usize>,
    ) -> Result<String> {
        let table_ref = self.format_table_reference(database, None, table);
        let mut select_sql = format!("SELECT * FROM {}", table_ref);
        if let Some(where_c) = where_clause {
            select_sql.push_str(" WHERE ");
            select_sql.push_str(where_c);
        }
        if let Some(lim) = limit {
            let pagination = self.format_pagination(lim, 0, "");
            select_sql.push_str(&pagination);
        }

        let result = connection.query(&select_sql, None, ExecOptions::default()).await
            .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?;

        let mut output = String::new();
        if let SqlResult::Query(query_result) = result {
            if !query_result.rows.is_empty() {
                let table_ident = self.quote_identifier(table);
                for row in &query_result.rows {
                    output.push_str("INSERT INTO ");
                    output.push_str(&table_ident);
                    output.push_str(" VALUES (");

                    for (i, value) in row.iter().enumerate() {
                        if i > 0 {
                            output.push_str(", ");
                        }
                        match value {
                            Some(v) => {
                                output.push('\'');
                                output.push_str(&v.replace('\'', "''"));
                                output.push('\'');
                            }
                            None => output.push_str("NULL"),
                        }
                    }

                    output.push_str(");\n");
                }
            }
        }

        Ok(output)
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
    fn rename_table(&self, database: &str, old_name: &str, new_name: &str) -> String;

    /// Drop view
    fn drop_view(&self, _database: &str, view: &str) -> String {
        format!("DROP VIEW IF EXISTS {}", self.quote_identifier(view))
    }

    /// Build column definition from ColumnDefinition (for table designer)
    fn build_column_def(&self, col: &ColumnDefinition) -> String;

    /// Build CREATE TABLE SQL from TableDesign
    fn build_create_table_sql(&self, design: &TableDesign) -> String;

    /// Build ALTER TABLE SQL from original and new TableDesign
    /// Returns a series of ALTER TABLE statements for the differences
    fn build_alter_table_sql(&self, original: &TableDesign, new: &TableDesign) -> String;

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
        Err(e) => {
            error!("Error parsing SQL: {}", e);
            fallback_split(script)
        },
    }
}

/// Check if the script can be parsed by sqlparser
/// Returns false for scripts containing syntax that sqlparser doesn't support well
pub fn can_use_sqlparser(script: &str) -> bool {
    can_use_sqlparser_with_db_type(script, DatabaseType::MySQL)
}

pub fn can_use_sqlparser_with_db_type(script: &str, db_type: DatabaseType) -> bool {
    let upper = script.to_uppercase();

    let common_problematic = [
        "CREATE OR REPLACE FUNCTION",
        "CREATE OR REPLACE PROCEDURE",
        "CREATE FUNCTION",
        "CREATE PROCEDURE",
        "CREATE TRIGGER",
        "CREATE DEFINER",
    ];

    for keyword in &common_problematic {
        if upper.contains(keyword) {
            return false;
        }
    }

    if upper.contains("BEGIN") && (upper.contains("END;") || upper.contains("END ;")) {
        return false;
    }

    match db_type {
        DatabaseType::MySQL => {
            if upper.contains("DELIMITER") {
                return false;
            }
        }
        DatabaseType::PostgreSQL => {
            if script.contains('$') && script.matches('$').count() >= 2 {
                return false;
            }
        }
        DatabaseType::MSSQL => {
            let lines: Vec<&str> = script.lines().collect();
            for line in lines {
                if line.trim().to_uppercase() == "GO" {
                    return false;
                }
            }
        }
        _ => {}
    }

    true
}

pub fn split_statements_for_database(script: &str, db_type: DatabaseType, dialect: Box<dyn Dialect>) -> Vec<String> {
    if !can_use_sqlparser_with_db_type(script, db_type) {
        return fallback_split_with_db_type(script, db_type);
    }

    match Parser::parse_sql(dialect.as_ref(), script) {
        Ok(statements) => statements.iter().map(|stmt| stmt.to_string()).filter(|stmt| !stmt.trim().is_empty()).collect(),
        Err(_) => fallback_split_with_db_type(script, db_type),
    }
}

pub fn fallback_split(script: &str) -> Vec<String> {
    fallback_split_with_db_type(script, DatabaseType::MySQL)
}

pub fn fallback_split_with_db_type(script: &str, db_type: DatabaseType) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = script.chars().peekable();

    let mut in_string = false;
    let mut string_char = '\0';
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut dollar_quote: Option<String> = None;

    let mut paren_depth = 0i32;
    let mut begin_depth = 0i32;
    let mut last_word_checked = String::new();
    let mut delimiter = ";".to_string();

    while let Some(ch) = chars.next() {
        // ---------- 行注释 ----------
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        // ---------- 块注释 ----------
        if in_block_comment {
            current.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                if let Some(next_ch) = chars.next() {
                    current.push(next_ch);
                }
                in_block_comment = false;
            }
            continue;
        }

        // ---------- Dollar Quote (PostgreSQL) ----------
        if let Some(ref tag) = dollar_quote {
            current.push(ch);
            if ch == '$' {
                let end_pos = current.len();
                let start_pos = end_pos.saturating_sub(tag.len());
                if current[start_pos..].ends_with(tag) {
                    dollar_quote = None;
                }
            }
            continue;
        }

        // ---------- 字符串 ----------
        if in_string {
            current.push(ch);
            if ch == string_char {
                if chars.peek() == Some(&string_char) {
                    if let Some(next_ch) = chars.next() {
                        current.push(next_ch);
                    }
                } else {
                    in_string = false;
                }
            } else if ch == '\\' && db_type == DatabaseType::MySQL {
                if let Some(_) = chars.peek() {
                    if let Some(next_ch) = chars.next() {
                        current.push(next_ch);
                    }
                }
            }
            continue;
        }

        // ---------- 注释起始 ----------
        if ch == '-' && chars.peek() == Some(&'-') {
            current.push(ch);
            if let Some(next_ch) = chars.next() {
                current.push(next_ch);
            }
            in_line_comment = true;
            continue;
        }

        if ch == '#' && db_type == DatabaseType::MySQL {
            current.push(ch);
            in_line_comment = true;
            continue;
        }

        if ch == '/' && chars.peek() == Some(&'*') {
            current.push(ch);
            if let Some(next_ch) = chars.next() {
                current.push(next_ch);
            }
            in_block_comment = true;
            continue;
        }

        // ---------- Dollar Quote 起始 (PostgreSQL) ----------
        if ch == '$' && db_type == DatabaseType::PostgreSQL {
            if let Some(tag) = try_parse_dollar_quote(&mut chars) {
                dollar_quote = Some(tag.clone());
                current.push_str(&tag);
                continue;
            }
        }

        // ---------- 字符串起始 ----------
        if ch == '\'' || ch == '"' {
            in_string = true;
            string_char = ch;
            current.push(ch);
            continue;
        }

        if ch == '`' && db_type == DatabaseType::MySQL {
            in_string = true;
            string_char = ch;
            current.push(ch);
            continue;
        }

        // ---------- 括号深度 ----------
        if ch == '(' {
            paren_depth += 1;
            current.push(ch);
            continue;
        }

        if ch == ')' {
            paren_depth = (paren_depth - 1).max(0);
            current.push(ch);
            continue;
        }

        current.push(ch);

        // ---------- BEGIN / END 深度 (只在空白字符后检测) ----------
        if ch.is_whitespace() || ch == ';' || ch == '$' {
            update_begin_depth(&current, &mut begin_depth, &mut last_word_checked);
        }

        // ---------- DELIMITER 命令 (MySQL) ----------
        if db_type == DatabaseType::MySQL && ch == '\n' {
            if let Some(new_delim) = try_parse_delimiter(&current) {
                delimiter = new_delim;
                let lines: Vec<&str> = current.lines().collect();
                if lines.len() > 1 {
                    current = lines[..lines.len() - 1].join("\n");
                } else {
                    current.clear();
                }
                continue;
            }
        }

        // ---------- GO 命令 (SQL Server) ----------
        if db_type == DatabaseType::MSSQL && ch == '\n' {
            let lines: Vec<&str> = current.lines().collect();
            if let Some(last_line) = lines.last() {
                if last_line.trim().to_uppercase() == "GO" {
                    let stmt_lines: Vec<&str> = lines[..lines.len() - 1].to_vec();
                    let stmt = stmt_lines.join("\n").trim().to_string();
                    if !stmt.is_empty() {
                        statements.push(stmt);
                    }
                    current.clear();
                    continue;
                }
            }
        }

        // ---------- 语句分割 ----------
        if paren_depth == 0 && begin_depth == 0 {
            let trimmed_current = current.trim_end();
            if trimmed_current.ends_with(&delimiter) {
                let stmt = trimmed_current
                    .strip_suffix(&delimiter)
                    .unwrap_or(trimmed_current)
                    .trim();

                if !stmt.is_empty() && !stmt.to_uppercase().starts_with("DELIMITER") {
                    statements.push(stmt.to_string());
                }
                current.clear();
            } else if db_type == DatabaseType::Oracle
                && current.trim().ends_with('\n')
                && current.trim_end().ends_with('/')
            {
                let stmt = current.trim().strip_suffix('/').unwrap_or(&current).trim();
                if !stmt.is_empty() {
                    statements.push(stmt.to_string());
                }
                current.clear();
            }
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() && !trimmed.to_uppercase().starts_with("DELIMITER") {
        statements.push(trimmed.to_string());
    }

    statements
}

fn try_parse_dollar_quote(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<String> {
    let mut tag = String::from("$");
    let mut lookahead = chars.clone();

    while let Some(c) = lookahead.next() {
        if c.is_alphanumeric() || c == '_' {
            tag.push(c);
        } else if c == '$' {
            tag.push(c);
            break;
        } else {
            return None;
        }
    }

    if tag.len() >= 2 && tag.ends_with('$') {
        for _ in 1..tag.len() {
            chars.next();
        }
        Some(tag)
    } else {
        None
    }
}

fn try_parse_delimiter(current: &str) -> Option<String> {
    let lines: Vec<&str> = current.lines().collect();
    if let Some(last_line) = lines.last() {
        let trimmed = last_line.trim();
        if trimmed.to_uppercase().starts_with("DELIMITER") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 2 {
                return Some(parts[1].to_string());
            }
        }
    }
    None
}

fn update_begin_depth(current: &str, begin_depth: &mut i32, last_word_checked: &mut String) {
    let upper = current.to_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();

    if let Some(last_word) = words.last() {
        let last_word_str = last_word.to_string();
        if last_word_str != *last_word_checked {
            *last_word_checked = last_word_str.clone();
            if last_word_str == "BEGIN" {
                *begin_depth += 1;
            } else if last_word_str.starts_with("END") {
                *begin_depth = (*begin_depth - 1).max(0);
            }
        }
    }
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
            | Statement::Pragma { .. }
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::{ClickHouseDialect, MySqlDialect};
    use sqlparser::parser::Parser;

    // ==================== split_statements_with_dialect tests ====================

    #[test]
    fn test_split_statements_with_dialect_mysql() {
        let sql = "SELECT * FROM users; INSERT INTO logs VALUES (1);";
        let stmts = split_statements_with_dialect(sql, &MySqlDialect {});
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_statements_with_dialect_postgresql() {
        use sqlparser::dialect::PostgreSqlDialect;
        let sql = "SELECT * FROM users; UPDATE users SET name = 'test';";
        let stmts = split_statements_with_dialect(sql, &PostgreSqlDialect {});
        assert_eq!(stmts.len(), 2);
    }

    // ==================== split_statements_for_database tests ====================

    #[test]
    fn test_simple_split() {
        let sql = "SELECT * FROM users; INSERT INTO logs VALUES (1);";
        let stmts = split_statements_for_database(sql, DatabaseType::MySQL, Box::new(MySqlDialect {}));
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_all_database_types() {
        let sql = "SELECT 1; SELECT 2;";
        for db_type in [
            DatabaseType::MySQL,
            DatabaseType::PostgreSQL,
            DatabaseType::SQLite,
            DatabaseType::MSSQL,
            DatabaseType::Oracle,
            DatabaseType::ClickHouse,
        ] {
           let dialect: Box<dyn Dialect> = match db_type {
                DatabaseType::MySQL => Box::new(MySqlDialect {}),
                DatabaseType::PostgreSQL => Box::new(PostgreSqlDialect {}),
                DatabaseType::MSSQL => Box::new(MsSqlDialect {}),
                DatabaseType::SQLite => Box::new(SQLiteDialect {}),
                DatabaseType::ClickHouse => Box::new(ClickHouseDialect {}),
                DatabaseType::Oracle => Box::new(OracleDialect {})
            };
            let stmts = split_statements_for_database(sql, db_type, dialect);
            assert_eq!(stmts.len(), 2, "Failed for {:?}", db_type);
        }
    }

    // ==================== fallback_split tests ====================

    #[test]
    fn test_fallback_split_default() {
        let sql = "SELECT * FROM users; INSERT INTO t VALUES (1);";
        let stmts = fallback_split(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_mysql_delimiter() {
        let sql = r#"
DELIMITER $$
CREATE PROCEDURE test()
BEGIN
    SELECT * FROM users;
    INSERT INTO logs VALUES (1);
END$$
DELIMITER ;
SELECT * FROM users;
        "#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE PROCEDURE"));
        assert!(stmts[1].contains("SELECT * FROM users"));
    }

    #[test]
    fn test_mysql_hash_comment() {
        let sql = "# This is a comment\nSELECT * FROM users; INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_mysql_backtick() {
        let sql = "SELECT * FROM `table;name`; INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_mysql_backslash_escape() {
        let sql = r#"SELECT * FROM users WHERE name = 'it\'s'; INSERT INTO t VALUES (1);"#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_postgresql_dollar_quote() {
        let sql = r#"
CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'This ; is not a delimiter';
END;
$$ LANGUAGE plpgsql;
SELECT * FROM users;
        "#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::PostgreSQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_postgresql_named_dollar_quote() {
        let sql = r#"
CREATE FUNCTION test() RETURNS void AS $func$
BEGIN
    SELECT 'nested $$ quote';
END;
$func$ LANGUAGE plpgsql;
SELECT 1;
        "#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::PostgreSQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_mssql_go_command() {
        let sql = r#"
SELECT * FROM users
GO
INSERT INTO logs VALUES (1)
GO
        "#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MSSQL);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("SELECT"));
        assert!(stmts[1].contains("INSERT"));
    }

    #[test]
    fn test_oracle_slash_delimiter() {
        let sql = "BEGIN\n    NULL;\nEND;\n/\nSELECT * FROM dual;";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::Oracle);
        assert!(stmts.len() >= 1);
    }

    #[test]
    fn test_string_with_semicolon() {
        let sql = r#"SELECT * FROM users WHERE note = 'a;b;c'; INSERT INTO t VALUES (1);"#;
        let stmts = split_statements_for_database(sql, DatabaseType::PostgreSQL, Box::new(PostgreSqlDialect {}));
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_double_quoted_string() {
        let sql = r#"SELECT * FROM users WHERE note = "a;b;c"; INSERT INTO t VALUES (1);"#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_escaped_quote() {
        let sql = "SELECT * FROM users WHERE note = 'it''s ok'; INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_line_comment() {
        let sql = "-- Comment with ;\nSELECT * FROM users; INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_block_comment() {
        let sql = "/* Block ; comment */ SELECT * FROM users; INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_nested_parentheses() {
        let sql = "SELECT * FROM (SELECT id FROM users WHERE id IN (1, 2, 3)); INSERT INTO t VALUES (1);";
        let stmts = fallback_split_with_db_type(sql, DatabaseType::MySQL);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_nested_begin_end() {
        let sql = r#"
BEGIN
    BEGIN
        SELECT 1;
    END;
    SELECT 2;
END;
SELECT 3;
        "#;
        let stmts = fallback_split_with_db_type(sql, DatabaseType::Oracle);
        assert!(stmts.len() >= 1);
    }

    #[test]
    fn test_empty_input() {
        let stmts = split_statements_for_database("", DatabaseType::MySQL, Box::new(MySqlDialect {}));
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_whitespace_only() {
        let stmts = split_statements_for_database("   \n\t  ", DatabaseType::MySQL, Box::new(MySqlDialect {}));
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_single_statement_no_semicolon() {
        let stmts = split_statements_for_database("SELECT * FROM users", DatabaseType::MySQL, Box::new(MySqlDialect {}));
        assert_eq!(stmts.len(), 1);
    }

    // ==================== can_use_sqlparser tests ====================

    #[test]
    fn test_can_use_sqlparser_default() {
        assert!(can_use_sqlparser("SELECT * FROM users"));
        assert!(!can_use_sqlparser("DELIMITER $$"));
        assert!(!can_use_sqlparser("CREATE FUNCTION test()"));
    }

    #[test]
    fn test_can_use_sqlparser_mysql() {
        assert!(can_use_sqlparser_with_db_type("SELECT * FROM users", DatabaseType::MySQL));
        assert!(!can_use_sqlparser_with_db_type("DELIMITER $$", DatabaseType::MySQL));
        assert!(!can_use_sqlparser_with_db_type("CREATE FUNCTION test()", DatabaseType::MySQL));
        assert!(!can_use_sqlparser_with_db_type("CREATE PROCEDURE test()", DatabaseType::MySQL));
    }

    #[test]
    fn test_can_use_sqlparser_postgresql() {
        assert!(can_use_sqlparser_with_db_type("SELECT * FROM users", DatabaseType::PostgreSQL));
        assert!(!can_use_sqlparser_with_db_type("CREATE FUNCTION test() AS $$ BEGIN END; $$", DatabaseType::PostgreSQL));
    }

    #[test]
    fn test_can_use_sqlparser_mssql() {
        assert!(can_use_sqlparser_with_db_type("SELECT * FROM users", DatabaseType::MSSQL));
        assert!(!can_use_sqlparser_with_db_type("SELECT * FROM users\nGO\nSELECT 1", DatabaseType::MSSQL));
    }

    #[test]
    fn test_can_use_sqlparser_begin_end() {
        assert!(!can_use_sqlparser_with_db_type("BEGIN SELECT 1; END;", DatabaseType::MySQL));
        assert!(!can_use_sqlparser_with_db_type("BEGIN\nSELECT 1;\nEND ;", DatabaseType::Oracle));
    }

    #[test]
    fn test_can_use_sqlparser_create_trigger() {
        assert!(!can_use_sqlparser_with_db_type("CREATE TRIGGER test AFTER INSERT", DatabaseType::MySQL));
    }

    // ==================== is_query_stmt tests (AST-based) ====================

    #[test]
    fn test_is_query_stmt_select() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT * FROM users").unwrap();
        assert!(is_query_stmt(&stmts[0]));
    }

    #[test]
    fn test_is_query_stmt_show() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SHOW TABLES").unwrap();
        assert!(is_query_stmt(&stmts[0]));
    }

    #[test]
    fn test_is_query_stmt_explain() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "EXPLAIN SELECT * FROM users").unwrap();
        assert!(is_query_stmt(&stmts[0]));
    }

    #[test]
    fn test_is_query_stmt_insert() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "INSERT INTO users VALUES (1)").unwrap();
        assert!(!is_query_stmt(&stmts[0]));
    }

    #[test]
    fn test_is_query_stmt_update() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "UPDATE users SET name = 'test'").unwrap();
        assert!(!is_query_stmt(&stmts[0]));
    }

    #[test]
    fn test_is_query_stmt_delete() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "DELETE FROM users").unwrap();
        assert!(!is_query_stmt(&stmts[0]));
    }

    // ==================== is_query_statement_fallback tests ====================

    #[test]
    fn test_is_query_statement_fallback_select() {
        assert!(is_query_statement_fallback("SELECT * FROM users"));
        assert!(is_query_statement_fallback("  select id from t  "));
    }

    #[test]
    fn test_is_query_statement_fallback_show() {
        assert!(is_query_statement_fallback("SHOW TABLES"));
        assert!(is_query_statement_fallback("SHOW DATABASES"));
    }

    #[test]
    fn test_is_query_statement_fallback_describe() {
        assert!(is_query_statement_fallback("DESCRIBE users"));
        assert!(is_query_statement_fallback("DESC users"));
    }

    #[test]
    fn test_is_query_statement_fallback_explain() {
        assert!(is_query_statement_fallback("EXPLAIN SELECT * FROM users"));
    }

    #[test]
    fn test_is_query_statement_fallback_with() {
        assert!(is_query_statement_fallback("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    }

    #[test]
    fn test_is_query_statement_fallback_pragma() {
        assert!(is_query_statement_fallback("PRAGMA table_info(users)"));
    }

    #[test]
    fn test_is_query_statement_fallback_non_query() {
        assert!(!is_query_statement_fallback("INSERT INTO users VALUES (1)"));
        assert!(!is_query_statement_fallback("UPDATE users SET name = 'test'"));
        assert!(!is_query_statement_fallback("DELETE FROM users"));
        assert!(!is_query_statement_fallback("CREATE TABLE t (id INT)"));
    }

    // ==================== classify_stmt tests (AST-based) ====================

    #[test]
    fn test_classify_stmt_query() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT * FROM users").unwrap();
        assert_eq!(classify_stmt(&stmts[0]), StatementType::Query);
    }

    #[test]
    fn test_classify_stmt_dml() {
        let insert = Parser::parse_sql(&MySqlDialect {}, "INSERT INTO users VALUES (1)").unwrap();
        assert_eq!(classify_stmt(&insert[0]), StatementType::Dml);

        let update = Parser::parse_sql(&MySqlDialect {}, "UPDATE users SET name = 'test'").unwrap();
        assert_eq!(classify_stmt(&update[0]), StatementType::Dml);

        let delete = Parser::parse_sql(&MySqlDialect {}, "DELETE FROM users").unwrap();
        assert_eq!(classify_stmt(&delete[0]), StatementType::Dml);
    }

    #[test]
    fn test_classify_stmt_ddl() {
        let create = Parser::parse_sql(&MySqlDialect {}, "CREATE TABLE t (id INT)").unwrap();
        assert_eq!(classify_stmt(&create[0]), StatementType::Ddl);

        let alter = Parser::parse_sql(&MySqlDialect {}, "ALTER TABLE t ADD COLUMN name VARCHAR(100)").unwrap();
        assert_eq!(classify_stmt(&alter[0]), StatementType::Ddl);

        let drop = Parser::parse_sql(&MySqlDialect {}, "DROP TABLE t").unwrap();
        assert_eq!(classify_stmt(&drop[0]), StatementType::Ddl);
    }

    #[test]
    fn test_classify_stmt_transaction() {
        let commit = Parser::parse_sql(&MySqlDialect {}, "COMMIT").unwrap();
        assert_eq!(classify_stmt(&commit[0]), StatementType::Transaction);

        let rollback = Parser::parse_sql(&MySqlDialect {}, "ROLLBACK").unwrap();
        assert_eq!(classify_stmt(&rollback[0]), StatementType::Transaction);
    }

    #[test]
    fn test_classify_stmt_command() {
        let use_stmt = Parser::parse_sql(&MySqlDialect {}, "USE mydb").unwrap();
        assert_eq!(classify_stmt(&use_stmt[0]), StatementType::Command);

        let set = Parser::parse_sql(&MySqlDialect {}, "SET autocommit = 1").unwrap();
        assert_eq!(classify_stmt(&set[0]), StatementType::Command);
    }

    // ==================== classify_fallback tests ====================

    #[test]
    fn test_classify_fallback_query() {
        assert_eq!(classify_fallback("SELECT * FROM users"), StatementType::Query);
        assert_eq!(classify_fallback("SHOW TABLES"), StatementType::Query);
        assert_eq!(classify_fallback("DESCRIBE users"), StatementType::Query);
    }

    #[test]
    fn test_classify_fallback_dml() {
        assert_eq!(classify_fallback("INSERT INTO users VALUES (1)"), StatementType::Dml);
        assert_eq!(classify_fallback("UPDATE users SET name = 'test'"), StatementType::Dml);
        assert_eq!(classify_fallback("DELETE FROM users"), StatementType::Dml);
        assert_eq!(classify_fallback("REPLACE INTO users VALUES (1)"), StatementType::Dml);
    }

    #[test]
    fn test_classify_fallback_ddl() {
        assert_eq!(classify_fallback("CREATE TABLE users (id INT)"), StatementType::Ddl);
        assert_eq!(classify_fallback("ALTER TABLE users ADD COLUMN name VARCHAR(100)"), StatementType::Ddl);
        assert_eq!(classify_fallback("DROP TABLE users"), StatementType::Ddl);
        assert_eq!(classify_fallback("TRUNCATE TABLE users"), StatementType::Ddl);
        assert_eq!(classify_fallback("RENAME TABLE old TO new"), StatementType::Ddl);
    }

    #[test]
    fn test_classify_fallback_transaction() {
        assert_eq!(classify_fallback("BEGIN"), StatementType::Transaction);
        assert_eq!(classify_fallback("COMMIT"), StatementType::Transaction);
        assert_eq!(classify_fallback("ROLLBACK"), StatementType::Transaction);
        assert_eq!(classify_fallback("START TRANSACTION"), StatementType::Transaction);
    }

    #[test]
    fn test_classify_fallback_command() {
        assert_eq!(classify_fallback("USE mydb"), StatementType::Command);
        assert_eq!(classify_fallback("SET autocommit = 1"), StatementType::Command);
    }

    #[test]
    fn test_classify_fallback_exec() {
        assert_eq!(classify_fallback("CALL my_procedure()"), StatementType::Exec);
        assert_eq!(classify_fallback("EXECUTE my_statement"), StatementType::Exec);
    }

    // ==================== analyze_query_editability tests (AST-based) ====================

    #[test]
    fn test_analyze_query_editability_simple() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT * FROM users").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_some());
            assert!(result.unwrap().contains("users"));
        }
    }

    #[test]
    fn test_analyze_query_editability_with_where() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT * FROM users WHERE id = 1").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_analyze_query_editability_with_join() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_analyze_query_editability_with_group_by() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT name, COUNT(*) FROM users GROUP BY name").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_analyze_query_editability_with_distinct() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT DISTINCT name FROM users").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_analyze_query_editability_with_aggregate() {
        let stmts = Parser::parse_sql(&MySqlDialect {}, "SELECT COUNT(*) FROM users").unwrap();
        if let Statement::Query(query) = &stmts[0] {
            let result = analyze_query_editability(query);
            assert!(result.is_none());
        }
    }

    // ==================== analyze_select_editability_fallback tests ====================

    #[test]
    fn test_analyze_select_editability_fallback_simple() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users"), Some("users".to_string()));
    }

    #[test]
    fn test_analyze_select_editability_fallback_quoted() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM `users`"), Some("users".to_string()));
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM \"users\""), Some("users".to_string()));
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_where() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users WHERE id = 1"), Some("users".to_string()));
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_join() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users JOIN orders ON users.id = orders.user_id"), None);
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users INNER JOIN orders"), None);
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users LEFT JOIN orders"), None);
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_group_by() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users GROUP BY name"), None);
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_aggregate() {
        assert_eq!(analyze_select_editability_fallback("SELECT COUNT(*) FROM users"), None);
        assert_eq!(analyze_select_editability_fallback("SELECT SUM(amount) FROM orders"), None);
        assert_eq!(analyze_select_editability_fallback("SELECT AVG(price) FROM products"), None);
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_distinct() {
        assert_eq!(analyze_select_editability_fallback("SELECT DISTINCT * FROM users"), None);
        assert_eq!(analyze_select_editability_fallback("SELECT DISTINCT name FROM users"), None);
    }

    #[test]
    fn test_analyze_select_editability_fallback_with_union() {
        assert_eq!(analyze_select_editability_fallback("SELECT * FROM users UNION SELECT * FROM admins"), None);
    }

    #[test]
    fn test_analyze_select_editability_fallback_non_select() {
        assert_eq!(analyze_select_editability_fallback("INSERT INTO users VALUES (1)"), None);
        assert_eq!(analyze_select_editability_fallback("UPDATE users SET name = 'test'"), None);
    }
}