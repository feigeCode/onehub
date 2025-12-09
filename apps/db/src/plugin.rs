use std::collections::HashMap;
use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, SqlResult};
use crate::types::*;
use anyhow::{Error, Result};
use async_trait::async_trait;
use one_core::storage::{DatabaseType, DbConnectionConfig, GlobalStorageState};
use one_core::storage::query_repository::QueryRepository;

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
            DatabaseType::MSSQL => "",
            DatabaseType::Oracle => "",
        }
    }

    /// Get database-specific SQL completion information
    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo::default()
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        let quote = self.identifier_quote();
        format!("{}{}{}", quote, identifier, quote)
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError>;

    // === Database/Schema Level Operations ===
    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>>;
    
    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView>;
    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>>;

    // === Table Operations ===
    async fn list_tables(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TableInfo>>;
    
    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView>;
    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView>;
    async fn list_indexes(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<IndexInfo>>;
    
    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView>;
    
    

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

    // === Tree Building ===
    async fn build_database_tree(&self, connection: &dyn DbConnection, node: &DbNode, global_storage_state: &GlobalStorageState) -> Result<Vec<DbNode>> {
        let mut nodes = Vec::new();
        let database = &node.name;
        let id = &node.id;
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("database".to_string(), database.to_string());
        // Tables folder
        let tables = self.list_tables(connection, database).await?;
        let table_count = tables.len();
        let mut table_folder = DbNode::new(
            format!("{}:table_folder", id),
            format!("Tables ({})", table_count),
            DbNodeType::TablesFolder,
            node.connection_id.clone()
        ).with_parent_context(id).with_metadata(metadata.clone());

        if table_count > 0 {
            let children: Vec<DbNode> = tables
                .into_iter()
                .map(|table_info| {
                    let mut metadata: HashMap<String, String> = HashMap::from(metadata.clone());
                    // Add comment as additional metadata if available
                    if let Some(comment) = &table_info.comment {
                        if !comment.is_empty() {
                            metadata.insert("comment".to_string(), comment.clone());
                        }
                    }

                    DbNode::new(
                        format!("{}:table_folder:{}", id, table_info.name),
                        table_info.name.clone(),
                        DbNodeType::Table,
                        node.connection_id.clone()
                    )
                    .with_children_flag(true)
                    .with_parent_context(format!("{}:table_folder", id))
                    .with_metadata(metadata)
                })
                .collect();
            table_folder.children = children;
            table_folder.has_children = true;
            table_folder.children_loaded = true;
        }
        nodes.push(table_folder);

        // Views folder
        let views = self.list_views(connection, database).await?;
        let view_count = views.len();
        if view_count > 0 {
            let mut views_folder = DbNode::new(
                format!("{}:views_folder", id),
                format!("Views ({})", view_count),
                DbNodeType::ViewsFolder,
                node.connection_id.clone()
            ).with_parent_context(id).with_metadata(metadata.clone());

            let children: Vec<DbNode> = views
                .into_iter()
                .map(|view| {
                    let mut metadata: HashMap<String, String> = HashMap::from(metadata.clone());
                    if let Some(comment) = view.comment {
                        metadata.insert("comment".to_string(), comment);
                    }
                    
                    let mut node = DbNode::new(
                        format!("{}:views_folder:{}", id, view.name),
                        view.name.clone(),
                        DbNodeType::View,
                        node.connection_id.clone()
                    ).with_parent_context(format!("{}:views_folder", id));
                    
                    if !metadata.is_empty() {
                        node = node.with_metadata(metadata);
                    }
                    node
                })
                .collect();

            views_folder.children = children;
            views_folder.has_children = true;
            views_folder.children_loaded = true;
            nodes.push(views_folder);
        }

        // Load queries folder
        let queries_folder = self.load_queries(&node, global_storage_state).await?;
        nodes.push(queries_folder);

        Ok(nodes)
    }

    async fn load_queries(&self, node: &DbNode, global_storage_state: &GlobalStorageState) -> std::result::Result<DbNode, Error> {
        let node_id_for_queries = node.id.clone();
        let connection_id_for_queries = node.connection_id.clone();
        let database_name = node.name.clone();  // Database node's name is the database name

        // 获取当前连接的信息
        let conn_repo_arc = global_storage_state.storage.get::<QueryRepository>().await;
        if  let Some(conn_repo) = conn_repo_arc {
            let pool = global_storage_state.storage.get_pool().await;
            return match pool {
                Ok(p) => {
                    let query_repo = (*conn_repo).clone();
                    let queries = query_repo.list_by_connection(&p, &connection_id_for_queries).await.unwrap_or_default();
                    // Create QueriesFolder node
                    let query_count = queries.len();

                    // Add database name to metadata
                    let mut metadata = HashMap::new();
                    metadata.insert("database".to_string(), database_name.clone());

                    let queries_folder_node = DbNode::new(
                        format!("{}:queries_folder", &node_id_for_queries),
                        format!("Queries ({})", query_count),
                        DbNodeType::QueriesFolder,
                        connection_id_for_queries.clone()
                    )
                        .with_parent_context(node_id_for_queries.clone())
                        .with_metadata(metadata.clone());

                    if !queries.is_empty() {
                        // Add NamedQuery children
                        let mut query_nodes = Vec::new();
                        for query in queries {
                            let mut query_metadata:HashMap<String, String> = HashMap::new();
                            metadata.iter().for_each(|(k,v)| {
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
                                connection_id_for_queries.clone()
                            )
                                .with_parent_context(format!("{}:queries_folder", &node_id_for_queries))
                                .with_metadata(query_metadata);
                            
                           

                            query_nodes.push(query_node);
                        }

                        let mut queries_folder_node = queries_folder_node;
                        queries_folder_node.children = query_nodes;
                        queries_folder_node.has_children = true;
                        queries_folder_node.children_loaded = true;
                        Ok(queries_folder_node)
                    } else {
                        // Add empty QueriesFolder node
                        Ok(queries_folder_node)
                    }
                }
                Err(e) => {
                    Err(e)
                }
            }

        }

        // Add database name to metadata
        let mut metadata = HashMap::new();
        metadata.insert("database".to_string(), database_name.clone());

        let queries_folder_node = DbNode::new(
            format!("{}:queries_folder", &node_id_for_queries),
            format!("Queries ({})", 0),
            DbNodeType::QueriesFolder,
            connection_id_for_queries.clone()
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
                        DbNode::new(format!("{}:{}", &node.id, db), db.clone(), DbNodeType::Database, node.id.clone())
                            .with_children_flag(false)
                            .with_parent_context(id)
                    })
                    .collect())
            }
            DbNodeType::Database => {
                self.build_database_tree(connection, node, global_storage_state).await
            }
            DbNodeType::TablesFolder | DbNodeType::ViewsFolder |
            DbNodeType::FunctionsFolder | DbNodeType::ProceduresFolder |
            DbNodeType::TriggersFolder | DbNodeType::SequencesFolder |
            DbNodeType::QueriesFolder => {
                if node.children_loaded {
                    Ok(node.children.clone())
                } else {
                    Ok(Vec::new())
                }
            }
            DbNodeType::Table => {
                let metadata = node.metadata.as_ref().unwrap();
                let db = metadata.get("database").unwrap();
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
                    node.connection_id.clone()
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
                                node.connection_id.clone()
                            )
                            .with_metadata(column_metadata)
                            .with_parent_context(format!("{}:columns_folder", id))
                        })
                        .collect();

                    columns_folder.children = column_nodes;
                    columns_folder.has_children = true;
                    columns_folder.children_loaded = true;
                }
                children.push(columns_folder);

                // Indexes folder
                let indexes = self.list_indexes(connection, db, table).await?;
                let index_count = indexes.len();
                let mut indexes_folder = DbNode::new(
                    format!("{}:indexes_folder", id),
                    format!("Indexes ({})", index_count),
                    DbNodeType::IndexesFolder,
                    node.connection_id.clone()
                ).with_parent_context(id)
                .with_metadata(folder_metadata.clone());

                if index_count > 0 {
                    let index_nodes: Vec<DbNode> = indexes
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
                                node.connection_id.clone()
                            )
                            .with_metadata(metadata)
                            .with_parent_context(format!("{}:indexes_folder", id))
                        })
                        .collect();

                    indexes_folder.children = index_nodes;
                    indexes_folder.has_children = true;
                    indexes_folder.children_loaded = true;
                }
                children.push(indexes_folder);

                Ok(children)
            }
            DbNodeType::ColumnsFolder | DbNodeType::IndexesFolder => {
                if node.children_loaded {
                    Ok(node.children.clone())
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Ok(Vec::new()),
        }
    }

    // === Query Execution ===
    async fn execute_query(
        &self,
        connection: &dyn DbConnection,
        database: &str,
        query: &str,
        params: Option<Vec<SqlValue>>,
    ) -> Result<SqlResult>;

    async fn execute_script(
        &self,
        connection: &dyn DbConnection,
        database: &str,
        script: &str,
        options: ExecOptions,
    ) -> Result<Vec<SqlResult>>;

    async fn switch_db(&self, connection: &dyn DbConnection, database: &str) -> Result<SqlResult>;

    // === Table Data Operations ===
    /// Query table data with pagination, filtering and sorting
    async fn query_table_data(
        &self,
        connection: &dyn DbConnection,
        request: &TableDataRequest,
    ) -> Result<TableDataResponse> {
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
        let total_count = match self.execute_query(connection, &request.database, &count_sql, None).await? {
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
        let data_sql = format!(
            "SELECT * FROM {}{}{}.{}{}{}{}{} LIMIT {} OFFSET {}",
            quote, request.database, quote,
            quote, request.table, quote,
            where_clause,
            order_clause,
            request.page_size,
            offset
        );

        // Execute data query
        let rows = match self.execute_query(connection, &request.database, &data_sql, None).await? {
            SqlResult::Query(result) => result.rows,
            _ => Vec::new(),
        };

        Ok(TableDataResponse {
            columns,
            rows,
            total_count,
            page: request.page,
            page_size: request.page_size,
            primary_key_indices,
            executed_sql: data_sql,
        })
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
    async fn drop_database(&self, connection: &dyn DbConnection, database: &str) -> Result<()> {
        let query = format!("DROP DATABASE IF EXISTS {}", self.quote_identifier(database));
        self.execute_query(connection, "", &query, None).await?;
        Ok(())
    }

    /// Drop table
    async fn drop_table(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<()> {
        let query = format!("DROP TABLE IF EXISTS {}", self.quote_identifier(table));
        self.execute_query(connection, database, &query, None).await?;
        Ok(())
    }

    /// Truncate table
    async fn truncate_table(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<()> {
        let query = format!("TRUNCATE TABLE {}", self.quote_identifier(table));
        self.execute_query(connection, database, &query, None).await?;
        Ok(())
    }

    /// Rename table
    async fn rename_table(&self, connection: &dyn DbConnection, database: &str, old_name: &str, new_name: &str) -> Result<()> {
        let query = match self.name() {
            DatabaseType::MySQL => format!(
                "RENAME TABLE {} TO {}",
                self.quote_identifier(old_name),
                self.quote_identifier(new_name)
            ),
            DatabaseType::PostgreSQL => format!(
                "ALTER TABLE {} RENAME TO {}",
                self.quote_identifier(old_name),
                self.quote_identifier(new_name)
            ),
            DatabaseType::MSSQL | DatabaseType::Oracle => todo!(),
        };
        self.execute_query(connection, database, &query, None).await?;
        Ok(())
    }

    /// Drop view
    async fn drop_view(&self, connection: &dyn DbConnection, database: &str, view: &str) -> Result<()> {
        let query = format!("DROP VIEW IF EXISTS {}", self.quote_identifier(view));
        self.execute_query(connection, database, &query, None).await?;
        Ok(())
    }
}
