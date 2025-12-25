use anyhow::Result;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, SqlResult};
use crate::clickhouse::connection::ClickHouseDbConnection;
use crate::plugin::{DatabaseOperationRequest, DatabasePlugin, SqlCompletionInfo};
use crate::types::*;

/// ClickHouse database plugin implementation (stateless)
pub struct ClickHousePlugin;

impl ClickHousePlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DatabasePlugin for ClickHousePlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::ClickHouse
    }

    fn identifier_quote(&self) -> &str {
        "`"
    }

    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
        Box::new(sqlparser::dialect::ClickHouseDialect {})
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                ("FINAL", "Force merge for ReplacingMergeTree"),
                ("SAMPLE", "Sample data clause"),
                ("PREWHERE", "Pre-filter clause (optimized WHERE)"),
                ("ARRAY JOIN", "Array join operation"),
                ("GLOBAL", "Global join modifier"),
                ("LOCAL", "Local join modifier"),
                ("ASOF", "ASOF join"),
                ("ANTI", "ANTI join"),
                ("SEMI", "SEMI join"),
                ("MATERIALIZED", "Materialized column/view"),
                ("ALIAS", "Alias column"),
                ("CODEC", "Column compression codec"),
                ("TTL", "Time to live expression"),
                ("SETTINGS", "Query/table settings"),
            ],
            functions: vec![
                // ClickHouse-specific functions
                ("now()", "Current timestamp"),
                ("today()", "Current date"),
                ("yesterday()", "Yesterday's date"),
                ("toDate(expr)", "Convert to Date"),
                ("toDateTime(expr)", "Convert to DateTime"),
                ("toString(expr)", "Convert to String"),
                ("toInt32(expr)", "Convert to Int32"),
                ("toUInt32(expr)", "Convert to UInt32"),
                ("toFloat64(expr)", "Convert to Float64"),
                ("arrayJoin(arr)", "Unfold array to rows"),
                ("arrayElement(arr, n)", "Get array element"),
                ("arraySlice(arr, offset, length)", "Array slice"),
                ("arrayMap(func, arr)", "Map function over array"),
                ("arrayFilter(func, arr)", "Filter array"),
                ("arrayReduce(func, arr)", "Reduce array"),
                ("groupArray(expr)", "Collect to array (aggregate)"),
                ("groupUniqArray(expr)", "Collect unique to array"),
                ("uniq(expr)", "Count unique values"),
                ("uniqExact(expr)", "Count unique values (exact)"),
                ("topK(n)(expr)", "Top K most frequent values"),
                ("quantile(level)(expr)", "Quantile aggregate"),
                ("median(expr)", "Median value"),
                ("stddevPop(expr)", "Population standard deviation"),
                ("varPop(expr)", "Population variance"),
                ("corr(x, y)", "Correlation"),
                ("covarPop(x, y)", "Population covariance"),
            ],
            operators: vec![
                ("GLOBAL IN", "Global IN operator"),
                ("NOT GLOBAL IN", "Negated global IN"),
                ("IN", "Set membership"),
                ("NOT IN", "Not in set"),
                ("LIKE", "Pattern match"),
                ("ILIKE", "Case-insensitive LIKE"),
                ("NOT LIKE", "Negated LIKE"),
            ],
            data_types: vec![
                ("Int8", "8-bit signed integer"),
                ("Int16", "16-bit signed integer"),
                ("Int32", "32-bit signed integer"),
                ("Int64", "64-bit signed integer"),
                ("UInt8", "8-bit unsigned integer"),
                ("UInt16", "16-bit unsigned integer"),
                ("UInt32", "32-bit unsigned integer"),
                ("UInt64", "64-bit unsigned integer"),
                ("Float32", "32-bit float"),
                ("Float64", "64-bit float"),
                ("Decimal(P, S)", "Decimal number"),
                ("String", "Variable-length string"),
                ("FixedString(N)", "Fixed-length string"),
                ("Date", "Date (days since 1970-01-01)"),
                ("DateTime", "Unix timestamp"),
                ("DateTime64(precision)", "High-precision timestamp"),
                ("UUID", "UUID type"),
                ("IPv4", "IPv4 address"),
                ("IPv6", "IPv6 address"),
                ("Enum8('a'=1,'b'=2)", "8-bit enum"),
                ("Enum16('a'=1,'b'=2)", "16-bit enum"),
                ("Array(T)", "Array of type T"),
                ("Tuple(T1, T2, ...)", "Tuple type"),
                ("Nullable(T)", "Nullable type"),
                ("LowCardinality(T)", "Low cardinality optimization"),
                ("JSON", "JSON data type"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id UInt64,\n  $2\n) ENGINE = MergeTree()\nORDER BY id", "Create table"),
                ("idx", "CREATE INDEX $1 ON $2 $3 TYPE $4", "Create index"),
                ("mat", "CREATE MATERIALIZED VIEW $1 AS\nSELECT $2\nFROM $3", "Create materialized view"),
            ],
        }
        .with_standard_sql()
    }

    async fn create_connection(
        &self,
        config: DbConnectionConfig,
    ) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        let mut conn = ClickHouseDbConnection::new(config);
        conn.connect().await?;
        Ok(Box::new(conn))
    }

    // === Database/Schema Level Operations ===

    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>> {
        let result = connection
            .query(
                "SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name",
                None,
                ExecOptions::default(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result
                .rows
                .iter()
                .filter_map(|row| row.first().and_then(|v| v.clone()))
                .collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView> {
        use gpui::px;

        let databases = self.list_databases_detailed(connection).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("engine", "Engine").width(px(120.0)),
            Column::new("tables", "Tables").width(px(80.0)).text_right(),
            Column::new("comment", "Comment").width(px(300.0)),
        ];

        let rows: Vec<Vec<String>> = databases
            .iter()
            .map(|db| {
                vec![
                    db.name.clone(),
                    db.charset.as_deref().unwrap_or("-").to_string(), // Using charset field for engine
                    db.table_count
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    db.comment.as_deref().unwrap_or("").to_string(),
                ]
            })
            .collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Database,
            title: "Databases".to_string(),
            columns,
            rows,
        })
    }

    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        let result = connection
            .query(
                "SELECT name, engine, comment FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name",
                None,
                ExecOptions::default(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut databases = Vec::new();

            for row in query_result.rows {
                if let (Some(name), engine, comment) = (
                    row.get(0).and_then(|v| v.clone()),
                    row.get(1).and_then(|v| v.clone()),
                    row.get(2).and_then(|v| v.clone()),
                ) {
                    databases.push(DatabaseInfo {
                        name: name.clone(),
                        charset: engine, // Store engine in charset field
                        collation: None,
                        size: None,
                        table_count: None,
                        comment,
                    });
                }
            }

            Ok(databases)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    // === Table Operations ===

    async fn list_tables(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            "SELECT name, engine, comment FROM system.tables WHERE database = '{}' ORDER BY name",
            database.replace("'", "''")
        );

        let result = connection
            .query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut tables = Vec::new();

            for row in query_result.rows {
                if let (Some(name), engine) = (
                    row.get(0).and_then(|v| v.clone()),
                    row.get(1).and_then(|v| v.clone()),
                ) {
                    let comment = row.get(2).and_then(|v| v.clone());

                    tables.push(TableInfo {
                        name: name.clone(),
                        schema: None,
                        row_count: None,
                        create_time: None,
                        charset: None,
                        collation: None,
                        engine,
                        comment,
                    });
                }
            }

            Ok(tables)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let tables = self.list_tables(connection, database).await?;

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("engine", "Engine").width(px(150.0)),
            Column::new("comment", "Comment").width(px(300.0)),
        ];

        let rows: Vec<Vec<String>> = tables
            .iter()
            .map(|table| {
                vec![
                    table.name.clone(),
                    table.engine.as_deref().unwrap_or("-").to_string(),
                    table.comment.as_deref().unwrap_or("").to_string(),
                ]
            })
            .collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Table,
            title: "Tables".to_string(),
            columns,
            rows,
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT name, type, default_kind, default_expression, comment, is_in_primary_key FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            database.replace("'", "''"),
            table.replace("'", "''")
        );

        let result = connection
            .query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut columns = Vec::new();

            for row in query_result.rows {
                if let (Some(name), Some(data_type)) = (
                    row.get(0).and_then(|v| v.clone()),
                    row.get(1).and_then(|v| v.clone()),
                ) {
                    let default_kind = row.get(2).and_then(|v| v.clone());
                    let default_expression = row.get(3).and_then(|v| v.clone());
                    let comment = row.get(4).and_then(|v| v.clone());
                    let is_primary_key = row.get(5).and_then(|v| v.clone()).map(|v| v == "1").unwrap_or(false);

                    let is_nullable = data_type.starts_with("Nullable(");
                    let default_value = if default_kind.as_deref() == Some("DEFAULT") {
                        default_expression
                    } else {
                        None
                    };

                    columns.push(ColumnInfo {
                        name: name.clone(),
                        data_type: data_type.clone(),
                        is_nullable,
                        default_value,
                        is_primary_key,
                        comment,
                    });
                }
            }

            Ok(columns)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns = self.list_columns(connection, database, table).await?;

        let column_defs = vec![
            Column::new("name", "Name").width(px(150.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("nullable", "Nullable").width(px(80.0)),
            Column::new("default", "Default").width(px(150.0)),
            Column::new("comment", "Comment").width(px(200.0)),
        ];

        let rows: Vec<Vec<String>> = columns
            .iter()
            .map(|col| {
                vec![
                    col.name.clone(),
                    col.data_type.clone(),
                    if col.is_nullable { "YES" } else { "NO" }.to_string(),
                    col.default_value.as_deref().unwrap_or("").to_string(),
                    col.comment.as_deref().unwrap_or("").to_string(),
                ]
            })
            .collect();

        Ok(ObjectView {
            db_node_type: DbNodeType::Column,
            title: "Columns".to_string(),
            columns: column_defs,
            rows,
        })
    }

    async fn list_indexes(&self, _connection: &dyn DbConnection, _database: &str, _table: &str) -> Result<Vec<IndexInfo>> {
        // ClickHouse doesn't have traditional indexes like MySQL
        // It has data skipping indexes, but they are not directly comparable
        Ok(Vec::new())
    }

    async fn list_indexes_view(&self, _connection: &dyn DbConnection, _database: &str, _table: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::Index,
            title: "Indexes".to_string(),
            columns: vec![
                Column::new("name", "Name").width(px(150.0)),
                Column::new("type", "Type").width(px(100.0)),
                Column::new("columns", "Columns").width(px(200.0)),
            ],
            rows: Vec::new(),
        })
    }

    // === View Operations ===

    async fn list_views(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<ViewInfo>> {
        // ClickHouse has materialized views, which are different from traditional views
        // For now, return empty
        Ok(Vec::new())
    }

    async fn list_views_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::View,
            title: "Views".to_string(),
            columns: vec![
                Column::new("name", "Name").width(px(200.0)),
                Column::new("definition", "Definition").width(px(400.0)),
            ],
            rows: Vec::new(),
        })
    }

    // === Function Operations ===

    async fn list_functions(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_functions_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::Function,
            title: "Functions".to_string(),
            columns: vec![Column::new("name", "Name").width(px(200.0))],
            rows: Vec::new(),
        })
    }

    // === Procedure Operations ===

    async fn list_procedures(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_procedures_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::Procedure,
            title: "Procedures".to_string(),
            columns: vec![Column::new("name", "Name").width(px(200.0))],
            rows: Vec::new(),
        })
    }

    // === Trigger Operations ===

    async fn list_triggers(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<TriggerInfo>> {
        Ok(Vec::new())
    }

    async fn list_triggers_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::Trigger,
            title: "Triggers".to_string(),
            columns: vec![Column::new("name", "Name").width(px(200.0))],
            rows: Vec::new(),
        })
    }

    // === Sequence Operations ===

    async fn list_sequences(&self, _connection: &dyn DbConnection, _database: &str) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }

    async fn list_sequences_view(&self, _connection: &dyn DbConnection, _database: &str) -> Result<ObjectView> {
        use gpui::px;

        Ok(ObjectView {
            db_node_type: DbNodeType::Sequence,
            title: "Sequences".to_string(),
            columns: vec![Column::new("name", "Name").width(px(200.0))],
            rows: Vec::new(),
        })
    }

    fn build_column_definition(&self, column: &ColumnInfo, include_name: bool) -> String {
        let mut def = String::new();

        if include_name {
            def.push_str(&self.quote_identifier(&column.name));
            def.push(' ');
        }

        if column.is_nullable {
            def.push_str(&format!("Nullable({})", column.data_type));
        } else {
            def.push_str(&column.data_type);
        }

        if let Some(default) = &column.default_value {
            def.push_str(&format!(" DEFAULT {}", default));
        }

        if let Some(comment) = &column.comment {
            def.push_str(&format!(" COMMENT '{}'", comment.replace("'", "''")));
        }

        def
    }

    // === Database Management Operations ===

    fn build_create_database_sql(&self, request: &DatabaseOperationRequest) -> String {
        let db_name = self.quote_identifier(&request.database_name);
        let mut sql = format!("CREATE DATABASE {}", db_name);

        if let Some(engine) = request.field_values.get("engine") {
            if !engine.is_empty() {
                sql.push_str(&format!(" ENGINE = {}", engine));
            }
        }

        if let Some(comment) = request.field_values.get("comment") {
            if !comment.is_empty() {
                sql.push_str(&format!(" COMMENT '{}'", comment.replace("'", "''")));
            }
        }

        sql
    }

    fn build_modify_database_sql(&self, request: &DatabaseOperationRequest) -> String {
        // ClickHouse doesn't support ALTER DATABASE for changing properties
        // Return a comment indicating this
        format!("-- ClickHouse does not support modifying database properties for '{}'", request.database_name)
    }

    fn build_drop_database_sql(&self, database_name: &str) -> String {
        format!("DROP DATABASE IF EXISTS {}", self.quote_identifier(database_name))
    }

    fn rename_table(&self, _database: &str, old_name: &str, new_name: &str) -> String {
        format!("RENAME TABLE {} TO {}", self.quote_identifier(old_name), self.quote_identifier(new_name))
    }

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

        if col.is_nullable {
            type_str = format!("Nullable({})", type_str);
        }
        def.push_str(&type_str);

        if let Some(default) = &col.default_value {
            if !default.is_empty() {
                def.push_str(&format!(" DEFAULT {}", default));
            }
        }

        if !col.comment.is_empty() {
            def.push_str(&format!(" COMMENT '{}'", col.comment.replace("'", "''")));
        }

        def
    }

    fn build_create_table_sql(&self, design: &TableDesign) -> String {
        let mut sql = String::new();
        sql.push_str("CREATE TABLE ");
        sql.push_str(&self.quote_identifier(&design.table_name));
        sql.push_str(" (\n");

        let mut definitions: Vec<String> = Vec::new();

        for col in &design.columns {
            definitions.push(format!("  {}", self.build_column_def(col)));
        }

        sql.push_str(&definitions.join(",\n"));
        sql.push_str("\n)");

        if let Some(engine) = &design.options.engine {
            sql.push_str(&format!(" ENGINE = {}", engine));
        } else {
            sql.push_str(" ENGINE = MergeTree()");
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
            sql.push_str(&format!(" ORDER BY ({})", pk_cols.join(", ")));
        }

        sql.push(';');
        sql
    }

    fn build_limit_clause(&self) -> String {
        " LIMIT 1".to_string()
    }

    fn build_where_and_limit_clause(
        &self,
        request: &crate::types::TableSaveRequest,
        original_data: &[String],
    ) -> (String, String) {
        let where_clause = self.build_table_change_where_clause(request, original_data);
        (where_clause, self.build_limit_clause())
    }

    fn build_alter_table_sql(&self, original: &TableDesign, new: &TableDesign) -> String {
        let mut statements: Vec<String> = Vec::new();
        let table_name = self.quote_identifier(&new.table_name);

        let original_cols: std::collections::HashMap<&str, &ColumnDefinition> = original.columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        let new_cols: std::collections::HashMap<&str, &ColumnDefinition> = new.columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        for name in original_cols.keys() {
            if !new_cols.contains_key(name) {
                statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {};",
                    table_name,
                    self.quote_identifier(name)
                ));
            }
        }

        for col in new.columns.iter() {
            if let Some(orig_col) = original_cols.get(col.name.as_str()) {
                if self.column_changed(orig_col, col) {
                    let type_str = self.build_type_string(col);
                    statements.push(format!(
                        "ALTER TABLE {} MODIFY COLUMN {} {};",
                        table_name,
                        self.quote_identifier(&col.name),
                        type_str
                    ));
                }
            } else {
                let col_def = self.build_column_def(col);
                statements.push(format!(
                    "ALTER TABLE {} ADD COLUMN {};",
                    table_name, col_def
                ));
            }
        }

        let original_indexes: std::collections::HashMap<&str, &IndexDefinition> = original.indexes
            .iter()
            .map(|i| (i.name.as_str(), i))
            .collect();
        let new_indexes: std::collections::HashMap<&str, &IndexDefinition> = new.indexes
            .iter()
            .map(|i| (i.name.as_str(), i))
            .collect();

        for name in original_indexes.keys() {
            if !new_indexes.contains_key(name) {
                statements.push(format!(
                    "ALTER TABLE {} DROP INDEX {};",
                    table_name,
                    self.quote_identifier(name)
                ));
            }
        }

        for (name, idx) in &new_indexes {
            if !original_indexes.contains_key(name) {
                let idx_cols: Vec<String> = idx.columns.iter()
                    .map(|c| self.quote_identifier(c))
                    .collect();

                statements.push(format!(
                    "ALTER TABLE {} ADD INDEX {} ({});",
                    table_name,
                    self.quote_identifier(name),
                    idx_cols.join(", ")
                ));
            }
        }

        if statements.is_empty() {
            "-- No changes detected".to_string()
        } else {
            statements.join("\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::DatabasePlugin;
    use crate::types::{ColumnDefinition, IndexDefinition, TableDesign, TableOptions};
    use std::collections::HashMap;

    fn create_plugin() -> ClickHousePlugin {
        ClickHousePlugin::new()
    }

    // ==================== Basic Plugin Info Tests ====================

    #[test]
    fn test_plugin_name() {
        let plugin = create_plugin();
        assert_eq!(plugin.name(), DatabaseType::ClickHouse);
    }

    #[test]
    fn test_identifier_quote() {
        let plugin = create_plugin();
        assert_eq!(plugin.identifier_quote(), "`");
    }

    #[test]
    fn test_quote_identifier() {
        let plugin = create_plugin();
        assert_eq!(plugin.quote_identifier("table_name"), "`table_name`");
        assert_eq!(plugin.quote_identifier("column"), "`column`");
    }

    // ==================== DDL SQL Generation Tests ====================

    #[test]
    fn test_drop_database() {
        let plugin = create_plugin();
        let sql = plugin.drop_database("test_db");
        assert!(sql.contains("DROP DATABASE"));
        assert!(sql.contains("`test_db`"));
    }

    #[test]
    fn test_drop_table() {
        let plugin = create_plugin();
        let sql = plugin.drop_table("test_db", "users");
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("`users`"));
    }

    #[test]
    fn test_truncate_table() {
        let plugin = create_plugin();
        let sql = plugin.truncate_table("test_db", "users");
        assert!(sql.contains("TRUNCATE TABLE"));
        assert!(sql.contains("`users`"));
    }

    #[test]
    fn test_rename_table() {
        let plugin = create_plugin();
        let sql = plugin.rename_table("test_db", "old_name", "new_name");
        assert!(sql.contains("RENAME TABLE"));
        assert!(sql.contains("`old_name`"));
        assert!(sql.contains("`new_name`"));
    }

    #[test]
    fn test_drop_view() {
        let plugin = create_plugin();
        let sql = plugin.drop_view("test_db", "my_view");
        assert!(sql.contains("DROP VIEW"));
        assert!(sql.contains("`my_view`"));
    }

    // ==================== Database Operations Tests ====================

    #[test]
    fn test_build_create_database_sql() {
        let plugin = create_plugin();
        let mut field_values = HashMap::new();
        field_values.insert("engine".to_string(), "Atomic".to_string());

        let request = crate::plugin::DatabaseOperationRequest {
            database_name: "new_db".to_string(),
            field_values,
        };

        let sql = plugin.build_create_database_sql(&request);
        assert!(sql.contains("CREATE DATABASE"));
        assert!(sql.contains("`new_db`"));
        assert!(sql.contains("ENGINE = Atomic"));
    }

    #[test]
    fn test_build_modify_database_sql() {
        let plugin = create_plugin();
        let field_values = HashMap::new();

        let request = crate::plugin::DatabaseOperationRequest {
            database_name: "my_db".to_string(),
            field_values,
        };

        let sql = plugin.build_modify_database_sql(&request);
        assert!(sql.contains("--"));
    }

    #[test]
    fn test_build_drop_database_sql() {
        let plugin = create_plugin();
        let sql = plugin.build_drop_database_sql("old_db");
        assert!(sql.contains("DROP DATABASE IF EXISTS"));
        assert!(sql.contains("`old_db`"));
    }

    // ==================== Column Definition Tests ====================

    #[test]
    fn test_build_column_def_simple() {
        let plugin = create_plugin();
        let col = ColumnDefinition::new("id")
            .data_type("UInt64")
            .nullable(false);

        let def = plugin.build_column_def(&col);
        assert!(def.contains("`id`"));
        assert!(def.contains("UInt64"));
        // ClickHouse uses Nullable() wrapper, not NOT NULL keyword
        assert!(!def.contains("Nullable"));
    }

    #[test]
    fn test_build_column_def_string() {
        let plugin = create_plugin();
        let col = ColumnDefinition::new("name")
            .data_type("String")
            .nullable(true);

        let def = plugin.build_column_def(&col);
        assert!(def.contains("`name`"));
        assert!(def.contains("String"));
    }

    #[test]
    fn test_build_column_def_with_default() {
        let plugin = create_plugin();
        let mut col = ColumnDefinition::new("status")
            .data_type("UInt8")
            .default_value("0");
        col.is_nullable = false;

        let def = plugin.build_column_def(&col);
        assert!(def.contains("DEFAULT 0"));
        // ClickHouse uses Nullable() wrapper, not NOT NULL keyword
        assert!(!def.contains("Nullable"));
    }

    // ==================== CREATE TABLE Tests ====================

    #[test]
    fn test_build_create_table_sql_simple() {
        let plugin = create_plugin();
        let design = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id")
                    .data_type("UInt64")
                    .nullable(false)
                    .primary_key(true),
                ColumnDefinition::new("event_name")
                    .data_type("String"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_create_table_sql(&design);
        assert!(sql.contains("CREATE TABLE `events`"));
        assert!(sql.contains("`id`"));
        assert!(sql.contains("UInt64"));
        assert!(sql.contains("`event_name`"));
        assert!(sql.contains("String"));
        // ClickHouse uses ORDER BY instead of PRIMARY KEY
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_build_create_table_sql_with_indexes() {
        let plugin = create_plugin();
        let design = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "logs".to_string(),
            columns: vec![
                ColumnDefinition::new("id")
                    .data_type("UInt64")
                    .nullable(false)
                    .primary_key(true),
                ColumnDefinition::new("user_id")
                    .data_type("UInt32")
                    .nullable(false),
            ],
            indexes: vec![
                IndexDefinition::new("idx_user_id")
                    .columns(vec!["user_id".to_string()])
                    .unique(false),
            ],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_create_table_sql(&design);
        // ClickHouse indexes are created separately, not in CREATE TABLE
        assert!(sql.contains("CREATE TABLE `logs`"));
        assert!(sql.contains("ORDER BY"));
    }

    // ==================== ALTER TABLE Tests ====================

    #[test]
    fn test_build_alter_table_sql_add_column() {
        let plugin = create_plugin();

        let original = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("UInt64"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let new = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("UInt64"),
                ColumnDefinition::new("timestamp").data_type("DateTime"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_alter_table_sql(&original, &new);
        assert!(sql.contains("ADD COLUMN"));
        assert!(sql.contains("`timestamp`"));
    }

    #[test]
    fn test_build_alter_table_sql_drop_column() {
        let plugin = create_plugin();

        let original = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("UInt64"),
                ColumnDefinition::new("old_column").data_type("String"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let new = TableDesign {
            database_name: "test_db".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("UInt64"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_alter_table_sql(&original, &new);
        assert!(sql.contains("DROP COLUMN"));
        assert!(sql.contains("`old_column`"));
    }

    // ==================== Completion Info Tests ====================

    #[test]
    fn test_get_completion_info() {
        let plugin = create_plugin();
        let info = plugin.get_completion_info();

        assert!(!info.keywords.is_empty());
        assert!(!info.functions.is_empty());
        assert!(!info.operators.is_empty());
        assert!(!info.data_types.is_empty());
        assert!(!info.snippets.is_empty());

        assert!(info.keywords.iter().any(|(k, _)| *k == "FINAL"));
        assert!(info.data_types.iter().any(|(t, _)| *t == "UInt64"));
    }
}
