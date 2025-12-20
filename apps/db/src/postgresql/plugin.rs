use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, ExecResult, SqlResult};
use crate::plugin::{DatabasePlugin, SqlCompletionInfo};
use crate::postgresql::connection::PostgresDbConnection;
use crate::types::*;

/// PostgreSQL database plugin implementation (stateless)
pub struct PostgresPlugin;

impl PostgresPlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DatabasePlugin for PostgresPlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::PostgreSQL
    }

    fn supports_schema(&self) -> bool {
        true
    }

    async fn list_schemas(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<String>> {
        let result = connection.query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
             ORDER BY schema_name",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list schemas: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter()
                .filter_map(|row| row.first().and_then(|v| v.clone()))
                .collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                // PostgreSQL-specific keywords only
                ("RETURNING", "Return inserted/updated rows"),
                ("SERIAL", "Auto-incrementing integer"),
                ("BIGSERIAL", "Auto-incrementing bigint"),
                ("CASCADE", "Cascade to dependent objects"),
                ("RESTRICT", "Restrict if dependencies exist"),
                ("CONCURRENTLY", "Non-blocking index creation"),
                ("ONLY", "Exclude inherited tables"),
                ("LATERAL", "Lateral subquery"),
                ("FETCH FIRST", "Limit rows (SQL standard)"),
                ("FOR UPDATE", "Lock rows for update"),
                ("FOR SHARE", "Lock rows for share"),
                ("SKIP LOCKED", "Skip locked rows"),
                ("NOWAIT", "Don't wait for locks"),
                ("NULLS FIRST", "Sort NULLs first"),
                ("NULLS LAST", "Sort NULLs last"),
                ("ILIKE", "Case-insensitive LIKE"),
                ("SIMILAR TO", "SQL regex pattern match"),
                ("OVER", "Window function clause"),
                ("PARTITION BY", "Window partition"),
                ("ROWS BETWEEN", "Window frame"),
                ("RANGE BETWEEN", "Window frame range"),
                ("WITH RECURSIVE", "Recursive CTE"),
                ("MATERIALIZED", "Materialized CTE"),
                ("NOT MATERIALIZED", "Non-materialized CTE"),
                ("TABLESAMPLE", "Sample table rows"),
                ("BERNOULLI", "Bernoulli sampling"),
                ("SYSTEM", "System sampling"),
            ],
            functions: vec![
                // PostgreSQL-specific functions only (standard SQL functions are added via with_standard_sql())
                ("CONCAT_WS(sep, str1, str2, ...)", "Concatenate with separator"),
                ("SUBSTRING(str FROM pos FOR len)", "Extract substring (PostgreSQL syntax)"),
                ("CHAR_LENGTH(str)", "Character length"),
                ("LPAD(str, len, fill)", "Left pad string"),
                ("RPAD(str, len, fill)", "Right pad string"),
                ("POSITION(sub IN str)", "Find substring position"),
                ("STRPOS(str, sub)", "Find substring position"),
                ("REPEAT(str, n)", "Repeat string"),
                ("SPLIT_PART(str, delim, n)", "Split and get part"),
                ("STRING_AGG(expr, delim)", "Aggregate strings"),
                ("INITCAP(str)", "Capitalize words"),
                ("REGEXP_REPLACE(str, pat, rep)", "Regex replace"),
                ("REGEXP_MATCHES(str, pat)", "Regex matches"),
                ("REGEXP_SPLIT_TO_ARRAY(str, pat)", "Split by regex"),
                ("TRANSLATE(str, from, to)", "Character translation"),
                ("TRUNC(x, s)", "Truncate to scale"),
                ("RANDOM()", "Random 0-1"),
                ("DIV(x, y)", "Integer division"),
                ("LOG(x)", "Natural logarithm"),
                ("LOG10(x)", "Base-10 logarithm"),
                ("EXP(x)", "Exponential"),
                ("GREATEST(a, b, ...)", "Maximum value"),
                ("LEAST(a, b, ...)", "Minimum value"),
                ("LOCALTIME", "Local time"),
                ("LOCALTIMESTAMP", "Local timestamp"),
                ("DATE_TRUNC(field, source)", "Truncate to precision"),
                ("DATE_PART(field, source)", "Extract field"),
                ("EXTRACT(field FROM source)", "Extract field"),
                ("AGE(ts1, ts2)", "Interval between timestamps"),
                ("AGE(ts)", "Age from current date"),
                ("MAKE_DATE(y, m, d)", "Create date"),
                ("MAKE_TIME(h, m, s)", "Create time"),
                ("MAKE_TIMESTAMP(y,m,d,h,mi,s)", "Create timestamp"),
                ("MAKE_INTERVAL(...)", "Create interval"),
                ("TO_CHAR(val, fmt)", "Format to string"),
                ("TO_DATE(str, fmt)", "Parse date"),
                ("TO_TIMESTAMP(str, fmt)", "Parse timestamp"),
                ("TO_NUMBER(str, fmt)", "Parse number"),
                ("CLOCK_TIMESTAMP()", "Actual current time"),
                ("STATEMENT_TIMESTAMP()", "Statement start time"),
                ("TRANSACTION_TIMESTAMP()", "Transaction start time"),
                ("ARRAY_AGG(col)", "Aggregate to array"),
                ("JSON_AGG(col)", "Aggregate to JSON array"),
                ("JSONB_AGG(col)", "Aggregate to JSONB array"),
                ("JSON_OBJECT_AGG(k, v)", "Aggregate to JSON object"),
                ("BOOL_AND(col)", "Logical AND"),
                ("BOOL_OR(col)", "Logical OR"),
                ("BIT_AND(col)", "Bitwise AND"),
                ("BIT_OR(col)", "Bitwise OR"),
                ("ROW_NUMBER()", "Row number in partition"),
                ("RANK()", "Rank with gaps"),
                ("DENSE_RANK()", "Rank without gaps"),
                ("NTILE(n)", "Divide into n buckets"),
                ("LAG(col, n)", "Previous row value"),
                ("LEAD(col, n)", "Next row value"),
                ("FIRST_VALUE(col)", "First value in frame"),
                ("LAST_VALUE(col)", "Last value in frame"),
                ("NTH_VALUE(col, n)", "Nth value in frame"),
                ("PERCENT_RANK()", "Relative rank"),
                ("CUME_DIST()", "Cumulative distribution"),
                ("JSON_BUILD_OBJECT(k, v, ...)", "Build JSON object"),
                ("JSON_BUILD_ARRAY(v, ...)", "Build JSON array"),
                ("JSONB_BUILD_OBJECT(k, v, ...)", "Build JSONB object"),
                ("JSONB_BUILD_ARRAY(v, ...)", "Build JSONB array"),
                ("JSON_EXTRACT_PATH(json, ...)", "Extract JSON path"),
                ("JSONB_EXTRACT_PATH(json, ...)", "Extract JSONB path"),
                ("JSON_EXTRACT_PATH_TEXT(json, ...)", "Extract as text"),
                ("JSONB_SET(target, path, val)", "Set JSONB value"),
                ("JSONB_INSERT(target, path, val)", "Insert JSONB value"),
                ("JSONB_PRETTY(jsonb)", "Pretty print JSONB"),
                ("JSONB_TYPEOF(jsonb)", "JSONB type"),
                ("JSONB_ARRAY_LENGTH(jsonb)", "JSONB array length"),
                ("JSONB_EACH(jsonb)", "Expand JSONB object"),
                ("JSONB_ARRAY_ELEMENTS(jsonb)", "Expand JSONB array"),
                ("JSONB_STRIP_NULLS(jsonb)", "Remove null values"),
                ("JSONB_PATH_QUERY(target, path)", "JSONPath query"),
                ("ARRAY_LENGTH(arr, dim)", "Array length"),
                ("ARRAY_DIMS(arr)", "Array dimensions"),
                ("ARRAY_UPPER(arr, dim)", "Upper bound"),
                ("ARRAY_LOWER(arr, dim)", "Lower bound"),
                ("ARRAY_POSITION(arr, elem)", "Element position"),
                ("ARRAY_POSITIONS(arr, elem)", "All positions"),
                ("ARRAY_REMOVE(arr, elem)", "Remove element"),
                ("ARRAY_REPLACE(arr, from, to)", "Replace element"),
                ("ARRAY_CAT(arr1, arr2)", "Concatenate arrays"),
                ("ARRAY_APPEND(arr, elem)", "Append element"),
                ("ARRAY_PREPEND(elem, arr)", "Prepend element"),
                ("UNNEST(arr)", "Expand array to rows"),
                ("GEN_RANDOM_UUID()", "Generate UUID"),
                ("MD5(str)", "MD5 hash"),
                ("ENCODE(data, fmt)", "Encode binary"),
                ("DECODE(str, fmt)", "Decode to binary"),
                ("PG_TYPEOF(val)", "Value type"),
                ("CURRENT_USER", "Current user"),
                ("CURRENT_DATABASE()", "Current database"),
                ("CURRENT_SCHEMA()", "Current schema"),
                ("VERSION()", "PostgreSQL version"),
            ],
            operators: vec![
                ("~", "Regex match (case-sensitive)"),
                ("~*", "Regex match (case-insensitive)"),
                ("!~", "Regex not match (case-sensitive)"),
                ("!~*", "Regex not match (case-insensitive)"),
                ("||", "String/Array concatenation"),
                ("->", "JSON object field"),
                ("->>", "JSON object field as text"),
                ("#>", "JSON path"),
                ("#>>", "JSON path as text"),
                ("@>", "Contains"),
                ("<@", "Contained by"),
                ("?", "Key exists"),
                ("?|", "Any key exists"),
                ("?&", "All keys exist"),
                ("@?", "JSONPath exists"),
                ("@@", "JSONPath match"),
                ("-", "Delete key/element"),
                ("#-", "Delete path"),
                ("&&", "Array overlap"),
                ("<<", "Range strictly left"),
                (">>", "Range strictly right"),
                ("&<", "Range not extend right"),
                ("&>", "Range not extend left"),
                ("-|-", "Range adjacent"),
            ],
            data_types: vec![
                ("SMALLINT", "2 byte integer"),
                ("INTEGER", "4 byte integer"),
                ("BIGINT", "8 byte integer"),
                ("SERIAL", "Auto-increment 4 byte"),
                ("BIGSERIAL", "Auto-increment 8 byte"),
                ("DECIMAL(p,s)", "Exact numeric"),
                ("NUMERIC(p,s)", "Exact numeric"),
                ("REAL", "4 byte float"),
                ("DOUBLE PRECISION", "8 byte float"),
                ("CHAR(n)", "Fixed-length string"),
                ("VARCHAR(n)", "Variable-length string"),
                ("TEXT", "Unlimited text"),
                ("BYTEA", "Binary data"),
                ("DATE", "Date"),
                ("TIME", "Time"),
                ("TIMESTAMP", "Date and time"),
                ("TIMESTAMPTZ", "Timestamp with timezone"),
                ("INTERVAL", "Time interval"),
                ("BOOLEAN", "True/False"),
                ("UUID", "UUID"),
                ("JSON", "JSON"),
                ("JSONB", "Binary JSON"),
                ("XML", "XML"),
                ("ARRAY", "Array"),
                ("INT4RANGE", "Integer range"),
                ("INT8RANGE", "Bigint range"),
                ("NUMRANGE", "Numeric range"),
                ("TSRANGE", "Timestamp range"),
                ("TSTZRANGE", "Timestamptz range"),
                ("DATERANGE", "Date range"),
                ("INET", "IP address"),
                ("CIDR", "Network address"),
                ("MACADDR", "MAC address"),
                ("POINT", "Geometric point"),
                ("LINE", "Geometric line"),
                ("CIRCLE", "Geometric circle"),
                ("TSVECTOR", "Text search vector"),
                ("TSQUERY", "Text search query"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id SERIAL PRIMARY KEY,\n  $2\n)", "Create table"),
                ("idx", "CREATE INDEX $1 ON $2 ($3)", "Create index"),
                ("cidx", "CREATE INDEX CONCURRENTLY $1 ON $2 ($3)", "Create index concurrently"),
                ("cte", "WITH $1 AS (\n  $2\n)\nSELECT * FROM $1", "Common table expression"),
                ("rcte", "WITH RECURSIVE $1 AS (\n  $2\n  UNION ALL\n  $3\n)\nSELECT * FROM $1", "Recursive CTE"),
                ("wf", "SELECT $1,\n  ROW_NUMBER() OVER (PARTITION BY $2 ORDER BY $3) AS rn\nFROM $4", "Window function"),
            ],
        }.with_standard_sql()
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        let mut conn = PostgresDbConnection::new(config);
        conn.connect().await?;
        Ok(Box::new(conn))
    }

    // === Database/Schema Level Operations ===

    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>> {
        let result = connection.query(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter()
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
            Column::new("name", "Name").width(px(180.0)),
            Column::new("charset", "Encoding").width(px(120.0)),
            Column::new("collation", "Collation").width(px(180.0)),
            Column::new("size", "Size").width(px(100.0)).text_right(),
            Column::new("tables", "Tables").width(px(80.0)).text_right(),
            Column::new("comment", "Comment").width(px(250.0)),
        ];
        
        let rows: Vec<Vec<String>> = databases.iter().map(|db| {
            vec![
                db.name.clone(),
                db.charset.as_deref().unwrap_or("-").to_string(),
                db.collation.as_deref().unwrap_or("-").to_string(),
                db.size.as_deref().unwrap_or("-").to_string(),
                db.table_count.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                db.comment.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Database,
            title: format!("{} database(s)", databases.len()),
            columns,
            rows,
        })
    }

    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        let result = connection.query(
            "SELECT 
                d.datname as name,
                pg_encoding_to_char(d.encoding) as charset,
                d.datcollate as collation,
                pg_size_pretty(pg_database_size(d.datname)) as size,
                (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public') as table_count,
                shobj_description(d.oid, 'pg_database') as comment
            FROM pg_database d
            WHERE d.datistemplate = false 
            ORDER BY d.datname",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let databases: Vec<DatabaseInfo> = query_result.rows.iter()
                .filter_map(|row| {
                    let name = row.first().and_then(|v| v.clone())?;
                    let charset = row.get(1).and_then(|v| v.clone());
                    let collation = row.get(2).and_then(|v| v.clone());
                    let size = row.get(3).and_then(|v| v.clone());
                    let table_count = row.get(4).and_then(|v| v.clone()).and_then(|s| s.parse::<i64>().ok());
                    let comment = row.get(5).and_then(|v| v.clone());
                    
                    Some(DatabaseInfo {
                        name,
                        charset,
                        collation,
                        size,
                        table_count,
                        comment,
                    })
                })
                .collect();
            Ok(databases)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }
    

    // === Table Operations ===

    async fn list_tables(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<TableInfo>> {
        let sql = "SELECT \
                t.tablename, \
                t.schemaname, \
                obj_description((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass) AS table_comment, \
                (SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = t.tablename AND n.nspname = t.schemaname) AS row_count \
             FROM pg_tables t \
             WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY t.schemaname, t.tablename";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let tables: Vec<TableInfo> = query_result.rows.iter().map(|row| {
                let row_count = row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse::<i64>().ok());

                TableInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    schema: row.get(1).and_then(|v| v.clone()),
                    comment: row.get(2).and_then(|v| v.clone()).filter(|s| !s.is_empty()),
                    engine: None,
                    row_count,
                    create_time: None,
                    charset: None,
                    collation: None,
                }
            }).collect();

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
            Column::new("rows", "Rows").width(px(100.0)).text_right(),
            Column::new("comment", "Comment").width(px(400.0)),
        ];
        
        let rows: Vec<Vec<String>> = tables.iter().map(|table| {
            vec![
                table.name.clone(),
                table.row_count.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                table.comment.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Table,
            title: format!("{} table(s)", tables.len()),
            columns,
            rows,
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, _database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT column_name, data_type, is_nullable, column_default, \
             (SELECT COUNT(*) FROM information_schema.key_column_usage kcu \
              WHERE kcu.table_name = c.table_name AND kcu.column_name = c.column_name \
              AND kcu.table_schema = 'public' AND EXISTS \
              (SELECT 1 FROM information_schema.table_constraints tc \
               WHERE tc.constraint_name = kcu.constraint_name AND tc.constraint_type = 'PRIMARY KEY')) > 0 AS is_primary \
             FROM information_schema.columns c \
             WHERE table_schema = 'public' AND table_name = '{}' \
             ORDER BY ordinal_position",
            table
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ColumnInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    data_type: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    is_nullable: row.get(2).and_then(|v| v.clone()).map(|v| v == "YES").unwrap_or(true),
                    is_primary_key: row.get(4).and_then(|v| v.clone()).map(|v| v == "t" || v == "true" || v == "1").unwrap_or(false),
                    default_value: row.get(3).and_then(|v| v.clone()),
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let columns_data = self.list_columns(connection, database, table).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("nullable", "Nullable").width(px(80.0)),
            Column::new("key", "Key").width(px(80.0)),
            Column::new("default", "Default").width(px(200.0)),
        ];
        
        let rows: Vec<Vec<String>> = columns_data.iter().map(|col| {
            vec![
                col.name.clone(),
                col.data_type.clone(),
                if col.is_nullable { "YES" } else { "NO" }.to_string(),
                if col.is_primary_key { "PRI" } else { "" }.to_string(),
                col.default_value.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Column,
            title: format!("{} column(s)", columns_data.len()),
            columns,
            rows,
        })
    }

    async fn list_indexes(&self, connection: &dyn DbConnection, _database: &str, table: &str) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            "SELECT i.relname AS index_name, \
             a.attname AS column_name, \
             ix.indisunique AS is_unique \
             FROM pg_class t \
             JOIN pg_index ix ON t.oid = ix.indrelid \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) \
             WHERE t.relname = '{}' AND t.relkind = 'r' \
             ORDER BY i.relname, a.attnum",
            table
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list indexes: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut indexes: HashMap<String, IndexInfo> = HashMap::new();

            for row in query_result.rows {
                let index_name = row.first().and_then(|v| v.clone()).unwrap_or_default();
                let column_name = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
                let is_unique = row.get(2).and_then(|v| v.clone()).map(|v| v == "t" || v == "true").unwrap_or(false);

                indexes.entry(index_name.clone())
                    .or_insert_with(|| IndexInfo {
                        name: index_name,
                        columns: Vec::new(),
                        is_unique,
                        index_type: Some("btree".to_string()),
                    })
                    .columns.push(column_name);
            }

            Ok(indexes.into_values().collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let indexes = self.list_indexes(connection, database, table).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("columns", "Columns").width(px(250.0)),
            Column::new("unique", "Unique").width(px(80.0)),
            Column::new("type", "Type").width(px(120.0)),
        ];
        
        let rows: Vec<Vec<String>> = indexes.iter().map(|idx| {
            vec![
                idx.name.clone(),
                idx.columns.join(", "),
                if idx.is_unique { "YES" } else { "NO" }.to_string(),
                idx.index_type.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Index,
            title: format!("{} index(es)", indexes.len()),
            columns,
            rows,
        })
    }


    // === View Operations ===

    async fn list_views(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<ViewInfo>> {
        let sql = "SELECT table_name, table_schema, view_definition FROM information_schema.views \
                   WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                   ORDER BY table_schema, table_name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list views: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ViewInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    schema: row.get(1).and_then(|v| v.clone()),
                    definition: row.get(2).and_then(|v| v.clone()),
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_views_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let views = self.list_views(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("definition", "Definition").width(px(400.0)),
        ];
        
        let rows: Vec<Vec<String>> = views.iter().map(|view| {
            vec![
                view.name.clone(),
                view.definition.as_deref().unwrap_or("").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::View,
            title: format!("{} view(s)", views.len()),
            columns,
            rows,
        })
    }

    // === Function Operations ===

    async fn list_functions(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = "SELECT routine_name, data_type FROM information_schema.routines WHERE routine_schema = 'public' AND routine_type = 'FUNCTION' ORDER BY routine_name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list functions: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: row.get(1).and_then(|v| v.clone()),
                    parameters: Vec::new(),
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_functions_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let functions = self.list_functions(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("return_type", "Return Type").width(px(150.0)),
        ];
        
        let rows: Vec<Vec<String>> = functions.iter().map(|func| {
            vec![
                func.name.clone(),
                func.return_type.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Function,
            title: format!("{} function(s)", functions.len()),
            columns,
            rows,
        })
    }


    // === Procedure Operations ===

    async fn list_procedures(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = "SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public' AND routine_type = 'PROCEDURE' ORDER BY routine_name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list procedures: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: None,
                    parameters: Vec::new(),
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_procedures_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let procedures = self.list_procedures(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
        ];
        
        let rows: Vec<Vec<String>> = procedures.iter().map(|proc| {
            vec![proc.name.clone()]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Procedure,
            title: format!("{} procedure(s)", procedures.len()),
            columns,
            rows,
        })
    }


    // === Trigger Operations ===

    async fn list_triggers(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<TriggerInfo>> {
        let sql = "SELECT trigger_name, event_object_table, event_manipulation, action_timing \
                   FROM information_schema.triggers \
                   WHERE trigger_schema = 'public' \
                   ORDER BY trigger_name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TriggerInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    table_name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    event: row.get(2).and_then(|v| v.clone()).unwrap_or_default(),
                    timing: row.get(3).and_then(|v| v.clone()).unwrap_or_default(),
                    definition: None,
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_triggers_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let triggers = self.list_triggers(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("table", "Table").width(px(150.0)),
            Column::new("event", "Event").width(px(100.0)),
            Column::new("timing", "Timing").width(px(100.0)),
        ];
        
        let rows: Vec<Vec<String>> = triggers.iter().map(|trigger| {
            vec![
                trigger.name.clone(),
                trigger.table_name.clone(),
                trigger.event.clone(),
                trigger.timing.clone(),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Trigger,
            title: format!("{} trigger(s)", triggers.len()),
            columns,
            rows,
        })
    }


    // === Sequence Operations ===

    async fn list_sequences(&self, connection: &dyn DbConnection, _database: &str) -> Result<Vec<SequenceInfo>> {
        let sql = "SELECT sequence_name, start_value::bigint, increment::bigint, min_value::bigint, max_value::bigint \
                   FROM information_schema.sequences \
                   WHERE sequence_schema = 'public' \
                   ORDER BY sequence_name";

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list sequences: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                SequenceInfo {
                    name: row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    start_value: row.get(1).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    increment: row.get(2).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    min_value: row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    max_value: row.get(4).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                }
            }).collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_sequences_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;
        
        let sequences = self.list_sequences(connection, database).await?;
        
        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("start", "Start").width(px(100.0)).text_right(),
            Column::new("increment", "Increment").width(px(100.0)).text_right(),
            Column::new("min", "Min").width(px(120.0)).text_right(),
            Column::new("max", "Max").width(px(120.0)).text_right(),
        ];
        
        let rows: Vec<Vec<String>> = sequences.iter().map(|seq| {
            vec![
                seq.name.clone(),
                seq.start_value.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                seq.increment.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                seq.min_value.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
                seq.max_value.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
            ]
        }).collect();
        
        Ok(ObjectView {
            db_node_type: DbNodeType::Sequence,
            title: format!("{} sequence(s)", sequences.len()),
            columns,
            rows,
        })
    }

    fn get_data_types(&self) -> Vec<DataTypeInfo> {
        vec![
            // 数值类型
            DataTypeInfo::new("SMALLINT", "Small integer (-32768 to 32767)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("INTEGER", "Standard integer").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("BIGINT", "Large integer").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("DECIMAL(10,2)", "Exact numeric").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("NUMERIC(10,2)", "Exact numeric (alias)").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("REAL", "Single-precision floating-point").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("DOUBLE PRECISION", "Double-precision floating-point").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("SERIAL", "Auto-incrementing integer").with_category(DataTypeCategory::Numeric),
            DataTypeInfo::new("BIGSERIAL", "Auto-incrementing bigint").with_category(DataTypeCategory::Numeric),
            
            // 字符串类型
            DataTypeInfo::new("CHAR(255)", "Fixed-length string").with_category(DataTypeCategory::String),
            DataTypeInfo::new("VARCHAR(255)", "Variable-length string").with_category(DataTypeCategory::String),
            DataTypeInfo::new("TEXT", "Variable-length text").with_category(DataTypeCategory::String),
            
            // 日期时间类型
            DataTypeInfo::new("DATE", "Date (no time)").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("TIME", "Time (no date)").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("TIMESTAMP", "Date and time").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("TIMESTAMPTZ", "Timestamp with timezone").with_category(DataTypeCategory::DateTime),
            DataTypeInfo::new("INTERVAL", "Time interval").with_category(DataTypeCategory::DateTime),
            
            // 布尔类型
            DataTypeInfo::new("BOOLEAN", "True/False").with_category(DataTypeCategory::Boolean),
            
            // 二进制类型
            DataTypeInfo::new("BYTEA", "Binary data").with_category(DataTypeCategory::Binary),
            
            // 结构化类型
            DataTypeInfo::new("JSON", "JSON document").with_category(DataTypeCategory::Structured),
            DataTypeInfo::new("JSONB", "Binary JSON (indexed)").with_category(DataTypeCategory::Structured),
            DataTypeInfo::new("XML", "XML document").with_category(DataTypeCategory::Structured),
            DataTypeInfo::new("ARRAY", "Array type").with_category(DataTypeCategory::Structured),
            
            // 其他类型
            DataTypeInfo::new("UUID", "Universally unique identifier").with_category(DataTypeCategory::Other),
            DataTypeInfo::new("INET", "IP address").with_category(DataTypeCategory::Other),
            DataTypeInfo::new("CIDR", "Network address").with_category(DataTypeCategory::Other),
            DataTypeInfo::new("MACADDR", "MAC address").with_category(DataTypeCategory::Other),
        ]
    }

    fn build_create_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        let encoding = request.field_values.get("encoding").map(|s| s.as_str()).unwrap_or("UTF8");

        format!("CREATE DATABASE \"{}\" ENCODING '{}';", db_name, encoding)
    }

    fn build_modify_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        format!("ALTER DATABASE \"{}\" SET search_path = public;", db_name)
    }

    fn build_drop_database_sql(&self, database_name: &str) -> String {
        format!("DROP DATABASE \"{}\";", database_name)
    }
}

impl Default for PostgresPlugin {
    fn default() -> Self {
        Self::new()
    }
}
