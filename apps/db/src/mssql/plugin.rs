use std::collections::HashMap;

use anyhow::Result;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};
use tracing::{info};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, SqlResult};
use crate::mssql::connection::MssqlDbConnection;
use crate::plugin::{DatabasePlugin, SqlCompletionInfo};
use crate::types::*;

/// MSSQL database plugin implementation (stateless)
pub struct MsSqlPlugin;

impl MsSqlPlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DatabasePlugin for MsSqlPlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::MSSQL
    }

    fn supports_schema(&self) -> bool {
        true
    }

    fn supports_sequences(&self) -> bool {
        true
    }

    async fn list_schemas(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<String>> {
        let sql = format!(
            r#"
            SELECT s.name
            FROM [{database}].sys.schemas s
            WHERE s.name NOT IN (
                'INFORMATION_SCHEMA', 'sys',
                'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                'db_backupoperator', 'db_datareader', 'db_datawriter',
                'db_denydatareader', 'db_denydatawriter'
            )
            ORDER BY s.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list schemas: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter()
                .filter_map(|row| row.first().and_then(|v| v.clone()))
                .collect())
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_schemas_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                s.name AS schema_name,
                dp.name AS owner,
                (SELECT COUNT(*) FROM [{database}].sys.tables t WHERE t.schema_id = s.schema_id) AS table_count
            FROM [{database}].sys.schemas s
            LEFT JOIN [{database}].sys.database_principals dp ON s.principal_id = dp.principal_id
            WHERE s.name NOT IN (
                'INFORMATION_SCHEMA', 'sys',
                'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                'db_backupoperator', 'db_datareader', 'db_datawriter',
                'db_denydatareader', 'db_denydatawriter'
            )
            ORDER BY s.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list schemas: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let columns = vec![
                Column::new("name", "Name").width(px(180.0)),
                Column::new("owner", "Owner").width(px(120.0)),
                Column::new("tables", "Tables").width(px(80.0)).text_right(),
            ];

            let rows: Vec<Vec<String>> = query_result.rows.iter().map(|row| {
                vec![
                    row.first().and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(2).and_then(|v| v.clone()).unwrap_or_else(|| "0".to_string()),
                ]
            }).collect();

            Ok(ObjectView {
                db_node_type: DbNodeType::Schema,
                title: format!("{} schema(s)", rows.len()),
                columns,
                rows,
            })
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                // MSSQL-specific keywords
                ("IDENTITY", "Auto-increment column"),
                ("CLUSTERED", "Clustered index"),
                ("NONCLUSTERED", "Non-clustered index"),
                ("FILEGROUP", "Filegroup specification"),
                ("GO", "Batch separator"),
                ("TRY", "Begin try block"),
                ("CATCH", "Begin catch block"),
                ("THROW", "Throw exception"),
                ("RAISERROR", "Raise error"),
                ("EXEC", "Execute stored procedure"),
                ("EXECUTE", "Execute SQL statement"),
                ("WAITFOR", "Wait for time/statement"),
                ("TOP", "Limit rows"),
                ("OFFSET", "Skip rows"),
                ("FETCH", "Fetch rows"),
                ("PARTITION BY", "Partition window function"),
                ("PIVOT", "Pivot rows to columns"),
                ("UNPIVOT", "Unpivot columns to rows"),
                ("CROSS APPLY", "Apply right expression for each left row"),
                ("OUTER APPLY", "Outer apply"),
                ("MERGE", "Merge statement"),
                ("OUTPUT", "Output clause"),
                ("INSERTED", "Inserted pseudo table"),
                ("DELETED", "Deleted pseudo table"),
                ("OVER", "Window function"),
                ("ROW_NUMBER", "Row number window function"),
            ],
            functions: vec![
                // MSSQL-specific functions
                ("LEN(str)", "String length"),
                ("DATALENGTH(expr)", "Data length in bytes"),
                ("CHARINDEX(substr, str)", "Find substring position"),
                ("PATINDEX(pattern, str)", "Pattern index"),
                ("STUFF(str, start, len, new)", "Replace part of string"),
                ("REPLICATE(str, count)", "Repeat string"),
                ("QUOTENAME(str)", "Quote identifier"),
                ("FORMAT(value, format)", "Format value"),
                ("ISNULL(expr, alt)", "Return alt if expr is NULL"),
                ("COALESCE(expr1, expr2, ...)", "Return first non-NULL"),
                ("NULLIF(expr1, expr2)", "Return NULL if equal"),
                ("IIF(cond, then, else)", "Conditional expression"),
                ("CHOOSE(index, val1, val2, ...)", "Choose value by index"),
                ("CAST(expr AS type)", "Type conversion"),
                ("CONVERT(type, expr)", "Type conversion"),
                ("TRY_CAST(expr AS type)", "Safe type conversion"),
                ("TRY_CONVERT(type, expr)", "Safe type conversion"),
                ("GETDATE()", "Current datetime"),
                ("GETUTCDATE()", "Current UTC datetime"),
                ("SYSDATETIME()", "High precision datetime"),
                ("DATEADD(part, num, date)", "Add interval to date"),
                ("DATEDIFF(part, date1, date2)", "Difference between dates"),
                ("DATEPART(part, date)", "Extract date part"),
                ("YEAR(date)", "Extract year"),
                ("MONTH(date)", "Extract month"),
                ("DAY(date)", "Extract day"),
                ("EOMONTH(date)", "End of month"),
                ("DATEFROMPARTS(y,m,d)", "Create date from parts"),
                ("ROUND(num, decimals)", "Round number"),
                ("CEILING(num)", "Ceiling function"),
                ("FLOOR(num)", "Floor function"),
                ("ABS(num)", "Absolute value"),
                ("POWER(x, y)", "Power function"),
                ("SQRT(num)", "Square root"),
                ("RAND()", "Random number 0-1"),
                ("NEWID()", "Generate GUID"),
                ("NEWSEQUENTIALID()", "Generate sequential GUID"),
                ("SCOPE_IDENTITY()", "Last identity value"),
                ("@@IDENTITY", "Last identity value"),
                ("@@ROWCOUNT", "Affected rows count"),
                ("@@VERSION", "SQL Server version"),
                ("DB_NAME()", "Current database name"),
                ("USER_NAME()", "Current user name"),
                ("OBJECT_NAME(id)", "Object name by ID"),
                ("OBJECT_ID(name)", "Object ID by name"),
                ("STRING_AGG(col, sep)", "Aggregate strings"),
                ("STRING_SPLIT(str, sep)", "Split string to table"),
                ("JSON_VALUE(json, path)", "Extract JSON scalar"),
                ("JSON_QUERY(json, path)", "Extract JSON object/array"),
                ("OPENJSON(json)", "Parse JSON to table"),
                ("FOR JSON", "Format result as JSON"),
                ("OPENXML(doc, xpath)", "Parse XML to table"),
                ("ISJSON(expr)", "Check if valid JSON"),
            ],
            operators: vec![
                ("+=", "Add and assign"),
                ("-=", "Subtract and assign"),
                ("*=", "Multiply and assign"),
                ("/=", "Divide and assign"),
                ("%=", "Modulo and assign"),
                ("!=", "Not equal"),
                ("!<", "Not less than"),
                ("!>", "Not greater than"),
            ],
            data_types: vec![
                ("BIT", "Boolean (0/1)"),
                ("TINYINT", "1 byte integer (0-255)"),
                ("SMALLINT", "2 byte integer"),
                ("INT", "4 byte integer"),
                ("BIGINT", "8 byte integer"),
                ("DECIMAL(p,s)", "Fixed-point number"),
                ("NUMERIC(p,s)", "Fixed-point number"),
                ("MONEY", "Currency (8 bytes)"),
                ("SMALLMONEY", "Currency (4 bytes)"),
                ("FLOAT", "Floating point"),
                ("REAL", "Single-precision float"),
                ("CHAR(n)", "Fixed-length string"),
                ("VARCHAR(n)", "Variable-length string"),
                ("VARCHAR(MAX)", "Large variable-length string"),
                ("TEXT", "Large text (deprecated)"),
                ("NCHAR(n)", "Fixed-length Unicode string"),
                ("NVARCHAR(n)", "Variable-length Unicode string"),
                ("NVARCHAR(MAX)", "Large Unicode string"),
                ("NTEXT", "Large Unicode text (deprecated)"),
                ("BINARY(n)", "Fixed-length binary"),
                ("VARBINARY(n)", "Variable-length binary"),
                ("VARBINARY(MAX)", "Large binary"),
                ("IMAGE", "Large binary (deprecated)"),
                ("DATE", "Date only"),
                ("TIME", "Time only"),
                ("DATETIME", "Date and time (legacy)"),
                ("DATETIME2", "High precision datetime"),
                ("SMALLDATETIME", "Low precision datetime"),
                ("DATETIMEOFFSET", "Datetime with timezone"),
                ("TIMESTAMP", "Row version number"),
                ("UNIQUEIDENTIFIER", "GUID"),
                ("XML", "XML document"),
                ("SQL_VARIANT", "Variable type"),
                ("GEOGRAPHY", "Spatial geography data"),
                ("GEOMETRY", "Spatial geometry data"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id INT IDENTITY(1,1) PRIMARY KEY,\n  $2\n)", "Create table"),
                ("idx", "CREATE INDEX $1 ON $2 ($3)", "Create index"),
                ("alt", "ALTER TABLE $1 ADD $2", "Add column"),
                ("jn", "JOIN $1 ON $2.$3 = $4.$5", "Join clause"),
                ("lj", "LEFT JOIN $1 ON $2.$3 = $4.$5", "Left join clause"),
                ("sp", "CREATE PROCEDURE $1\nAS\nBEGIN\n  $2\nEND", "Create stored procedure"),
                ("try", "BEGIN TRY\n  $1\nEND TRY\nBEGIN CATCH\n  SELECT ERROR_MESSAGE()\nEND CATCH", "Try-catch block"),
            ],
        }.with_standard_sql()
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        info!("[MSSQL Plugin] Creating connection to {}:{}", config.host, config.port);
        let mut conn = MssqlDbConnection::new(config);
        conn.connect().await?;
        info!("[MSSQL Plugin] Connection created successfully");
        Ok(Box::new(conn))
    }

    // === Database/Schema Level Operations ===

    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>> {
        info!("[MSSQL Plugin] Listing databases...");
        let result = connection.query(
            "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name",
            None,
            ExecOptions::default()
        ).await.map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let databases: Vec<String> = query_result.rows.iter()
                .filter_map(|row| row.first().and_then(|v| v.clone()))
                .collect();
            info!("[MSSQL Plugin] Found {} databases", databases.len());
            Ok(databases)
        } else {
            Err(anyhow::anyhow!("Unexpected result type"))
        }
    }

    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView> {
        use gpui::px;

        let sql = r#"
            SELECT
                d.name,
                SUSER_SNAME(d.owner_sid) as owner,
                d.create_date,
                d.compatibility_level,
                d.collation_name
            FROM sys.databases d
            WHERE d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
            ORDER BY d.name
        "#;

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(3).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(4).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("owner", "Owner").width(px(120.0)),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("compat_level", "Compat Level").width(px(100.0)),
            Column::new("collation", "Collation").width(px(200.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Database,
            title: "Databases".to_string(),
        })
    }

    async fn list_tables(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            r#"
            SELECT
                t.TABLE_NAME,
                t.TABLE_SCHEMA
            FROM [{database}].INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TableInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    schema: row.get(1).and_then(|v| v.clone()),
                    comment: None,
                    engine: None,
                    row_count: None,
                    create_time: None,
                    charset: None,
                    collation: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                t.TABLE_NAME,
                t.TABLE_TYPE,
                CAST(ep.value AS NVARCHAR(MAX)) as table_comment
            FROM [{database}].INFORMATION_SCHEMA.TABLES t
            LEFT JOIN [{database}].sys.extended_properties ep
                ON ep.major_id = OBJECT_ID('[{database}].[' + t.TABLE_SCHEMA + '].[' + t.TABLE_NAME + ']')
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("TABLE".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("type", "Type").width(px(100.0)),
            Column::new("comment", "Comment").width(px(300.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Table,
            title: "Tables".to_string(),
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            r#"
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                COLUMNPROPERTY(OBJECT_ID('[{database}].[' + c.TABLE_SCHEMA + '].[{table}]'), c.COLUMN_NAME, 'IsIdentity') as is_identity,
                CAST(ep.value AS NVARCHAR(MAX)) as column_comment,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_primary_key
            FROM [{database}].INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN [{database}].sys.extended_properties ep
                ON ep.major_id = OBJECT_ID('[{database}].[' + c.TABLE_SCHEMA + '].[{table}]')
                AND ep.minor_id = c.ORDINAL_POSITION
                AND ep.name = 'MS_Description'
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM [{database}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN [{database}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = '{table}'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_NAME = '{table}'
            ORDER BY c.ORDINAL_POSITION
            "#,
            database = database.replace("]", "]]"),
            table = table.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                let is_nullable = row.get(2).and_then(|v| v.clone()).unwrap_or("YES".to_string()) == "YES";
                let is_primary_key = row.get(6).and_then(|v| v.clone()).map(|v| v == "1").unwrap_or(false);
                ColumnInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    data_type: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    is_nullable,
                    is_primary_key,
                    default_value: row.get(3).and_then(|v| v.clone()),
                    comment: row.get(5).and_then(|v| v.clone()),
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_columns_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns_data = self.list_columns(connection, database, table).await?;

        let rows: Vec<Vec<String>> = columns_data.iter().map(|col| {
            vec![
                col.name.clone(),
                col.data_type.clone(),
                if col.is_nullable { "YES" } else { "NO" }.to_string(),
                col.default_value.as_deref().unwrap_or("-").to_string(),
                col.comment.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("type", "Type").width(px(120.0)),
            Column::new("nullable", "Null").width(px(60.0)),
            Column::new("default", "Default").width(px(120.0)),
            Column::new("comment", "Comment").width(px(250.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Column,
            title: format!("Columns - {}", table),
        })
    }

    async fn list_indexes(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            r#"
            SELECT
                i.name as index_name,
                COL_NAME(ic.object_id, ic.column_id) as column_name,
                i.type_desc as index_type,
                i.is_unique
            FROM [{database}].sys.indexes i
            INNER JOIN [{database}].sys.index_columns ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            WHERE i.object_id = OBJECT_ID('[{database}]..[{table}]')
                AND i.type > 0
            ORDER BY i.name, ic.key_ordinal
            "#,
            database = database.replace("]", "]]"),
            table = table.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list indexes: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            let mut indexes: HashMap<String, IndexInfo> = HashMap::new();

            for row in &query_result.rows {
                let index_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
                let column_name = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
                let index_type = row.get(2).and_then(|v| v.clone()).unwrap_or_default();
                let is_unique = row.get(3).and_then(|v| v.clone()).unwrap_or("0".to_string()) == "1";

                indexes.entry(index_name.clone())
                    .or_insert_with(|| IndexInfo {
                        name: index_name.clone(),
                        columns: vec![],
                        is_unique,
                        index_type: Some(index_type),
                    })
                    .columns.push(column_name);
            }

            Ok(indexes.into_values().collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_views_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                v.TABLE_NAME,
                CAST(ep.value AS NVARCHAR(MAX)) as view_comment
            FROM [{database}].INFORMATION_SCHEMA.VIEWS v
            LEFT JOIN [{database}].sys.extended_properties ep
                ON ep.major_id = OBJECT_ID('[{database}].[' + v.TABLE_SCHEMA + '].[' + v.TABLE_NAME + ']')
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
            ORDER BY v.TABLE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list views: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("comment", "Comment").width(px(400.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::View,
            title: "Views".to_string(),
        })
    }

    async fn list_functions_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                r.ROUTINE_NAME,
                r.ROUTINE_TYPE,
                r.DATA_TYPE
            FROM [{database}].INFORMATION_SCHEMA.ROUTINES r
            WHERE r.ROUTINE_TYPE = 'FUNCTION'
            ORDER BY r.ROUTINE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list functions: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("type", "Type").width(px(120.0)),
            Column::new("return_type", "Return Type").width(px(150.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Function,
            title: "Functions".to_string(),
        })
    }

    async fn list_procedures_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                r.ROUTINE_NAME,
                r.CREATED,
                r.LAST_ALTERED
            FROM [{database}].INFORMATION_SCHEMA.ROUTINES r
            WHERE r.ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY r.ROUTINE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list procedures: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("modified", "Modified").width(px(180.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Procedure,
            title: "Stored Procedures".to_string(),
        })
    }

    async fn list_triggers_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                tr.name as trigger_name,
                OBJECT_NAME(tr.parent_id) as table_name,
                tr.is_disabled
            FROM [{database}].sys.triggers tr
            WHERE tr.parent_class = 1
            ORDER BY tr.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).map(|v| if v == "0" { "Enabled" } else { "Disabled" }.to_string()).unwrap_or("Unknown".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("table", "Table").width(px(200.0)),
            Column::new("status", "Status").width(px(100.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Trigger,
            title: "Triggers".to_string(),
        })
    }

    async fn list_sequences_view(&self, connection: &dyn DbConnection, database: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                s.name,
                TYPE_NAME(s.user_type_id) as data_type,
                CAST(s.start_value AS VARCHAR) as start_value,
                CAST(s.increment AS VARCHAR) as increment,
                CAST(s.current_value AS VARCHAR) as current_value
            FROM [{database}].sys.sequences s
            ORDER BY s.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list sequences: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(3).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(4).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("type", "Type").width(px(100.0)),
            Column::new("start", "Start").width(px(100.0)),
            Column::new("increment", "Increment").width(px(100.0)),
            Column::new("current", "Current").width(px(100.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Sequence,
            title: "Sequences".to_string(),
        })
    }

    // === Missing trait methods ===

    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        let sql = r#"
            SELECT
                d.name,
                SUSER_SNAME(d.owner_sid) as owner,
                d.create_date,
                d.collation_name,
                COUNT(t.name) as table_count
            FROM sys.databases d
            LEFT JOIN sys.tables t ON d.database_id = DB_ID(d.name)
            WHERE d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
            GROUP BY d.name, d.owner_sid, d.create_date, d.collation_name
            ORDER BY d.name
        "#;

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list databases: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                DatabaseInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    charset: None,
                    collation: row.get(3).and_then(|v| v.clone()),
                    size: None,
                    table_count: row.get(4).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    comment: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_indexes_view(&self, connection: &dyn DbConnection, database: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let indexes = self.list_indexes(connection, database, table).await?;

        let rows: Vec<Vec<String>> = indexes.iter().map(|idx| {
            vec![
                idx.name.clone(),
                idx.columns.join(", "),
                idx.index_type.as_deref().unwrap_or("-").to_string(),
                if idx.is_unique { "Yes" } else { "No" }.to_string(),
            ]
        }).collect();

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("columns", "Columns").width(px(250.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("unique", "Unique").width(px(80.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Index,
            title: format!("Indexes - {}", table),
        })
    }

    async fn list_views(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<ViewInfo>> {
        let sql = format!(
            r#"
            SELECT
                v.TABLE_NAME,
                v.TABLE_SCHEMA
            FROM [{database}].INFORMATION_SCHEMA.VIEWS v
            ORDER BY v.TABLE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list views: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ViewInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    schema: row.get(1).and_then(|v| v.clone()),
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_functions(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            r#"
            SELECT
                r.ROUTINE_NAME,
                r.ROUTINE_SCHEMA,
                r.DATA_TYPE
            FROM [{database}].INFORMATION_SCHEMA.ROUTINES r
            WHERE r.ROUTINE_TYPE = 'FUNCTION'
            ORDER BY r.ROUTINE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list functions: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: row.get(2).and_then(|v| v.clone()),
                    parameters: vec![],
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_procedures(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            r#"
            SELECT
                r.ROUTINE_NAME,
                r.ROUTINE_SCHEMA
            FROM [{database}].INFORMATION_SCHEMA.ROUTINES r
            WHERE r.ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY r.ROUTINE_NAME
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list procedures: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                FunctionInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    return_type: None,
                    parameters: vec![],
                    definition: None,
                    comment: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_triggers(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<TriggerInfo>> {
        let sql = format!(
            r#"
            SELECT
                tr.name as trigger_name,
                OBJECT_NAME(tr.parent_id) as table_name,
                tr.is_disabled
            FROM [{database}].sys.triggers tr
            WHERE tr.parent_class = 1
            ORDER BY tr.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TriggerInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    table_name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    event: "UNKNOWN".to_string(),
                    timing: "UNKNOWN".to_string(),
                    definition: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_sequences(&self, connection: &dyn DbConnection, database: &str) -> Result<Vec<SequenceInfo>> {
        let sql = format!(
            r#"
            SELECT
                s.name,
                TYPE_NAME(s.user_type_id) as data_type,
                CAST(s.start_value AS VARCHAR) as start_value,
                CAST(s.increment AS VARCHAR) as increment,
                CAST(s.current_value AS VARCHAR) as current_value
            FROM [{database}].sys.sequences s
            ORDER BY s.name
            "#,
            database = database.replace("]", "]]")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list sequences: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                SequenceInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    start_value: row.get(2).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    increment: row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    min_value: None,
                    max_value: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    fn build_create_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        let collation = request.field_values.get("collation").map(|s| s.as_str());

        let mut sql = format!("CREATE DATABASE [{}]", db_name.replace("]", "]]"));
        if let Some(coll) = collation {
            sql.push_str(&format!(" COLLATE {}", coll));
        }
        sql.push(';');
        sql
    }

    fn build_modify_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let db_name = &request.database_name;
        let collation = request.field_values.get("collation").map(|s| s.as_str());

        let mut sql = format!("ALTER DATABASE [{}]", db_name.replace("]", "]]"));
        if let Some(coll) = collation {
            sql.push_str(&format!(" COLLATE {}", coll));
        }
        sql.push(';');
        sql
    }

    fn build_drop_database_sql(&self, database_name: &str) -> String {
        format!("DROP DATABASE [{}];", database_name.replace("]", "]]"))
    }

    fn build_create_schema_sql(&self, schema_name: &str) -> String {
        format!("CREATE SCHEMA [{}];", schema_name.replace("]", "]]"))
    }

    fn build_drop_schema_sql(&self, schema_name: &str) -> String {
        format!("DROP SCHEMA [{}];", schema_name.replace("]", "]]"))
    }

    fn build_comment_schema_sql(&self, schema_name: &str, comment: &str) -> Option<String> {
        Some(format!(
            "EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'{}', @level0type=N'SCHEMA', @level0name=N'{}';",
            comment.replace("'", "''"),
            schema_name.replace("'", "''")
        ))
    }
}
