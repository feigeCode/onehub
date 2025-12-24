use std::collections::HashMap;

use anyhow::Result;
use gpui_component::table::Column;
use one_core::storage::{DatabaseType, DbConnectionConfig};

use crate::connection::{DbConnection, DbError};
use crate::executor::{ExecOptions, SqlResult};
use crate::oracle::connection::OracleDbConnection;
use crate::plugin::{DatabasePlugin, SqlCompletionInfo};
use crate::types::*;

pub struct OraclePlugin;

impl OraclePlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DatabasePlugin for OraclePlugin {
    fn name(&self) -> DatabaseType {
        DatabaseType::Oracle
    }

    fn identifier_quote(&self) -> &str {
        "\""
    }

    fn supports_sequences(&self) -> bool {
        true
    }

    fn get_completion_info(&self) -> SqlCompletionInfo {
        SqlCompletionInfo {
            keywords: vec![
                ("ROWNUM", "Row number pseudo-column"),
                ("ROWID", "Row identifier"),
                ("DUAL", "Dummy table for SELECT"),
                ("CONNECT BY", "Hierarchical query"),
                ("START WITH", "Hierarchical query start"),
                ("LEVEL", "Hierarchical query level"),
                ("PRIOR", "Parent row in hierarchy"),
                ("NOCYCLE", "Prevent cycles in hierarchy"),
                ("SIBLINGS", "Order siblings"),
                ("PIVOT", "Pivot rows to columns"),
                ("UNPIVOT", "Unpivot columns to rows"),
                ("MERGE", "Merge statement"),
                ("USING", "Merge source"),
                ("MATCHED", "When matched clause"),
                ("FLASHBACK", "Flashback query"),
                ("AS OF", "Point-in-time query"),
                ("VERSIONS", "Row versioning"),
                ("PARTITION BY", "Partition clause"),
                ("MODEL", "Model clause"),
                ("RETURNING", "Return clause"),
                ("BULK COLLECT", "Bulk collect into"),
                ("FORALL", "Bulk DML"),
                ("EXECUTE IMMEDIATE", "Dynamic SQL"),
                ("DBMS_OUTPUT", "Debug output"),
            ],
            functions: vec![
                ("NVL(expr, alt)", "Return alt if expr is NULL"),
                ("NVL2(expr, val1, val2)", "Return val1 if not NULL, else val2"),
                ("DECODE(expr, search, result, ...)", "Conditional expression"),
                ("TO_CHAR(expr, format)", "Convert to string"),
                ("TO_DATE(str, format)", "Convert to date"),
                ("TO_NUMBER(str)", "Convert to number"),
                ("TO_TIMESTAMP(str, format)", "Convert to timestamp"),
                ("TRUNC(date, fmt)", "Truncate date"),
                ("ADD_MONTHS(date, n)", "Add months to date"),
                ("MONTHS_BETWEEN(d1, d2)", "Months between dates"),
                ("LAST_DAY(date)", "Last day of month"),
                ("NEXT_DAY(date, day)", "Next occurrence of day"),
                ("EXTRACT(part FROM date)", "Extract date component"),
                ("SYSDATE", "Current date and time"),
                ("SYSTIMESTAMP", "Current timestamp with timezone"),
                ("CURRENT_DATE", "Session date"),
                ("CURRENT_TIMESTAMP", "Session timestamp"),
                ("INSTR(str, substr)", "Find substring position"),
                ("SUBSTR(str, pos, len)", "Extract substring"),
                ("REPLACE(str, from, to)", "Replace string"),
                ("TRANSLATE(str, from, to)", "Character translation"),
                ("INITCAP(str)", "Capitalize first letter"),
                ("LPAD(str, len, pad)", "Left pad string"),
                ("RPAD(str, len, pad)", "Right pad string"),
                ("REGEXP_LIKE(str, pattern)", "Regex match"),
                ("REGEXP_SUBSTR(str, pattern)", "Regex substring"),
                ("REGEXP_REPLACE(str, pattern, repl)", "Regex replace"),
                ("REGEXP_INSTR(str, pattern)", "Regex position"),
                ("LISTAGG(col, sep)", "Aggregate to list"),
                ("XMLAGG(xml)", "Aggregate XML"),
                ("XMLELEMENT(name, value)", "Create XML element"),
                ("JSON_VALUE(json, path)", "Extract JSON scalar"),
                ("JSON_QUERY(json, path)", "Extract JSON object"),
                ("JSON_TABLE(json, path)", "Parse JSON to table"),
                ("ROW_NUMBER() OVER(...)", "Row number window function"),
                ("RANK() OVER(...)", "Rank window function"),
                ("DENSE_RANK() OVER(...)", "Dense rank"),
                ("LEAD(col, offset) OVER(...)", "Next row value"),
                ("LAG(col, offset) OVER(...)", "Previous row value"),
                ("FIRST_VALUE(col) OVER(...)", "First value in window"),
                ("LAST_VALUE(col) OVER(...)", "Last value in window"),
                ("SYS_GUID()", "Generate GUID"),
                ("RAWTOHEX(raw)", "Convert raw to hex"),
                ("HEXTORAW(hex)", "Convert hex to raw"),
                ("USER", "Current user name"),
                ("SYS_CONTEXT(namespace, param)", "Get context value"),
            ],
            operators: vec![
                ("||", "String concatenation"),
                (":=", "Assignment (PL/SQL)"),
                ("=>", "Named parameter"),
                ("**", "Exponentiation"),
                ("..", "Range (PL/SQL)"),
            ],
            data_types: vec![
                ("NUMBER", "Numeric (default precision)"),
                ("NUMBER(p)", "Numeric with precision"),
                ("NUMBER(p,s)", "Numeric with precision and scale"),
                ("VARCHAR2(n)", "Variable-length string"),
                ("NVARCHAR2(n)", "Unicode variable-length string"),
                ("CHAR(n)", "Fixed-length string"),
                ("NCHAR(n)", "Unicode fixed-length string"),
                ("CLOB", "Character large object"),
                ("NCLOB", "Unicode character large object"),
                ("BLOB", "Binary large object"),
                ("BFILE", "External binary file"),
                ("DATE", "Date and time"),
                ("TIMESTAMP", "Timestamp"),
                ("TIMESTAMP WITH TIME ZONE", "Timestamp with timezone"),
                ("TIMESTAMP WITH LOCAL TIME ZONE", "Timestamp with local timezone"),
                ("INTERVAL YEAR TO MONTH", "Year-month interval"),
                ("INTERVAL DAY TO SECOND", "Day-second interval"),
                ("RAW(n)", "Raw binary data"),
                ("LONG RAW", "Long raw binary (deprecated)"),
                ("ROWID", "Row identifier"),
                ("UROWID", "Universal row identifier"),
                ("XMLTYPE", "XML data"),
                ("JSON", "JSON data (21c+)"),
                ("BOOLEAN", "Boolean (23c+)"),
                ("BINARY_FLOAT", "32-bit floating point"),
                ("BINARY_DOUBLE", "64-bit floating point"),
            ],
            snippets: vec![
                ("crt", "CREATE TABLE $1 (\n  id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n  $2\n)", "Create table with identity"),
                ("idx", "CREATE INDEX $1 ON $2 ($3)", "Create index"),
                ("seq", "CREATE SEQUENCE $1 START WITH 1 INCREMENT BY 1", "Create sequence"),
                ("pkg", "CREATE OR REPLACE PACKAGE $1 AS\n  $2\nEND $1;", "Create package"),
                ("proc", "CREATE OR REPLACE PROCEDURE $1 AS\nBEGIN\n  $2\nEND;", "Create procedure"),
                ("func", "CREATE OR REPLACE FUNCTION $1 RETURN $2 AS\nBEGIN\n  RETURN $3;\nEND;", "Create function"),
                ("trg", "CREATE OR REPLACE TRIGGER $1\nBEFORE INSERT ON $2\nFOR EACH ROW\nBEGIN\n  $3\nEND;", "Create trigger"),
            ],
        }.with_standard_sql()
    }

    async fn create_connection(&self, config: DbConnectionConfig) -> Result<Box<dyn DbConnection + Send + Sync>, DbError> {
        let mut conn = OracleDbConnection::new(config);
        conn.connect().await?;
        Ok(Box::new(conn))
    }

    async fn list_databases(&self, connection: &dyn DbConnection) -> Result<Vec<String>> {
        let result = connection.query(
            "SELECT username FROM all_users WHERE oracle_maintained = 'N' ORDER BY username",
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

    async fn list_databases_view(&self, connection: &dyn DbConnection) -> Result<ObjectView> {
        use gpui::px;

        let sql = r#"
            SELECT
                u.username,
                u.created,
                u.default_tablespace,
                u.temporary_tablespace,
                u.account_status
            FROM all_users u
            WHERE u.oracle_maintained = 'N'
            ORDER BY u.username
        "#;

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list schemas: {}", e))?;

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
            Column::new("name", "Schema").width(px(180.0)),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("tablespace", "Tablespace").width(px(150.0)),
            Column::new("temp_tablespace", "Temp Tablespace").width(px(150.0)),
            Column::new("status", "Status").width(px(100.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Database,
            title: "Schemas".to_string(),
        })
    }

    async fn list_databases_detailed(&self, connection: &dyn DbConnection) -> Result<Vec<DatabaseInfo>> {
        let sql = r#"
            SELECT
                u.username,
                u.default_tablespace,
                (SELECT COUNT(*) FROM all_tables t WHERE t.owner = u.username) as table_count
            FROM all_users u
            WHERE u.oracle_maintained = 'N'
            ORDER BY u.username
        "#;

        let result = connection.query(sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list schemas: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                DatabaseInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    charset: None,
                    collation: None,
                    size: None,
                    table_count: row.get(2).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    comment: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            r#"
            SELECT
                t.table_name,
                c.comments
            FROM all_tables t
            LEFT JOIN all_tab_comments c ON t.owner = c.owner AND t.table_name = c.table_name
            WHERE t.owner = '{}'
            ORDER BY t.table_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TableInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    schema: Some(schema.to_string()),
                    comment: row.get(1).and_then(|v| v.clone()),
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

    async fn list_tables_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                t.table_name,
                c.comments,
                t.num_rows,
                t.last_analyzed
            FROM all_tables t
            LEFT JOIN all_tab_comments c ON t.owner = c.owner AND t.table_name = c.table_name
            WHERE t.owner = '{}'
            ORDER BY t.table_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables: {}", e))?;

        let rows: Vec<Vec<String>> = if let SqlResult::Query(query_result) = result {
            query_result.rows.iter().map(|row| {
                vec![
                    row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    row.get(1).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(2).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(3).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("comment", "Comment").width(px(300.0)),
            Column::new("rows", "Rows").width(px(100.0)),
            Column::new("analyzed", "Last Analyzed").width(px(180.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Table,
            title: "Tables".to_string(),
        })
    }

    async fn list_columns(&self, connection: &dyn DbConnection, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            r#"
            SELECT
                c.column_name,
                c.data_type ||
                    CASE
                        WHEN c.data_type IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'RAW') THEN '(' || c.data_length || ')'
                        WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL THEN '(' || c.data_precision || ',' || NVL(c.data_scale, 0) || ')'
                        ELSE ''
                    END as data_type,
                c.nullable,
                c.data_default,
                (SELECT CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END
                 FROM all_cons_columns cc
                 JOIN all_constraints con ON cc.constraint_name = con.constraint_name AND cc.owner = con.owner
                 WHERE cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
                   AND con.constraint_type = 'P') as is_pk,
                cm.comments
            FROM all_tab_columns c
            LEFT JOIN all_col_comments cm ON c.owner = cm.owner AND c.table_name = cm.table_name AND c.column_name = cm.column_name
            WHERE c.owner = '{}' AND c.table_name = '{}'
            ORDER BY c.column_id
            "#,
            schema.replace("'", "''"),
            table.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                let is_nullable = row.get(2).and_then(|v| v.clone()).unwrap_or("Y".to_string()) == "Y";
                let is_pk = row.get(4).and_then(|v| v.clone()).unwrap_or("N".to_string()) == "Y";
                ColumnInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    data_type: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    is_nullable,
                    is_primary_key: is_pk,
                    default_value: row.get(3).and_then(|v| v.clone()),
                    comment: row.get(5).and_then(|v| v.clone()),
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_columns_view(&self, connection: &dyn DbConnection, schema: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let columns_data = self.list_columns(connection, schema, table).await?;

        let rows: Vec<Vec<String>> = columns_data.iter().map(|col| {
            vec![
                col.name.clone(),
                col.data_type.clone(),
                if col.is_nullable { "YES" } else { "NO" }.to_string(),
                if col.is_primary_key { "YES" } else { "NO" }.to_string(),
                col.default_value.as_deref().unwrap_or("-").to_string(),
                col.comment.as_deref().unwrap_or("-").to_string(),
            ]
        }).collect();

        let columns = vec![
            Column::new("name", "Name").width(px(180.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("nullable", "Nullable").width(px(60.0)),
            Column::new("pk", "PK").width(px(50.0)),
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

    async fn list_indexes(&self, connection: &dyn DbConnection, schema: &str, table: &str) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            r#"
            SELECT
                i.index_name,
                ic.column_name,
                i.index_type,
                i.uniqueness
            FROM all_indexes i
            JOIN all_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
            WHERE i.owner = '{}' AND i.table_name = '{}'
            ORDER BY i.index_name, ic.column_position
            "#,
            schema.replace("'", "''"),
            table.replace("'", "''")
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
                let is_unique = row.get(3).and_then(|v| v.clone()).unwrap_or("NONUNIQUE".to_string()) == "UNIQUE";

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

    async fn list_indexes_view(&self, connection: &dyn DbConnection, schema: &str, table: &str) -> Result<ObjectView> {
        use gpui::px;

        let indexes = self.list_indexes(connection, schema, table).await?;

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

    async fn list_views(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<ViewInfo>> {
        let sql = format!(
            r#"
            SELECT
                v.view_name,
                c.comments
            FROM all_views v
            LEFT JOIN all_tab_comments c ON v.owner = c.owner AND v.view_name = c.table_name
            WHERE v.owner = '{}'
            ORDER BY v.view_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list views: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                ViewInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    schema: Some(schema.to_string()),
                    definition: None,
                    comment: row.get(1).and_then(|v| v.clone()),
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_views_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                v.view_name,
                c.comments
            FROM all_views v
            LEFT JOIN all_tab_comments c ON v.owner = c.owner AND v.view_name = c.table_name
            WHERE v.owner = '{}'
            ORDER BY v.view_name
            "#,
            schema.replace("'", "''")
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

    async fn list_functions(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            r#"
            SELECT
                object_name,
                object_type
            FROM all_objects
            WHERE owner = '{}' AND object_type = 'FUNCTION'
            ORDER BY object_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list functions: {}", e))?;

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

    async fn list_functions_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                object_name,
                status,
                created,
                last_ddl_time
            FROM all_objects
            WHERE owner = '{}' AND object_type = 'FUNCTION'
            ORDER BY object_name
            "#,
            schema.replace("'", "''")
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
                    row.get(3).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("status", "Status").width(px(100.0)),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("modified", "Modified").width(px(180.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Function,
            title: "Functions".to_string(),
        })
    }

    async fn list_procedures(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            r#"
            SELECT
                object_name
            FROM all_objects
            WHERE owner = '{}' AND object_type = 'PROCEDURE'
            ORDER BY object_name
            "#,
            schema.replace("'", "''")
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

    async fn list_procedures_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                object_name,
                status,
                created,
                last_ddl_time
            FROM all_objects
            WHERE owner = '{}' AND object_type = 'PROCEDURE'
            ORDER BY object_name
            "#,
            schema.replace("'", "''")
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
                    row.get(3).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(250.0)),
            Column::new("status", "Status").width(px(100.0)),
            Column::new("created", "Created").width(px(180.0)),
            Column::new("modified", "Modified").width(px(180.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Procedure,
            title: "Procedures".to_string(),
        })
    }

    async fn list_triggers(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<TriggerInfo>> {
        let sql = format!(
            r#"
            SELECT
                trigger_name,
                table_name,
                triggering_event,
                trigger_type
            FROM all_triggers
            WHERE owner = '{}'
            ORDER BY trigger_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                TriggerInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    table_name: row.get(1).and_then(|v| v.clone()).unwrap_or_default(),
                    event: row.get(2).and_then(|v| v.clone()).unwrap_or_default(),
                    timing: row.get(3).and_then(|v| v.clone()).unwrap_or_default(),
                    definition: None,
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_triggers_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                trigger_name,
                table_name,
                triggering_event,
                trigger_type,
                status
            FROM all_triggers
            WHERE owner = '{}'
            ORDER BY trigger_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list triggers: {}", e))?;

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
            Column::new("table", "Table").width(px(150.0)),
            Column::new("event", "Event").width(px(150.0)),
            Column::new("type", "Type").width(px(150.0)),
            Column::new("status", "Status").width(px(100.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Trigger,
            title: "Triggers".to_string(),
        })
    }

    async fn list_sequences(&self, connection: &dyn DbConnection, schema: &str) -> Result<Vec<SequenceInfo>> {
        let sql = format!(
            r#"
            SELECT
                sequence_name,
                min_value,
                max_value,
                increment_by,
                last_number
            FROM all_sequences
            WHERE sequence_owner = '{}'
            ORDER BY sequence_name
            "#,
            schema.replace("'", "''")
        );

        let result = connection.query(&sql, None, ExecOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list sequences: {}", e))?;

        if let SqlResult::Query(query_result) = result {
            Ok(query_result.rows.iter().map(|row| {
                SequenceInfo {
                    name: row.get(0).and_then(|v| v.clone()).unwrap_or_default(),
                    start_value: row.get(4).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    increment: row.get(3).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    min_value: row.get(1).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                    max_value: row.get(2).and_then(|v| v.clone()).and_then(|s| s.parse().ok()),
                }
            }).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn list_sequences_view(&self, connection: &dyn DbConnection, schema: &str) -> Result<ObjectView> {
        use gpui::px;

        let sql = format!(
            r#"
            SELECT
                sequence_name,
                min_value,
                max_value,
                increment_by,
                last_number,
                cache_size,
                cycle_flag
            FROM all_sequences
            WHERE sequence_owner = '{}'
            ORDER BY sequence_name
            "#,
            schema.replace("'", "''")
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
                    row.get(5).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                    row.get(6).and_then(|v| v.clone()).unwrap_or("-".to_string()),
                ]
            }).collect()
        } else {
            vec![]
        };

        let columns = vec![
            Column::new("name", "Name").width(px(200.0)),
            Column::new("min", "Min").width(px(100.0)),
            Column::new("max", "Max").width(px(100.0)),
            Column::new("increment", "Increment").width(px(100.0)),
            Column::new("last", "Last Value").width(px(100.0)),
            Column::new("cache", "Cache").width(px(80.0)),
            Column::new("cycle", "Cycle").width(px(60.0)),
        ];

        Ok(ObjectView {
            columns,
            rows,
            db_node_type: DbNodeType::Sequence,
            title: "Sequences".to_string(),
        })
    }

    fn build_create_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let schema_name = &request.database_name;
        let password = request.field_values.get("password").map(|s| s.as_str()).unwrap_or("password");

        format!(
            "CREATE USER \"{}\" IDENTIFIED BY \"{}\";\nGRANT CONNECT, RESOURCE TO \"{}\";",
            schema_name.replace("\"", "\"\""),
            password.replace("\"", "\"\""),
            schema_name.replace("\"", "\"\"")
        )
    }

    fn build_modify_database_sql(&self, request: &crate::plugin::DatabaseOperationRequest) -> String {
        let schema_name = &request.database_name;
        let mut statements = Vec::new();

        if let Some(tablespace) = request.field_values.get("tablespace") {
            statements.push(format!(
                "ALTER USER \"{}\" DEFAULT TABLESPACE {}",
                schema_name.replace("\"", "\"\""),
                tablespace
            ));
        }

        if statements.is_empty() {
            format!("-- No modifications for schema \"{}\"", schema_name)
        } else {
            statements.join(";\n") + ";"
        }
    }

    fn build_drop_database_sql(&self, schema_name: &str) -> String {
        format!("DROP USER \"{}\" CASCADE;", schema_name.replace("\"", "\"\""))
    }
    fn rename_table(&self, _database: &str, old_name: &str, new_name: &str) -> String {
        format!(
            "ALTER TABLE {} RENAME TO {}",
            self.quote_identifier(old_name),
            self.quote_identifier(new_name)
        )
    }

    fn drop_view(&self, _database: &str, view: &str) -> String {
        format!("DROP VIEW {}", self.quote_identifier(view))
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
                    let col_name = self.quote_identifier(&col.name);
                    let type_str = self.build_type_string(col);

                    if orig_col.data_type != col.data_type || orig_col.length != col.length {
                        statements.push(format!(
                            "ALTER TABLE {} MODIFY {} {};",
                            table_name, col_name, type_str
                        ));
                    }

                    if orig_col.is_nullable != col.is_nullable {
                        if col.is_nullable {
                            statements.push(format!(
                                "ALTER TABLE {} MODIFY {} NULL;",
                                table_name, col_name
                            ));
                        } else {
                            statements.push(format!(
                                "ALTER TABLE {} MODIFY {} NOT NULL;",
                                table_name, col_name
                            ));
                        }
                    }

                    if orig_col.default_value != col.default_value {
                        if let Some(default) = &col.default_value {
                            statements.push(format!(
                                "ALTER TABLE {} MODIFY {} DEFAULT {};",
                                table_name, col_name, default
                            ));
                        } else {
                            statements.push(format!(
                                "ALTER TABLE {} MODIFY {} DEFAULT NULL;",
                                table_name, col_name
                            ));
                        }
                    }
                }
            } else {
                let col_def = self.build_column_def(col);
                statements.push(format!(
                    "ALTER TABLE {} ADD {};",
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

        for (name, idx) in &original_indexes {
            if !new_indexes.contains_key(name) {
                if idx.is_primary {
                    statements.push(format!(
                        "ALTER TABLE {} DROP PRIMARY KEY;",
                        table_name
                    ));
                } else {
                    statements.push(format!(
                        "DROP INDEX {};",
                        self.quote_identifier(name)
                    ));
                }
            }
        }

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
                    let unique_str = if idx.is_unique { "UNIQUE " } else { "" };
                    statements.push(format!(
                        "CREATE {}INDEX {} ON {} ({});",
                        unique_str,
                        self.quote_identifier(name),
                        table_name,
                        idx_cols.join(", ")
                    ));
                }
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

    fn create_plugin() -> OraclePlugin {
        OraclePlugin::new()
    }

    // ==================== Basic Plugin Info Tests ====================

    #[test]
    fn test_plugin_name() {
        let plugin = create_plugin();
        assert_eq!(plugin.name(), DatabaseType::Oracle);
    }

    #[test]
    fn test_identifier_quote() {
        let plugin = create_plugin();
        assert_eq!(plugin.identifier_quote(), "\"");
    }

    #[test]
    fn test_quote_identifier() {
        let plugin = create_plugin();
        assert_eq!(plugin.quote_identifier("table_name"), "\"table_name\"");
        assert_eq!(plugin.quote_identifier("column"), "\"column\"");
    }

    #[test]
    fn test_supports_sequences() {
        let plugin = create_plugin();
        assert!(plugin.supports_sequences());
    }

    #[test]
    fn test_supports_schema() {
        let plugin = create_plugin();
        assert!(!plugin.supports_schema());
    }

    // ==================== DDL SQL Generation Tests ====================

    #[test]
    fn test_drop_table() {
        let plugin = create_plugin();
        let sql = plugin.drop_table("test_schema", "users");
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn test_truncate_table() {
        let plugin = create_plugin();
        let sql = plugin.truncate_table("test_schema", "users");
        assert!(sql.contains("TRUNCATE TABLE"));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn test_rename_table() {
        let plugin = create_plugin();
        let sql = plugin.rename_table("test_schema", "old_name", "new_name");
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("RENAME TO"));
        assert!(sql.contains("\"old_name\""));
        assert!(sql.contains("\"new_name\""));
    }

    #[test]
    fn test_drop_view() {
        let plugin = create_plugin();
        let sql = plugin.drop_view("test_schema", "my_view");
        assert!(sql.contains("DROP VIEW"));
        assert!(sql.contains("\"my_view\""));
    }

    // ==================== Database/Schema Operations Tests ====================

    #[test]
    fn test_build_create_database_sql() {
        let plugin = create_plugin();
        let mut field_values = HashMap::new();
        field_values.insert("password".to_string(), "secret123".to_string());

        let request = crate::plugin::DatabaseOperationRequest {
            database_name: "new_schema".to_string(),
            field_values,
        };

        let sql = plugin.build_create_database_sql(&request);
        assert!(sql.contains("CREATE USER"));
        assert!(sql.contains("\"new_schema\""));
        assert!(sql.contains("IDENTIFIED BY"));
        assert!(sql.contains("GRANT CONNECT, RESOURCE"));
    }

    #[test]
    fn test_build_modify_database_sql_with_tablespace() {
        let plugin = create_plugin();
        let mut field_values = HashMap::new();
        field_values.insert("tablespace".to_string(), "USERS".to_string());

        let request = crate::plugin::DatabaseOperationRequest {
            database_name: "my_schema".to_string(),
            field_values,
        };

        let sql = plugin.build_modify_database_sql(&request);
        assert!(sql.contains("ALTER USER"));
        assert!(sql.contains("\"my_schema\""));
        assert!(sql.contains("DEFAULT TABLESPACE"));
    }

    #[test]
    fn test_build_modify_database_sql_no_changes() {
        let plugin = create_plugin();
        let field_values = HashMap::new();

        let request = crate::plugin::DatabaseOperationRequest {
            database_name: "my_schema".to_string(),
            field_values,
        };

        let sql = plugin.build_modify_database_sql(&request);
        assert!(sql.contains("--"));
    }

    #[test]
    fn test_build_drop_database_sql() {
        let plugin = create_plugin();
        let sql = plugin.build_drop_database_sql("old_schema");
        assert!(sql.contains("DROP USER"));
        assert!(sql.contains("\"old_schema\""));
        assert!(sql.contains("CASCADE"));
    }

    // ==================== Column Definition Tests ====================

    #[test]
    fn test_build_column_def_simple() {
        let plugin = create_plugin();
        let col = ColumnDefinition::new("id")
            .data_type("NUMBER")
            .nullable(false)
            .primary_key(true);

        let def = plugin.build_column_def(&col);
        assert!(def.contains("\"id\""));
        assert!(def.contains("NUMBER"));
        assert!(def.contains("NOT NULL"));
    }

    #[test]
    fn test_build_column_def_varchar2() {
        let plugin = create_plugin();
        let col = ColumnDefinition::new("name")
            .data_type("VARCHAR2")
            .length(100)
            .nullable(true);

        let def = plugin.build_column_def(&col);
        assert!(def.contains("\"name\""));
        assert!(def.contains("VARCHAR2(100)"));
        assert!(!def.contains("NOT NULL"));
    }

    #[test]
    fn test_build_column_def_number_with_precision() {
        let plugin = create_plugin();
        let mut col = ColumnDefinition::new("price")
            .data_type("NUMBER")
            .length(10)
            .nullable(false);
        col.scale = Some(2);

        let def = plugin.build_column_def(&col);
        assert!(def.contains("\"price\""));
        assert!(def.contains("NUMBER(10,2)"));
    }

    #[test]
    fn test_build_column_def_with_default() {
        let plugin = create_plugin();
        let mut col = ColumnDefinition::new("status")
            .data_type("NUMBER")
            .default_value("0");
        col.is_nullable = false;

        let def = plugin.build_column_def(&col);
        assert!(def.contains("DEFAULT 0"));
        assert!(def.contains("NOT NULL"));
    }

    // ==================== CREATE TABLE Tests ====================

    #[test]
    fn test_build_create_table_sql_simple() {
        let plugin = create_plugin();
        let design = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id")
                    .data_type("NUMBER")
                    .nullable(false)
                    .primary_key(true),
                ColumnDefinition::new("name")
                    .data_type("VARCHAR2")
                    .length(100),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_create_table_sql(&design);
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("NUMBER"));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("VARCHAR2(100)"));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_build_create_table_sql_with_indexes() {
        let plugin = create_plugin();
        let design = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDefinition::new("id")
                    .data_type("NUMBER")
                    .nullable(false)
                    .primary_key(true),
                ColumnDefinition::new("user_id")
                    .data_type("NUMBER")
                    .nullable(false),
                ColumnDefinition::new("email")
                    .data_type("VARCHAR2")
                    .length(100),
            ],
            indexes: vec![
                IndexDefinition::new("idx_user_id")
                    .columns(vec!["user_id".to_string()])
                    .unique(false),
                IndexDefinition::new("idx_email")
                    .columns(vec!["email".to_string()])
                    .unique(true),
            ],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_create_table_sql(&design);
        assert!(sql.contains("INDEX \"idx_user_id\""));
        assert!(sql.contains("UNIQUE INDEX \"idx_email\""));
    }

    #[test]
    fn test_build_create_table_sql_with_date_column() {
        let plugin = create_plugin();
        let design = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("id")
                    .data_type("NUMBER")
                    .nullable(false)
                    .primary_key(true),
                ColumnDefinition::new("created_at")
                    .data_type("DATE")
                    .nullable(false),
                ColumnDefinition::new("updated_at")
                    .data_type("TIMESTAMP")
                    .nullable(true),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_create_table_sql(&design);
        assert!(sql.contains("\"created_at\""));
        assert!(sql.contains("DATE"));
        assert!(sql.contains("\"updated_at\""));
        assert!(sql.contains("TIMESTAMP"));
    }

    // ==================== ALTER TABLE Tests ====================

    #[test]
    fn test_build_alter_table_sql_add_column() {
        let plugin = create_plugin();

        let original = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("NUMBER"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let new = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("NUMBER"),
                ColumnDefinition::new("email").data_type("VARCHAR2").length(100),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_alter_table_sql(&original, &new);
        assert!(sql.contains("ADD"));
        assert!(sql.contains("\"email\""));
    }

    #[test]
    fn test_build_alter_table_sql_drop_column() {
        let plugin = create_plugin();

        let original = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("NUMBER"),
                ColumnDefinition::new("old_column").data_type("VARCHAR2").length(50),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let new = TableDesign {
            database_name: "test_schema".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id").data_type("NUMBER"),
            ],
            indexes: vec![],
            foreign_keys: vec![],
            options: TableOptions::default(),
        };

        let sql = plugin.build_alter_table_sql(&original, &new);
        assert!(sql.contains("DROP COLUMN"));
        assert!(sql.contains("\"old_column\""));
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

        assert!(info.keywords.iter().any(|(k, _)| *k == "ROWNUM"));
        assert!(info.keywords.iter().any(|(k, _)| *k == "DUAL"));
        assert!(info.functions.iter().any(|(f, _)| f.starts_with("NVL(")));
        assert!(info.functions.iter().any(|(f, _)| f.starts_with("TO_CHAR")));
        assert!(info.data_types.iter().any(|(t, _)| *t == "VARCHAR2(n)"));
        assert!(info.data_types.iter().any(|(t, _)| *t == "NUMBER"));
    }

    #[test]
    fn test_completion_info_has_oracle_specific_types() {
        let plugin = create_plugin();
        let info = plugin.get_completion_info();

        assert!(info.data_types.iter().any(|(t, _)| *t == "CLOB"));
        assert!(info.data_types.iter().any(|(t, _)| *t == "BLOB"));
        assert!(info.data_types.iter().any(|(t, _)| *t == "XMLTYPE"));
        assert!(info.data_types.iter().any(|(t, _)| t.contains("TIMESTAMP")));
    }

    #[test]
    fn test_completion_info_has_oracle_specific_functions() {
        let plugin = create_plugin();
        let info = plugin.get_completion_info();

        assert!(info.functions.iter().any(|(f, _)| f.starts_with("DECODE")));
        assert!(info.functions.iter().any(|(f, _)| f.starts_with("LISTAGG")));
        assert!(info.functions.iter().any(|(f, _)| f.starts_with("SYS_GUID")));
    }
}
