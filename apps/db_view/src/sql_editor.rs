use std::rc::Rc;
use std::str::FromStr;

use anyhow::Result;
use db::plugin::SqlCompletionInfo;
use gpui::{App, AppContext, Context, Entity, IntoElement, Render, SharedString, Styled as _, Subscription, Task, Window};
use gpui_component::highlighter::Language;
use gpui_component::input::{
    CodeActionProvider, CompletionProvider, HoverProvider, Input, InputEvent, InputState, TabSize,
};
use gpui_component::{Rope, RopeExt};
use lsp_types::{
    CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, CompletionTextEdit,
    Hover, HoverContents, InsertReplaceEdit, MarkedString, Range as LspRange, TextEdit, Uri,
    WorkspaceEdit,
};

use db::sql_editor::sql_context_inferrer::{ContextInferrer, SqlContext as InferredSqlContext};
use sum_tree::Bias;
use db::sql_editor::sql_symbol_table::SymbolTable;
use db::sql_editor::sql_tokenizer::SqlTokenizer;

/// Simple schema hints to improve autocomplete suggestions.
#[derive(Clone, Default)]
pub struct SqlSchema {
    pub tables: Vec<(String, String)>,   // (name, doc)
    pub columns: Vec<(String, String)>,  // global (name, doc)
    pub columns_by_table: std::collections::HashMap<String, Vec<(String, String)>>,
}

impl SqlSchema {
    pub fn with_tables(
        mut self,
        tables: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.tables = tables.into_iter().map(|(n, d)| (n.into(), d.into())).collect();
        self
    }
    pub fn with_columns(
        mut self,
        columns: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.columns = columns.into_iter().map(|(n, d)| (n.into(), d.into())).collect();
        self
    }
    pub fn with_table_columns(
        mut self,
        table: impl Into<String>,
        columns: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.columns_by_table.insert(
            table.into(),
            columns
                .into_iter()
                .map(|(n, d)| (n.into(), d.into()))
                .collect(),
        );
        self
    }
}

/// SQL context for smarter completion suggestions
#[derive(Debug, Clone, PartialEq)]
pub enum SqlContext {
    /// Start of statement or unknown context
    Start,
    /// After SELECT keyword, expecting columns
    SelectColumns,
    /// After FROM/JOIN/INTO/UPDATE, expecting table name
    TableName,
    /// After WHERE/AND/OR/ON, expecting condition
    Condition,
    /// After ORDER BY/GROUP BY, expecting column
    OrderBy,
    /// After SET (in UPDATE), expecting column = value
    SetClause,
    /// After VALUES, expecting values
    Values,
    /// After CREATE TABLE, expecting table definition
    CreateTable,
    /// After dot (table.column), expecting column name
    DotColumn(String),
    /// After function name with open paren
    FunctionArgs,
}

/// Priority scores for context-aware completion sorting.
/// Lower scores appear first in completion list (higher priority).
/// 
/// Default priority order (without context):
/// 1. Keywords (1000-1999)
/// 2. Tables (2000-2999)
/// 3. Columns (3000-3999)
/// 4. Functions (4000-4999)
/// 5. Snippets (5000+)
///
/// In specific contexts, relevant items get boosted to appear before keywords.
pub mod completion_priority {
    // Base priorities by item type (lower = higher priority)
    pub const KEYWORDS_BASE: i32 = 1000;
    pub const TABLES_BASE: i32 = 2000;
    pub const COLUMNS_BASE: i32 = 3000;
    pub const SNIPPETS_BASE: i32 = 4000;
    pub const OPERATORS_BASE: i32 = 4500;
    pub const FUNCTIONS_BASE: i32 = 5000;

    // Context boost (subtract from base to increase priority)
    // Large boost to make context-relevant items appear before keywords
    pub const CONTEXT_BOOST: i32 = 2500;
    pub const PREFIX_MATCH_BOOST: i32 = 200;

    use super::SqlContext;
    use lsp_types::CompletionItemKind;

    /// Calculate priority score for a completion item based on context.
    /// Lower scores appear first (higher priority).
    pub fn calculate_score(
        context: &SqlContext,
        item_kind: Option<CompletionItemKind>,
        matches_prefix: bool,
    ) -> i32 {
        // Determine base score by item type
        let base_score = match item_kind {
            Some(CompletionItemKind::KEYWORD) => KEYWORDS_BASE,
            Some(CompletionItemKind::STRUCT) => TABLES_BASE,
            Some(CompletionItemKind::FIELD) => COLUMNS_BASE,
            Some(CompletionItemKind::FUNCTION) => FUNCTIONS_BASE,
            Some(CompletionItemKind::OPERATOR) => OPERATORS_BASE,
            Some(CompletionItemKind::SNIPPET) => SNIPPETS_BASE,
            _ => COLUMNS_BASE, // Default to columns priority
        };

        // Apply context boost for relevant items
        let context_boost = match (context, item_kind) {
            // DotColumn: columns get boost
            (SqlContext::DotColumn(_), Some(CompletionItemKind::FIELD)) => CONTEXT_BOOST,
            
            // TableName: tables get boost
            (SqlContext::TableName, Some(CompletionItemKind::STRUCT)) => CONTEXT_BOOST,
            
            // SelectColumns: columns get boost
            (SqlContext::SelectColumns, Some(CompletionItemKind::FIELD)) => CONTEXT_BOOST,
            
            // Condition/OrderBy/SetClause: columns get boost
            (SqlContext::Condition | SqlContext::OrderBy | SqlContext::SetClause, Some(CompletionItemKind::FIELD)) => CONTEXT_BOOST,
            
            // FunctionArgs: columns get boost
            (SqlContext::FunctionArgs, Some(CompletionItemKind::FIELD)) => CONTEXT_BOOST,
            
            _ => 0,
        };

        // Apply prefix match boost
        let prefix_boost = if matches_prefix { PREFIX_MATCH_BOOST } else { 0 };

        // Lower score = higher priority
        base_score - context_boost - prefix_boost
    }

    /// Convert score to sort_text format.
    /// Lower scores appear first (higher priority).
    /// Format: "{score:05}_{label}" for stable sorting.
    pub fn score_to_sort_text(score: i32, label: &str) -> String {
        // Lower score = higher priority, so use score directly
        format!("{:05}_{}", score.max(0).min(99999), label)
    }
}

// Built-in SQL keywords and docs
const SQL_KEYWORDS: &[(&str, &str)] = &[
    ("SELECT", "Query rows from table(s)"),
    ("INSERT", "Insert new rows"),
    ("UPDATE", "Update existing rows"),
    ("DELETE", "Delete rows"),
    ("CREATE", "Create database object"),
    ("ALTER", "Modify database object"),
    ("DROP", "Remove database object"),
    ("TRUNCATE", "Remove all rows from table"),
    ("FROM", "Specify source table(s)"),
    ("WHERE", "Filter rows with predicates"),
    ("JOIN", "Combine rows from tables"),
    ("INNER JOIN", "Inner join tables"),
    ("LEFT JOIN", "Left outer join"),
    ("RIGHT JOIN", "Right outer join"),
    ("FULL JOIN", "Full outer join"),
    ("CROSS JOIN", "Cross product of tables"),
    ("ON", "Join condition"),
    ("USING", "Join using common columns"),
    ("GROUP BY", "Group rows for aggregation"),
    ("HAVING", "Filter grouped rows"),
    ("ORDER BY", "Sort result set"),
    ("ASC", "Ascending order"),
    ("DESC", "Descending order"),
    ("LIMIT", "Limit number of rows"),
    ("OFFSET", "Skip rows"),
    ("VALUES", "Specify values for INSERT"),
    ("INTO", "Target table for INSERT"),
    ("SET", "Set column values for UPDATE"),
    ("AND", "Logical AND"),
    ("OR", "Logical OR"),
    ("NOT", "Logical NOT"),
    ("IN", "Value in list"),
    ("EXISTS", "Subquery returns rows"),
    ("BETWEEN", "Value in range"),
    ("LIKE", "Pattern matching"),
    ("IS NULL", "Check for NULL"),
    ("IS NOT NULL", "Check for non-NULL"),
    ("AS", "Alias"),
    ("DISTINCT", "Remove duplicates"),
    ("ALL", "Include all rows"),
    ("UNION", "Combine result sets"),
    ("UNION ALL", "Combine without dedup"),
    ("INTERSECT", "Common rows"),
    ("EXCEPT", "Difference of sets"),
    ("CASE", "Conditional expression"),
    ("WHEN", "Condition in CASE"),
    ("THEN", "Result in CASE"),
    ("ELSE", "Default in CASE"),
    ("END", "End CASE expression"),
    ("WITH", "Common table expression"),
    ("TABLE", "Table keyword"),
    ("INDEX", "Index keyword"),
    ("VIEW", "View keyword"),
    ("PRIMARY KEY", "Primary key constraint"),
    ("FOREIGN KEY", "Foreign key constraint"),
    ("REFERENCES", "Reference constraint"),
    ("UNIQUE", "Unique constraint"),
    ("CHECK", "Check constraint"),
    ("DEFAULT", "Default value"),
    ("NOT NULL", "Not null constraint"),
    ("NULL", "NULL value"),
    ("TRUE", "Boolean true"),
    ("FALSE", "Boolean false"),
];

const SQL_FUNCTIONS: &[(&str, &str)] = &[
    ("COUNT(*)", "Count all rows"),
    ("COUNT(col)", "Count non-NULL values"),
    ("SUM(col)", "Sum of values"),
    ("AVG(col)", "Average value"),
    ("MIN(col)", "Minimum value"),
    ("MAX(col)", "Maximum value"),
    ("COALESCE(val1, val2, ...)", "First non-NULL value"),
    ("NULLIF(val1, val2)", "NULL if values equal"),
    ("CAST(expr AS type)", "Type conversion"),
    ("UPPER(str)", "Convert to uppercase"),
    ("LOWER(str)", "Convert to lowercase"),
    ("TRIM(str)", "Remove whitespace"),
    ("LENGTH(str)", "String length"),
    ("SUBSTRING(str, pos, len)", "Extract substring"),
    ("CONCAT(str1, str2)", "Concatenate strings"),
    ("REPLACE(str, from, to)", "Replace substring"),
    ("ABS(x)", "Absolute value"),
    ("ROUND(x, d)", "Round number"),
    ("FLOOR(x)", "Round down"),
    ("CEIL(x)", "Round up"),
    ("NOW()", "Current timestamp"),
    ("CURRENT_DATE", "Current date"),
    ("CURRENT_TIME", "Current time"),
];

#[derive(Clone)]
pub struct DefaultSqlCompletionProvider {
    schema: SqlSchema,
    db_completion_info: Option<SqlCompletionInfo>,
}

impl DefaultSqlCompletionProvider {
    pub fn new(schema: SqlSchema) -> Self {
        Self { schema, db_completion_info: None }
    }

    pub fn with_db_completion_info(mut self, info: SqlCompletionInfo) -> Self {
        self.db_completion_info = Some(info);
        self
    }

    /// Parse SQL text and return both context and symbol table.
    ///
    /// This method is used when we need the symbol table for DotColumn filtering.
    fn parse_context_with_symbols(text: &str, offset: usize) -> (SqlContext, SymbolTable) {
        let mut tokenizer = SqlTokenizer::new(text);
        let tokens = tokenizer.tokenize();
        let symbol_table = SymbolTable::build_from_tokens(&tokens);
        let inferred = ContextInferrer::infer(&tokens, offset, &symbol_table);
        (Self::convert_context(inferred), symbol_table)
    }

    /// Convert InferredSqlContext to local SqlContext enum.
    fn convert_context(inferred: InferredSqlContext) -> SqlContext {
        match inferred {
            InferredSqlContext::Start => SqlContext::Start,
            InferredSqlContext::SelectColumns => SqlContext::SelectColumns,
            InferredSqlContext::TableName => SqlContext::TableName,
            InferredSqlContext::Condition => SqlContext::Condition,
            InferredSqlContext::OrderBy => SqlContext::OrderBy,
            InferredSqlContext::SetClause => SqlContext::SetClause,
            InferredSqlContext::Values => SqlContext::Values,
            InferredSqlContext::CreateTable => SqlContext::CreateTable,
            InferredSqlContext::DotColumn(alias) => SqlContext::DotColumn(alias),
            InferredSqlContext::FunctionArgs => SqlContext::FunctionArgs,
        }
    }
}

impl CompletionProvider for DefaultSqlCompletionProvider {
    fn completions(
        &self,
        rope: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        cx: &mut Context<InputState>,
    ) -> Task<Result<CompletionResponse>> { let rope = rope.clone();
        let schema = self.schema.clone();
        let db_info = self.db_completion_info.clone();

        cx.background_spawn(async move {
            let text = rope.to_string();

            // Check if inside a comment (-- style)
            let before_cursor = &text[..offset.min(text.len())];
            let last_newline = before_cursor.rfind('\n').map(|p| p + 1).unwrap_or(0);
            let current_line = &before_cursor[last_newline..];
            if current_line.contains("--") {
                return Ok(CompletionResponse::Array(vec![]));
            }

            // Use tokenizer-based context parsing with symbol table
            let (context, symbol_table) = Self::parse_context_with_symbols(&text, offset);

            // Current word - find word start by scanning backwards from offset
            // Use clip_offset to ensure we're on a char boundary
            let mut start_offset = rope.clip_offset(offset, Bias::Left);
            while start_offset > 0 {
                let prev_offset = rope.clip_offset(start_offset.saturating_sub(1), Bias::Left);
                if prev_offset >= start_offset {
                    break;
                }
                let ch = rope.char(prev_offset);
                if !(ch.is_alphanumeric() || ch == '_') {
                    break;
                }
                start_offset = prev_offset;
            }
            let current_word = rope.slice(start_offset..offset).to_string().to_uppercase();

            let start_pos = rope.offset_to_position(start_offset);
            let end_pos = rope.offset_to_position(offset);
            let replace_range = LspRange::new(start_pos, end_pos);

            let mut items = Vec::new();

            let matches_filter = |label: &str| -> bool {
                current_word.is_empty() || label.to_uppercase().starts_with(&current_word)
            };

            let matched_prefix = |label: &str| -> String {
                let lu = label.to_uppercase();
                if !current_word.is_empty() && lu.starts_with(&current_word) {
                    label.chars().take(current_word.chars().count()).collect()
                } else {
                    String::new()
                }
            };

            // Handle dot context (table.column) - highest priority
            // Uses SymbolTable to resolve alias to actual table name
            if let SqlContext::DotColumn(alias_or_table) = &context {
                // Resolve alias to table name using symbol table
                // If alias is found, use the resolved table name; otherwise use as-is
                let resolved_table = symbol_table
                    .resolve(alias_or_table)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| alias_or_table.clone());

                // Try to find columns for the resolved table
                // First try exact match, then case-insensitive match
                let columns = schema.columns_by_table.get(&resolved_table)
                    .or_else(|| {
                        // Case-insensitive lookup
                        let lower = resolved_table.to_lowercase();
                        schema.columns_by_table.iter()
                            .find(|(k, _)| k.to_lowercase() == lower)
                            .map(|(_, v)| v)
                    });

                if let Some(cols) = columns {
                    for (column, doc) in cols {
                        if matches_filter(column) {
                            let matches_prefix = !current_word.is_empty()
                                && column.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::FIELD),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: column.clone(),
                                kind: Some(CompletionItemKind::FIELD),
                                detail: Some(format!("{}.column", resolved_table)),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: column.clone(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(column)),
                                documentation: Some(lsp_types::Documentation::String(doc.clone())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, column)),
                                ..Default::default()
                            });
                        }
                    }
                }
                // Sort by score and truncate (Requirement 5.6: limit to 50 items)
                items.sort_by(|a, b| {
                    a.sort_text.as_ref().unwrap_or(&a.label)
                        .cmp(b.sort_text.as_ref().unwrap_or(&b.label))
                });
                items.truncate(50);
                return Ok(CompletionResponse::Array(items));
            }

            // Context-aware completion priorities
            // (show_tables, show_columns, show_keywords, show_functions, show_types)
            let (show_tables, show_columns, show_keywords, show_functions, show_types) = match context {
                SqlContext::TableName => (true, false, false, false, false),
                SqlContext::SelectColumns => (false, true, true, true, false), // Allow keywords like FROM, AS, DISTINCT
                SqlContext::OrderBy | SqlContext::SetClause => (false, true, true, true, false),
                SqlContext::Condition => (false, true, true, true, false),
                SqlContext::FunctionArgs => (false, true, false, true, false),
                SqlContext::CreateTable => (false, false, true, false, true),
                SqlContext::Values => (false, false, false, true, false),
                SqlContext::Start => (true, true, true, false, false), // Don't show functions at start
                SqlContext::DotColumn(_) => (false, true, false, false, false), // Only show columns for table.column
            };

            // Tables - priority based on context (Requirement 5.2)
            if show_tables {
                for (table, doc) in &schema.tables {
                    if matches_filter(table) {
                        let matches_prefix = !current_word.is_empty()
                            && table.to_uppercase().starts_with(&current_word);
                        let score = completion_priority::calculate_score(
                            &context,
                            Some(CompletionItemKind::STRUCT),
                            matches_prefix,
                        );
                        items.push(CompletionItem {
                            label: table.clone(),
                            kind: Some(CompletionItemKind::STRUCT),
                            detail: Some("Table".to_string()),
                            text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                InsertReplaceEdit {
                                    new_text: table.clone(),
                                    insert: replace_range.clone(),
                                    replace: replace_range.clone(),
                                },
                            )),
                            filter_text: Some(matched_prefix(table)),
                            documentation: Some(lsp_types::Documentation::String(doc.clone())),
                            sort_text: Some(completion_priority::score_to_sort_text(score, table)),
                            ..Default::default()
                        });
                    }
                }
            }

            // Columns - priority based on context (Requirements 5.3, 5.4)
            if show_columns {
                // In contexts where we have table information (SelectColumns, Condition, OrderBy, SetClause),
                // show columns from tables in FROM/JOIN clauses
                let use_table_columns = matches!(
                    context,
                    SqlContext::SelectColumns | SqlContext::Condition | SqlContext::OrderBy | SqlContext::SetClause
                );

                if use_table_columns {
                    // Get all tables from symbol table
                    let tables: Vec<String> = symbol_table
                        .all_aliases()
                        .map(|(_, table)| table.to_string())
                        .collect();

                    // Deduplicate tables (in case of multiple aliases for same table)
                    let mut seen_tables = std::collections::HashSet::new();
                    for table in tables {
                        if seen_tables.insert(table.to_lowercase()) {
                            // Try to find columns for this table
                            let columns = schema.columns_by_table.get(&table)
                                .or_else(|| {
                                    // Case-insensitive lookup
                                    let lower = table.to_lowercase();
                                    schema.columns_by_table.iter()
                                        .find(|(k, _)| k.to_lowercase() == lower)
                                        .map(|(_, v)| v)
                                });

                            if let Some(cols) = columns {
                                for (column, doc) in cols {
                                    if matches_filter(column) {
                                        let matches_prefix = !current_word.is_empty()
                                            && column.to_uppercase().starts_with(&current_word);
                                        let score = completion_priority::calculate_score(
                                            &context,
                                            Some(CompletionItemKind::FIELD),
                                            matches_prefix,
                                        );
                                        items.push(CompletionItem {
                                            label: column.clone(),
                                            kind: Some(CompletionItemKind::FIELD),
                                            detail: Some(format!("{}.column", table)),
                                            text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                                InsertReplaceEdit {
                                                    new_text: column.clone(),
                                                    insert: replace_range.clone(),
                                                    replace: replace_range.clone(),
                                                },
                                            )),
                                            filter_text: Some(matched_prefix(column)),
                                            documentation: Some(lsp_types::Documentation::String(doc.clone())),
                                            sort_text: Some(completion_priority::score_to_sort_text(score, column)),
                                            ..Default::default()
                                        });
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // For other contexts (FunctionArgs, Start), show global columns
                    for (column, doc) in &schema.columns {
                        if matches_filter(column) {
                            let matches_prefix = !current_word.is_empty()
                                && column.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::FIELD),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: column.clone(),
                                kind: Some(CompletionItemKind::FIELD),
                                detail: Some("Column".to_string()),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: column.clone(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(column)),
                                documentation: Some(lsp_types::Documentation::String(doc.clone())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, column)),
                                ..Default::default()
                            });
                        }
                    }
                }
            }

            // Keywords - lower priority than context-specific items
            if show_keywords {
                // Standard SQL keywords
                for (keyword, doc) in SQL_KEYWORDS {
                    if matches_filter(keyword) {
                        let matches_prefix = !current_word.is_empty()
                            && keyword.to_uppercase().starts_with(&current_word);
                        let score = completion_priority::calculate_score(
                            &context,
                            Some(CompletionItemKind::KEYWORD),
                            matches_prefix,
                        );
                        items.push(CompletionItem {
                            label: keyword.to_string(),
                            kind: Some(CompletionItemKind::KEYWORD),
                            text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                InsertReplaceEdit {
                                    new_text: keyword.to_string(),
                                    insert: replace_range.clone(),
                                    replace: replace_range.clone(),
                                },
                            )),
                            filter_text: Some(matched_prefix(keyword)),
                            documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                            sort_text: Some(completion_priority::score_to_sort_text(score, keyword)),
                            ..Default::default()
                        });
                    }
                }
                // Database-specific keywords
                if let Some(ref info) = db_info {
                    for (keyword, doc) in &info.keywords {
                        if matches_filter(keyword) {
                            let matches_prefix = !current_word.is_empty()
                                && keyword.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::KEYWORD),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: keyword.to_string(),
                                kind: Some(CompletionItemKind::KEYWORD),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: keyword.to_string(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(keyword)),
                                documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, keyword)),
                                ..Default::default()
                            });
                        }
                    }
                    // Database-specific operators - higher priority in Condition context
                    for (op, doc) in &info.operators {
                        if matches_filter(op) {
                            let matches_prefix = !current_word.is_empty()
                                && op.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::OPERATOR),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: op.to_string(),
                                kind: Some(CompletionItemKind::OPERATOR),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: op.to_string(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(op)),
                                documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, op)),
                                ..Default::default()
                            });
                        }
                    }
                }
            }

            // Functions - priority based on context (Requirement 5.3)
            if show_functions {
                // Standard SQL functions
                for (func, doc) in SQL_FUNCTIONS {
                    let func_name = func.split('(').next().unwrap_or("");
                    if matches_filter(func_name) {
                        let matches_prefix = !current_word.is_empty()
                            && func_name.to_uppercase().starts_with(&current_word);
                        let score = completion_priority::calculate_score(
                            &context,
                            Some(CompletionItemKind::FUNCTION),
                            matches_prefix,
                        );
                        items.push(CompletionItem {
                            label: func.to_string(),
                            kind: Some(CompletionItemKind::FUNCTION),
                            text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                InsertReplaceEdit {
                                    new_text: func.to_string(),
                                    insert: replace_range.clone(),
                                    replace: replace_range.clone(),
                                },
                            )),
                            filter_text: Some(matched_prefix(func_name)),
                            documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                            sort_text: Some(completion_priority::score_to_sort_text(score, func)),
                            ..Default::default()
                        });
                    }
                }
                // Database-specific functions
                if let Some(ref info) = db_info {
                    for (func, doc) in &info.functions {
                        let func_name = func.split('(').next().unwrap_or("");
                        if matches_filter(func_name) {
                            let matches_prefix = !current_word.is_empty()
                                && func_name.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::FUNCTION),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: func.to_string(),
                                kind: Some(CompletionItemKind::FUNCTION),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: func.to_string(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(func_name)),
                                documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, func)),
                                ..Default::default()
                            });
                        }
                    }
                }
            }

            // Data types - priority based on context
            if show_types {
                if let Some(ref info) = db_info {
                    for (dtype, doc) in &info.data_types {
                        if matches_filter(dtype) {
                            let matches_prefix = !current_word.is_empty()
                                && dtype.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::TYPE_PARAMETER),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: dtype.to_string(),
                                kind: Some(CompletionItemKind::TYPE_PARAMETER),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: dtype.to_string(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                filter_text: Some(matched_prefix(dtype)),
                                documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, dtype)),
                                ..Default::default()
                            });
                        }
                    }
                }
            }

            // Snippets - lowest priority (only at start)
            if matches!(context, SqlContext::Start) {
                // Default snippets
                let default_snippets: &[(&str, &str, &str)] = &[
                    ("sel*", "SELECT * FROM $1 WHERE $2", "Select all columns"),
                    ("selc", "SELECT COUNT(*) FROM $1 WHERE $2", "Count rows"),
                    ("ins", "INSERT INTO $1 ($2) VALUES ($3)", "Insert row"),
                    ("upd", "UPDATE $1 SET $2 WHERE $3", "Update rows"),
                    ("del", "DELETE FROM $1 WHERE $2", "Delete rows"),
                ];
                for (label, insert_text, doc) in default_snippets {
                    if matches_filter(label) {
                        let matches_prefix = !current_word.is_empty()
                            && label.to_uppercase().starts_with(&current_word);
                        let score = completion_priority::calculate_score(
                            &context,
                            Some(CompletionItemKind::SNIPPET),
                            matches_prefix,
                        );
                        items.push(CompletionItem {
                            label: label.to_string(),
                            kind: Some(CompletionItemKind::SNIPPET),
                            text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                InsertReplaceEdit {
                                    new_text: insert_text.to_string(),
                                    insert: replace_range.clone(),
                                    replace: replace_range.clone(),
                                },
                            )),
                            insert_text_format: Some(lsp_types::InsertTextFormat::SNIPPET),
                            filter_text: Some(matched_prefix(label)),
                            documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                            sort_text: Some(completion_priority::score_to_sort_text(score, label)),
                            ..Default::default()
                        });
                    }
                }
                // Database-specific snippets
                if let Some(ref info) = db_info {
                    for (label, insert_text, doc) in &info.snippets {
                        if matches_filter(label) {
                            let matches_prefix = !current_word.is_empty()
                                && label.to_uppercase().starts_with(&current_word);
                            let score = completion_priority::calculate_score(
                                &context,
                                Some(CompletionItemKind::SNIPPET),
                                matches_prefix,
                            );
                            items.push(CompletionItem {
                                label: label.to_string(),
                                kind: Some(CompletionItemKind::SNIPPET),
                                text_edit: Some(CompletionTextEdit::InsertAndReplace(
                                    InsertReplaceEdit {
                                        new_text: insert_text.to_string(),
                                        insert: replace_range.clone(),
                                        replace: replace_range.clone(),
                                    },
                                )),
                                insert_text_format: Some(lsp_types::InsertTextFormat::SNIPPET),
                                filter_text: Some(matched_prefix(label)),
                                documentation: Some(lsp_types::Documentation::String(doc.to_string())),
                                sort_text: Some(completion_priority::score_to_sort_text(score, label)),
                                ..Default::default()
                            });
                        }
                    }
                }
            }

            items.sort_by(|a, b| {
                a.sort_text
                    .as_ref()
                    .unwrap_or(&a.label)
                    .cmp(b.sort_text.as_ref().unwrap_or(&b.label))
            });
            items.truncate(50);
            Ok(CompletionResponse::Array(items))
        })
    }

    fn is_completion_trigger(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<InputState>,
    ) -> bool {
        if new_text.ends_with(";") {
            return false;
        }
        true
    }
}

#[derive(Clone)]
pub struct DefaultSqlHoverProvider {
    db_completion_info: Option<SqlCompletionInfo>,
}

impl DefaultSqlHoverProvider {
    pub fn new() -> Self {
        Self { db_completion_info: None }
    }

    pub fn with_db_completion_info(mut self, info: SqlCompletionInfo) -> Self {
        self.db_completion_info = Some(info);
        self
    }
}

impl HoverProvider for DefaultSqlHoverProvider {
    fn hover(
        &self,
        text: &Rope,
        offset: usize,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Task<Result<Option<Hover>>> {
        let word = text.word_at(offset).to_uppercase();

        // Check standard SQL keywords
        for (keyword, doc) in SQL_KEYWORDS {
            if *keyword == word.as_str() {
                let hover = Hover {
                    contents: HoverContents::Scalar(MarkedString::String(format!(
                        "**{}**\n\n{}",
                        keyword, doc
                    ))),
                    range: None,
                };
                return Task::ready(Ok(Some(hover)));
            }
        }

        // Check standard SQL functions
        for (func, doc) in SQL_FUNCTIONS {
            let func_name = func.split('(').next().unwrap_or("");
            if func_name == word.as_str() {
                let hover = Hover {
                    contents: HoverContents::Scalar(MarkedString::String(format!(
                        "**{}**\n\n{}",
                        func, doc
                    ))),
                    range: None,
                };
                return Task::ready(Ok(Some(hover)));
            }
        }

        // Check database-specific keywords and functions
        if let Some(ref info) = self.db_completion_info {
            for (keyword, doc) in &info.keywords {
                if *keyword == word.as_str() {
                    let hover = Hover {
                        contents: HoverContents::Scalar(MarkedString::String(format!(
                            "**{}**\n\n{}",
                            keyword, doc
                        ))),
                        range: None,
                    };
                    return Task::ready(Ok(Some(hover)));
                }
            }
            for (func, doc) in &info.functions {
                let func_name = func.split('(').next().unwrap_or("");
                if func_name == word.as_str() {
                    let hover = Hover {
                        contents: HoverContents::Scalar(MarkedString::String(format!(
                            "**{}**\n\n{}",
                            func, doc
                        ))),
                        range: None,
                    };
                    return Task::ready(Ok(Some(hover)));
                }
            }
            for (op, doc) in &info.operators {
                if *op == word.as_str() {
                    let hover = Hover {
                        contents: HoverContents::Scalar(MarkedString::String(format!(
                            "**{}**\n\n{}",
                            op, doc
                        ))),
                        range: None,
                    };
                    return Task::ready(Ok(Some(hover)));
                }
            }
            for (dtype, doc) in &info.data_types {
                let type_name = dtype.split('(').next().unwrap_or(dtype);
                if type_name == word.as_str() {
                    let hover = Hover {
                        contents: HoverContents::Scalar(MarkedString::String(format!(
                            "**{}**\n\n{}",
                            dtype, doc
                        ))),
                        range: None,
                    };
                    return Task::ready(Ok(Some(hover)));
                }
            }
        }

        Task::ready(Ok(None))
    }
}

#[derive(Clone)]
struct SqlActionsProvider {
    /// Callback for executing SQL.
    on_execute: Option<Rc<dyn Fn(String, &mut Window, &mut App) + 'static>>,
}

impl SqlActionsProvider {
    fn new() -> Self {
        Self { on_execute: None }
    }
    #[allow(dead_code)]
    fn with_execute(
        mut self,
        f: Rc<dyn Fn(String, &mut Window, &mut App) + 'static>,
    ) -> Self {
        self.on_execute = Some(f);
        self
    }

    fn format_sql(sql: &str) -> String {
        let mut formatted = String::new();
        let mut indent_level = 0;
        let lines: Vec<&str> = sql.lines().collect();
        for line in lines {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with("FROM")
                || trimmed.starts_with("WHERE")
                || trimmed.starts_with("JOIN")
                || trimmed.starts_with("INNER")
                || trimmed.starts_with("LEFT")
                || trimmed.starts_with("RIGHT")
                || trimmed.starts_with("ORDER BY")
                || trimmed.starts_with("GROUP BY")
                || trimmed.starts_with("HAVING")
                || trimmed.starts_with("LIMIT")
            {
                indent_level = 0;
            }
            formatted.push_str(&"  ".repeat(indent_level));
            formatted.push_str(trimmed);
            formatted.push('\n');
            if trimmed.starts_with("SELECT") {
                indent_level = 1;
            }
        }
        formatted.trim_end().to_string()
    }

    fn minify_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn uppercase_keywords(sql: &str) -> String {
        let mut result = String::new();
        let mut current_word = String::new();
        let mut in_string = false;
        let mut string_char = ' ';
        for ch in sql.chars() {
            if (ch == '\'' || ch == '"') && !in_string {
                if !current_word.is_empty() {
                    result.push_str(&Self::uppercase_if_keyword(&current_word));
                    current_word.clear();
                }
                in_string = true;
                string_char = ch;
                result.push(ch);
                continue;
            } else if in_string && ch == string_char {
                in_string = false;
                result.push(ch);
                continue;
            }
            if in_string {
                result.push(ch);
                continue;
            }
            if ch.is_alphanumeric() || ch == '_' {
                current_word.push(ch);
            } else {
                if !current_word.is_empty() {
                    result.push_str(&Self::uppercase_if_keyword(&current_word));
                    current_word.clear();
                }
                result.push(ch);
            }
        }
        if !current_word.is_empty() {
            result.push_str(&Self::uppercase_if_keyword(&current_word));
        }
        result
    }

    fn uppercase_if_keyword(word: &str) -> String {
        let upper = word.to_uppercase();
        if SQL_KEYWORDS.iter().any(|(kw, _)| *kw == upper.as_str()) {
            upper
        } else {
            word.to_string()
        }
    }
}

impl CodeActionProvider for SqlActionsProvider {
    fn id(&self) -> SharedString {
        "SqlActionsProvider".into()
    }

    fn code_actions(
        &self,
        state: Entity<InputState>,
        range: std::ops::Range<usize>,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Task<Result<Vec<lsp_types::CodeAction>>> {
        let state_read = state.read(_cx);
        let document_uri = Uri::from_str("file://one-hub").unwrap();
        let mut actions = vec![];

        if !range.is_empty() {
            let old_text = state_read.text().slice(range.clone()).to_string();
            let start = state_read.text().offset_to_position(range.start);
            let end = state_read.text().offset_to_position(range.end);
            let lsp_range = lsp_types::Range { start, end };

            // Uppercase
            let new_text = Self::uppercase_keywords(&old_text);
            actions.push(lsp_types::CodeAction {
                title: "Uppercase Keywords".into(),
                kind: Some(lsp_types::CodeActionKind::REFACTOR),
                edit: Some(WorkspaceEdit {
                    changes: Some(
                        std::iter::once((
                            document_uri.clone(),
                            vec![TextEdit { range: lsp_range.clone(), new_text }],
                        ))
                        .collect(),
                    ),
                    document_changes: None,
                    change_annotations: None,
                }),
                ..Default::default()
            });

            // Minify
            let new_text = Self::minify_sql(&old_text);
            actions.push(lsp_types::CodeAction {
                title: "Minify SQL".into(),
                kind: Some(lsp_types::CodeActionKind::REFACTOR),
                edit: Some(WorkspaceEdit {
                    changes: Some(
                        std::iter::once((
                            document_uri.clone(),
                            vec![TextEdit { range: lsp_range.clone(), new_text }],
                        ))
                        .collect(),
                    ),
                    document_changes: None,
                    change_annotations: None,
                }),
                ..Default::default()
            });
        }

        // Format whole document
        let old_text = state_read.text().to_string();
        let new_text = Self::format_sql(&old_text);
        let start = state_read.text().offset_to_position(0);
        let end = state_read.text().offset_to_position(state_read.text().len());
        let lsp_range = lsp_types::Range { start, end };
        actions.push(lsp_types::CodeAction {
            title: "Format SQL".into(),
            kind: Some(lsp_types::CodeActionKind::REFACTOR),
            edit: Some(WorkspaceEdit {
                changes: Some(
                    std::iter::once((
                        document_uri.clone(),
                        vec![TextEdit { range: lsp_range, new_text }],
                    ))
                    .collect(),
                ),
                document_changes: None,
                change_annotations: None,
            }),
            ..Default::default()
        });

        Task::ready(Ok(actions))
    }

    fn perform_code_action(
        &self,
        state: Entity<InputState>,
        action: lsp_types::CodeAction,
        _push_to_history: bool,
        window: &mut Window,
        cx: &mut App,
    ) -> Task<Result<()>> {
        let _ = (state, action, window, cx);
        Task::ready(Ok(()))
    }
}

/// A reusable SQL editor component built on top of `Input`.
pub struct SqlEditor {
    editor: Entity<InputState>,
    _subscriptions: Vec<Subscription>,
}

impl SqlEditor {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editor = cx.new(|cx| {
            let mut editor = InputState::new(window, cx)
                .code_editor(Language::from_str("sql"))
                .line_number(true)
                .searchable(true)
                .indent_guides(true)
                .tab_size(TabSize { tab_size: 2, hard_tabs: false })
                .soft_wrap(false)
                .placeholder("Enter your SQL query here...");

            // Defaults: completion + hover + actions
            let default_schema = SqlSchema::default();
            editor.lsp.completion_provider =
                Some(Rc::new(DefaultSqlCompletionProvider::new(default_schema)));
            editor.lsp.hover_provider = Some(Rc::new(DefaultSqlHoverProvider::new()));

            editor
        });

        let _subscriptions = vec![cx.subscribe_in(
            &editor,
            window,
            move |_, _, _: &InputEvent, _window, cx| cx.notify(),
        )];

        // Provide default text utilities as code actions (format/minify/uppercase)
        editor.update(cx, |state, _| {
            state.lsp.code_action_providers.push(Rc::new(SqlActionsProvider::new()));
        });

        Self { editor, _subscriptions }
    }

    /// Set database-specific completion information from plugin
    pub fn set_db_completion_info(
        &mut self,
        info: SqlCompletionInfo,
        schema: SqlSchema,
        cx: &mut Context<Self>,
    ) {
        let completion_provider = DefaultSqlCompletionProvider::new(schema)
            .with_db_completion_info(info.clone());
        let hover_provider = DefaultSqlHoverProvider::new()
            .with_db_completion_info(info);

        self.editor.update(cx, |state, _| {
            state.lsp.completion_provider = Some(Rc::new(completion_provider));
            state.lsp.hover_provider = Some(Rc::new(hover_provider));
        });
    }

    /// Access underlying editor state.
    pub fn input(&self) -> Entity<InputState> {
        self.editor.clone()
    }

    /// Replace default completion provider.
    pub fn set_completion_provider(
        &mut self,
        provider: Rc<dyn CompletionProvider>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor
            .update(cx, |state, _| state.lsp.completion_provider = Some(provider));
    }

    /// Set schema for default completion provider.
    pub fn set_schema(
        &mut self,
        schema: SqlSchema,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |state, _| {
            state.lsp.completion_provider = Some(Rc::new(DefaultSqlCompletionProvider::new(
                schema,
            )));
        });
    }

    /// Replace hover provider.
    pub fn set_hover_provider(
        &mut self,
        provider: Rc<dyn HoverProvider>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor
            .update(cx, |state, _| state.lsp.hover_provider = Some(provider));
    }

    /// Add a custom code action provider.
    pub fn add_code_action_provider(
        &mut self,
        provider: Rc<dyn CodeActionProvider>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor
            .update(cx, |state, _| state.lsp.code_action_providers.push(provider));
    }

    /// Convenient toggles for consumers
    pub fn set_line_number(&mut self, on: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.editor
            .update(cx, |s, cx| s.set_line_number(on, window, cx));
    }
    pub fn set_soft_wrap(&mut self, on: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |s, cx| s.set_soft_wrap(on, window, cx));
    }
    pub fn set_indent_guides(&mut self, on: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.editor
            .update(cx, |s, cx| s.set_indent_guides(on, window, cx));
    }
    pub fn set_value(&mut self, text: String, window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |s, cx| s.set_value(text, window, cx));
    }

    /// Get the current text content of the editor.
    /// This is a convenience method that accesses the underlying InputState.
    pub fn get_text<T>(&self, cx: &Context<T>) -> String {
        use std::ops::Deref;
        self.editor.read(cx.deref()).text().to_string()
    }

    /// Get the current text content using App context.
    pub fn get_text_from_app(&self, cx: &App) -> String {
        self.editor.read(cx).text().to_string()
    }
}

impl Render for SqlEditor {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        Input::new(&self.editor).size_full()
    }
}
