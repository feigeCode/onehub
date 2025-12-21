use one_core::storage::traits::Repository;
use one_core::gpui_tokio::Tokio;
use crate::sql_editor::SqlEditor;
use crate::sql_result_tab::SqlResultTabContainer;
use one_core::tab_container::{TabContent, TabContentType};
use db::{GlobalDbState};
use gpui::{px, AnyElement, App, AppContext, AsyncApp, ClickEvent, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, WeakEntity, Window};
use gpui::prelude::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::resizable::{resizable_panel, v_resizable};
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::{h_flex, v_flex, ActiveTheme, Icon, IconName, IndexPath, Sizable, Size, WindowExt};
use std::any::Any;
use tracing::log::error;
use one_core::storage::GlobalStorageState;
use one_core::storage::query_repository::QueryRepository;

// Events emitted by SqlEditorTabContent
#[derive(Debug, Clone)]
pub enum SqlEditorEvent {
    /// Query was saved successfully
    QuerySaved { connection_id: String, database: Option<String> },
}

pub struct SqlEditorTab {
    title: SharedString,
    editor: Entity<SqlEditor>,
    connection_id: String,
    // Multiple result tabs
    sql_result_tab_container: Entity<SqlResultTabContainer> ,
    database_select: Entity<SelectState<SearchableVec<String>>>,
    // Add focus handle
    focus_handle: FocusHandle,
}

impl SqlEditorTab {
    pub fn new_with_config(
        title: impl Into<SharedString>,
        connection_id: impl Into<String>,
        query_id: Option<i64>,
        initial_database: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let editor = cx.new(|cx| SqlEditor::new(window, cx));
        let focus_handle = cx.focus_handle();
        // Create database select with empty items initially
        let database_select = cx.new(|cx| {
            SelectState::new(SearchableVec::new(vec![]), None, window, cx)
        });

        let instance = Self {
            title: title.into(),
            editor: editor.clone(),
            connection_id: connection_id.into(),
            sql_result_tab_container: cx.new(|cx| SqlResultTabContainer::new(window, cx)),
            database_select: database_select.clone(),
            focus_handle,
        };

        // Bind select event
        instance.bind_select_event(cx);

        // Load databases in background
        instance.load_databases_async(initial_database, query_id, cx, window);

        instance
    }

    fn bind_select_event(&self, cx: &mut App){
        let this = self.clone();
        cx.subscribe(&self.database_select, move |_select, event, cx| {
           let global_state = cx.global::<GlobalDbState>().clone();
            if let SelectEvent::Confirm(Some(db_name)) = event {
                let db = db_name.clone();
                let instance = this.clone();
                cx.spawn(async move |cx| {
                    instance.update_schema_for_db(global_state, &db, cx).await;
                }).detach();
            }
        }).detach();
    }

    pub fn set_sql(&self, sql: String, window: &mut Window, cx: &mut App) {
        self.editor.update(cx, |e, cx| e.set_value(sql, window, cx));
    }



    /// Load databases into the select dropdown
    fn load_databases_async(&self, init_db: Option<String>, query_id: Option<i64>, cx: &mut App, window: &mut Window) {
        let _ = window;
        let global_state = cx.global::<GlobalDbState>().clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();
        let connection_id = self.connection_id.clone();
        let database_select = self.database_select.clone();
        let editor = self.editor.clone();
        let initial_database = init_db.clone();
        let instance = self.clone();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let databases = match global_state.list_databases(cx, connection_id.clone()).await {
                Ok(result) => result,
                Err(e) => {
                    error!("Failed to load databases for {}: {}", connection_id, e);
                    Self::notify_async(cx, format!("Failed to load databases: {}", e));
                    return;
                }
            };

            let mut sql_content = None;
            let mut resolved_database = initial_database.clone();

            if let Some(query_id) = query_id {
                let storage = storage_manager.clone();
                match Tokio::spawn_result(cx, async move {
                    let storage = storage.clone();
                    let query_repo = storage.get::<QueryRepository>().await
                        .ok_or_else(|| anyhow::anyhow!("Query repository not found"))?;
                    query_repo.get(query_id).await
                }) {
                    Ok(task) => match task.await {
                        Ok(Some(query)) => {
                            sql_content = Some(query.content);
                            if let Some(db_name) = query.database_name {
                                resolved_database = Some(db_name);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            error!("Failed to get query {}: {}", query_id, e);
                            Self::notify_async(cx, format!("Failed to load saved query: {}", e));
                        }
                    },
                    Err(e) => {
                        error!("Failed to enqueue query load: {}", e);
                        Self::notify_async(cx, format!("Failed to load saved query: {}", e));
                    }
                }
            }
            
            let selected_name = resolved_database.clone().or_else(|| databases.get(0).cloned());

           cx.update(|cx| {
                if let Some(window_id) = cx.active_window() {
                    cx.update_window(window_id, |_entity, window, cx| {
                        database_select.update(cx, |state, cx| {
                            if databases.is_empty() {
                                let items = SearchableVec::new(vec!["No databases available".to_string()]);
                                state.set_items(items, window, cx);
                                state.set_selected_index(None, window, cx);
                            } else {
                                let items = SearchableVec::new(databases.clone());
                                state.set_items(items, window, cx);
                                if let Some(name) = selected_name.as_ref() {
                                    if let Some(index) = databases.iter().position(|d| d == name) {
                                        state.set_selected_index(Some(IndexPath::new(index)), window, cx);
                                    }
                                }
                            }
                        });
                        if let Some(sql) = sql_content {
                            editor.update(cx, |e, cx| {
                                e.set_value(sql.clone(), window, cx);
                            });
                        }
                    })
                } else {
                    Err(anyhow::anyhow!("No active window"))
                }
            }).ok();

            if let Some(ref db) = resolved_database {
                instance.update_schema_for_db(global_state, db, cx).await;
            }
        }).detach();
    }

    /// Update SQL editor schema with tables and columns from current database
    pub async fn update_schema_for_db(&self, global_state: GlobalDbState, database: &str, cx: &mut AsyncApp) {
        use crate::sql_editor::SqlSchema;

        let connection_id = self.connection_id.clone();
        let editor = self.editor.clone();
        let db = database.to_string();

        let tables = match global_state.list_tables(cx, connection_id.clone(), db.clone()).await {
            Ok(result) => result,
            Err(e) => {
                eprintln!("Failed to get tables: {}", e);
                return;
            }
        };

        // Get database-specific completion info
        let db_completion_info = match global_state.get_completion_info(cx, connection_id.clone()).await {
            Ok(info) => info,
            Err(e) => {
                eprintln!("Failed to get completion info: {}", e);
                return;
            }
        };

        let mut schema = SqlSchema::default();

        // Add tables to schema
        let table_items: Vec<(String, String)> = tables.iter()
            .map(|t| {
                let description = if let Some(comment) = &t.comment {
                    format!("Table: {} - {}", t.name, comment)
                } else {
                    format!("Table: {}", t.name)
                };
                (t.name.clone(), description)
            })
            .collect();
        schema = schema.with_tables(table_items);

        // Load columns for each table
        for table in &tables {
            if let Ok(columns) = global_state.list_columns(cx, connection_id.clone(), db.clone(), table.name.clone()).await {
                let column_items: Vec<(String, String)> = columns.iter()
                    .map(|c| (c.name.clone(), format!("{} - {}", c.data_type,
                                                      c.comment.as_ref().unwrap_or(&String::new()))))
                    .collect();
                schema = schema.with_table_columns(&table.name, column_items);
            }
        }

        // Update editor with schema and database-specific completion info
        _ = editor.update(cx, |e, cx| {
            e.set_db_completion_info(db_completion_info, schema, cx);
        });
    }

    fn get_sql_text(&self, cx: &App) -> String {
        self.editor.read(cx).get_text_from_app(cx)
    }

    fn notify_async(cx: &mut AsyncApp, message: String) {
        let _ = cx.update(|cx| {
            if let Some(window_id) = cx.active_window() {
                let notification = message.clone();
                cx.update_window(window_id, move |_entity, window, cx| {
                    window.push_notification(notification.clone(), cx);
                })
            } else {
                Err(anyhow::anyhow!("No active window"))
            }
        });
    }

    fn handle_run_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let selected_text = self.editor.read(cx).get_selected_text_from_app(cx);
        let sql = if selected_text.trim().is_empty() {
            self.get_sql_text(cx)
        } else {
            selected_text
        };

        let connection_id = self.connection_id.clone();
        let sql_result_tab_container = self.sql_result_tab_container.clone();

        let current_database_value = match self.database_select.read(cx).selected_value() {
            Some(database) => Some(database.clone()),
            None => {
                window.push_notification("Please select a database", cx);
                return;
            }
        };

        if sql.trim().is_empty() {
            window.push_notification("Please enter a query", cx);
            return;
        }

        sql_result_tab_container.update(cx, |container, cx| {
            container.handle_run_query(sql, connection_id, current_database_value, window, cx);
        })
    }

    fn handle_format_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let text = self.get_sql_text(cx);
        if text.trim().is_empty() {
            window.push_notification("No SQL to format", cx);
            return;
        }

        let formatted = Self::format_sql(&text);
        self.editor.update(cx, |s, cx| s.set_value(formatted, window, cx));
    }

    /// Format SQL with proper indentation and line breaks
    fn format_sql(sql: &str) -> String {
        let mut formatted = String::new();
        let mut indent_level: usize = 0;
        let lines: Vec<&str> = sql.lines().collect();

        for line in lines {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Decrease indent for closing parentheses
            if trimmed.starts_with(')') {
                indent_level = indent_level.saturating_sub(1);
            }

            // Keywords that should be on new line at base level
            let is_major_keyword = trimmed.starts_with("SELECT")
                || trimmed.starts_with("FROM")
                || trimmed.starts_with("WHERE")
                || trimmed.starts_with("GROUP BY")
                || trimmed.starts_with("HAVING")
                || trimmed.starts_with("ORDER BY")
                || trimmed.starts_with("LIMIT")
                || trimmed.starts_with("UNION")
                || trimmed.starts_with("INSERT")
                || trimmed.starts_with("UPDATE")
                || trimmed.starts_with("DELETE")
                || trimmed.starts_with("CREATE")
                || trimmed.starts_with("ALTER")
                || trimmed.starts_with("DROP");

            let is_join = trimmed.starts_with("INNER JOIN")
                || trimmed.starts_with("LEFT JOIN")
                || trimmed.starts_with("RIGHT JOIN")
                || trimmed.starts_with("FULL JOIN")
                || trimmed.starts_with("CROSS JOIN")
                || trimmed.starts_with("JOIN");

            // Set indent level for major keywords
            if is_major_keyword && indent_level > 0 {
                indent_level = 0;
            }

            // Add indentation
            if !formatted.is_empty() && !formatted.ends_with('\n') {
                formatted.push('\n');
            }
            formatted.push_str(&"  ".repeat(indent_level));
            formatted.push_str(trimmed);

            // Increase indent after SELECT or other keywords
            if trimmed.ends_with("SELECT") || trimmed.starts_with("SELECT") {
                if !trimmed.ends_with(';') {
                    indent_level = 1;
                }
            }

            // JOIN at same level as FROM
            if is_join {
                indent_level = 0;
            }

            // Handle opening parentheses
            if trimmed.ends_with('(') {
                indent_level += 1;
            }

            // Add newline
            formatted.push('\n');
        }

        formatted.trim().to_string()
    }

    fn handle_compress_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let text = self.get_sql_text(cx);
        let compressed = text
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        self.editor.update(cx, |e, cx| e.set_value(compressed, window, cx));
    }

    fn handle_save_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        use one_core::storage::query_model::Query;
        use std::time::{SystemTime, UNIX_EPOCH};

        let sql = self.get_sql_text(cx);
        if sql.trim().is_empty() {
            window.push_notification("Query content is empty", cx);
            return;
        }

        let connection_id = self.connection_id.clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();
        let saved_database = self.database_select.read(cx).selected_value().cloned();

        let start = SystemTime::now();
        let since_epoch = match start.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration,
            Err(e) => {
                error!("Failed to generate query name: {}", e);
                window.push_notification("Failed to save query: invalid system time", cx);
                return;
            }
        };
        let query_name = format!("Query_{}", since_epoch.as_secs());

        let mut query = Query::new(query_name, sql, connection_id.clone(), saved_database.clone());
        cx.spawn(async move |this: WeakEntity<Self>, cx: &mut AsyncApp| {

            let storage = storage_manager.clone();
            match Tokio::spawn_result(cx, async move {
                let storage = storage.clone();
                let query_repo = storage
                    .get::<QueryRepository>()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("Query repository not found"))?;
                query_repo.insert(&mut query).await?;
                Ok::<Option<i64>, anyhow::Error>(query.id)
            }) {
                Ok(task) => match task.await {
                    Ok(_) => {
                        if let Err(e) = cx.update(|cx| {
                            if let Some(window_id) = cx.active_window() {
                                cx.update_window(window_id, |_entity, window, cx| {
                                    window.push_notification("Query saved", cx);
                                })
                            } else {
                                Err(anyhow::anyhow!("No active window"))
                            }
                        }) {
                            error!("Failed to show save notification: {}", e);
                        }

                        if let Err(e) = cx.update(|cx| {
                            if let Some(entity) = this.upgrade() {
                                entity.update(cx, |this, cx| {
                                    cx.emit(SqlEditorEvent::QuerySaved {
                                        connection_id: this.connection_id.clone(),
                                        database: saved_database.clone(),
                                    });
                                });
                                Ok(())
                            } else {
                                Err(anyhow::anyhow!("SqlEditorTab dropped"))
                            }
                        }) {
                            error!("Failed to emit QuerySaved event: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to save query: {}", e);
                        SqlEditorTab::notify_async(cx, format!("Failed to save query: {}", e));
                    }
                },
                Err(e) => {
                    error!("Failed to enqueue query save: {}", e);
                    SqlEditorTab::notify_async(cx, format!("Failed to save query: {}", e));
                }
            }

            Ok::<(), anyhow::Error>(())
        })
        .detach();
    }

    fn handle_show_results(&mut self, _: &ClickEvent, _window: &mut Window, cx: &mut Context<Self>) {
        self.sql_result_tab_container.update(cx, |container, cx| {
            container.show(cx);
        });
    }

    fn handle_export_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        use gpui::ClipboardItem;

        let selected_text = self.editor.read(cx).get_selected_text_from_app(cx);
        let sql = if selected_text.trim().is_empty() {
            self.get_sql_text(cx)
        } else {
            selected_text
        };

        if sql.trim().is_empty() {
            window.push_notification("No SQL to export", cx);
            return;
        }

        // Copy SQL to clipboard
        cx.write_to_clipboard(ClipboardItem::new_string(sql.clone()));
        window.push_notification("SQL copied to clipboard", cx);
    }

    fn handle_clear_editor(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |e, cx| e.set_value(String::new(), window, cx));
        window.push_notification("Editor cleared", cx);
    }

    fn handle_copy_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        use gpui::ClipboardItem;

        let selected_text = self.editor.read(cx).get_selected_text_from_app(cx);
        let sql = if selected_text.trim().is_empty() {
            self.get_sql_text(cx)
        } else {
            selected_text
        };

        if sql.trim().is_empty() {
            window.push_notification("No SQL to copy", cx);
            return;
        }

        cx.write_to_clipboard(ClipboardItem::new_string(sql));
        window.push_notification("SQL copied to clipboard", cx);
    }

    fn handle_uppercase_keywords(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let text = self.get_sql_text(cx);
        if text.trim().is_empty() {
            window.push_notification("No SQL to process", cx);
            return;
        }

        let uppercased = Self::uppercase_keywords(&text);
        self.editor.update(cx, |e, cx| e.set_value(uppercased, window, cx));
    }

    /// Convert SQL keywords to uppercase while preserving string literals
    fn uppercase_keywords(sql: &str) -> String {
        let keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE",
            "IS", "NULL", "AS", "DISTINCT", "ALL", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
            "CROSS", "ON", "USING", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT",
            "OFFSET", "UNION", "INTERSECT", "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE",
            "SET", "DELETE", "CREATE", "TABLE", "INDEX", "VIEW", "ALTER", "DROP", "TRUNCATE",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "CASE",
            "WHEN", "THEN", "ELSE", "END", "WITH", "RECURSIVE",
        ];

        let mut result = String::new();
        let mut current_word = String::new();
        let mut in_string = false;
        let mut string_char = ' ';

        for ch in sql.chars() {
            // Handle string literals
            if (ch == '\'' || ch == '"') && !in_string {
                if !current_word.is_empty() {
                    let upper = current_word.to_uppercase();
                    if keywords.contains(&upper.as_str()) {
                        result.push_str(&upper);
                    } else {
                        result.push_str(&current_word);
                    }
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

            // Build words
            if ch.is_alphanumeric() || ch == '_' {
                current_word.push(ch);
            } else {
                if !current_word.is_empty() {
                    let upper = current_word.to_uppercase();
                    if keywords.contains(&upper.as_str()) {
                        result.push_str(&upper);
                    } else {
                        result.push_str(&current_word);
                    }
                    current_word.clear();
                }
                result.push(ch);
            }
        }

        if !current_word.is_empty() {
            let upper = current_word.to_uppercase();
            if keywords.contains(&upper.as_str()) {
                result.push_str(&upper);
            } else {
                result.push_str(&current_word);
            }
        }

        result
    }
}


impl Render for SqlEditorTab {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let editor = self.editor.clone();
        let database_select = self.database_select.clone();

        // Check if there are any results and if the panel is visible
        let has_results = self.sql_result_tab_container.read(cx).has_results(cx);
        let results_visible = self.sql_result_tab_container.read(cx).is_visible(cx);

        // Check if there is selected text in the editor
        let has_selection = !self.editor.read(cx).get_selected_text_from_app(cx).trim().is_empty();

        // Build the main layout with conditional resizable panels
        v_flex()
            .size_full()
            .child(
                v_resizable("sql-editor-resizable")
                    .child(
                        resizable_panel()
                            .child(
                                v_flex()
                                    .size_full()
                                    .gap_2()
                                    .child(
                                // Toolbar
                                h_flex()
                                    .gap_2()
                                    .p_2()
                                    .bg(cx.theme().muted)
                                    .rounded_md()
                                    .items_center()
                                    .w_full()
                                    .child(
                                        // Database selector
                                        Select::new(&database_select)
                                            .with_size(Size::Small)
                                            .placeholder("Select Database")
                                            .w(px(200.))
                                    )
                                    .child(
                                        Button::new("run-query")
                                            .with_size(Size::Small)
                                            .primary()
                                            .label(if has_selection {
                                                "运行已选择的 (⌘+Enter)"
                                            } else {
                                                "运行 (⌘+Enter)"
                                            })
                                            .icon(IconName::ArrowRight)
                                            .on_click(cx.listener(Self::handle_run_query)),
                                    )
                                    .child(
                                        Button::new("format-query")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("格式化")
                                            .icon(IconName::Star)
                                            .on_click(cx.listener(Self::handle_format_query)),
                                    )
                                    .child(
                                        Button::new("uppercase-keywords")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("大写关键字")
                                            .on_click(cx.listener(Self::handle_uppercase_keywords)),
                                    )
                                    .child(
                                        Button::new("save-query")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("保存查询")
                                            .icon(IconName::Plus)
                                            .on_click(cx.listener(Self::handle_save_query)),
                                    )
                                    .child(
                                        Button::new("compress-query")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("压缩")
                                            .on_click(cx.listener(Self::handle_compress_query)),
                                    )
                                    .child(
                                        Button::new("copy-query")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("复制")
                                            .icon(IconName::Copy)
                                            .on_click(cx.listener(Self::handle_copy_query)),
                                    )
                                    .child(
                                        Button::new("clear-editor")
                                            .with_size(Size::Small)
                                            .ghost()
                                            .label("清空")
                                            .icon(IconName::Delete)
                                            .on_click(cx.listener(Self::handle_clear_editor)),
                                    )
                                    .when(has_results && !results_visible, |this| {
                                        this.child(
                                            Button::new("show-results")
                                                .with_size(Size::Small)
                                                .ghost()
                                                .label("Show Results")
                                                .icon(IconName::ArrowUp)
                                                .on_click(cx.listener(Self::handle_show_results))
                                        )
                                    })
                            )
                            .child(
                                // Editor
                                v_flex()
                                    .flex_1()
                                    .child(editor.clone())
                            )
                            )
                    )
                    .when(has_results && results_visible, |this| {
                        this.child(
                            // Bottom panel: Results with tabs
                            resizable_panel()
                                .size(px(400.))
                                .size_range(px(400.)..px(800.))
                                .child(self.sql_result_tab_container.clone())
                        )
                    })
            )
    }
}


// Make it Clone so we can use it in closures
impl Clone for SqlEditorTab {
    fn clone(&self) -> Self {
        Self {
            title: self.title.clone(),
            editor: self.editor.clone(),
            connection_id: self.connection_id.clone(),
            sql_result_tab_container: self.sql_result_tab_container.clone(),
            database_select: self.database_select.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Focusable for SqlEditorTab {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<SqlEditorEvent> for SqlEditorTab {}


pub struct SqlEditorTabContent {
    title: SharedString,
    sql_editor_tab: Entity<SqlEditorTab>
}

impl SqlEditorTabContent {

    pub fn new(
        title: impl Into<SharedString>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        // Create with empty connection_id - should not be used in practice
        Self::new_with_config(title, "", None, None, window, cx)
    }

    pub fn new_with_config(
        title: impl Into<SharedString>,
        connection_id: impl Into<String>,
        query_id: Option<i64>,
        initial_database: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        let title = title.into();
        let sql_editor_tab = cx.new(|cx| SqlEditorTab::new_with_config(title.clone(), connection_id, query_id, initial_database, window, cx));

        Self {
            title,
            sql_editor_tab
        }
    }

    pub fn new_with_query_id(
        query_id: i64,
        title: impl Into<SharedString>,
        connection_id: impl Into<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        Self::new_with_config(title, connection_id, Some(query_id), None, window, cx)
    }
}

impl TabContent for SqlEditorTabContent {
    fn title(&self) -> SharedString {
        self.title.clone()
    }

    fn icon(&self) -> Option<Icon> {
        Some(IconName::File.color())
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, _window: &mut Window, _cx: &mut App) -> AnyElement {
        self.sql_editor_tab.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::SqlEditor
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}


impl Clone for SqlEditorTabContent  {
    fn clone(&self) -> Self {
        Self {
            title: self.title.clone(),
            sql_editor_tab: self.sql_editor_tab.clone()
        }
    }
}