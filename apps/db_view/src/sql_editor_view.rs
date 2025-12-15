use one_core::storage::traits::Repository;
use one_core::gpui_tokio::Tokio;
use crate::sql_editor::SqlEditor;
use crate::sql_result_tab::SqlResultTabContainer;
use one_core::tab_container::{TabContent, TabContentType};
use db::{GlobalDbState};
use gpui::{div, px, AnyElement, App, AppContext, AsyncApp, ClickEvent, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, WeakEntity, Window};
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::resizable::{resizable_panel, v_resizable};
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::{h_flex, v_flex, ActiveTheme, IconName, IndexPath, Sizable, Size, WindowExt};
use std::any::Any;
use std::sync::{Arc, RwLock};
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
        let editor = cx.new(|cx| SqlEditor::new(window, cx));
        let focus_handle = cx.focus_handle();

        let result_tabs = Arc::new(RwLock::new(Vec::new()));
        let active_result_tab = Arc::new(RwLock::new(0));

        // Create database select with empty items initially
        let database_select = cx.new(|cx| {
            SelectState::new(SearchableVec::new(vec![]), None, window, cx)
        });

        let instance = Self {
            title: title.into(),
            editor: editor.clone(),
            connection_id: connection_id.into(),
            sql_result_tab_container: cx.new(|cx| SqlResultTabContainer::new(result_tabs, active_result_tab,cx)),
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

    // Create a new instance that loads a specific query by ID
    pub fn new_with_query_id(
        query_id: i64,
        title: impl Into<SharedString>,
        connection_id: impl Into<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        Self::new_with_config(title, connection_id, Some(query_id), None, window, cx)
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
                    eprintln!("Failed to get connection: {}", e);
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
                    let pool = storage.get_pool().await?;
                    query_repo.get(&pool, query_id).await
                }) {
                    Ok(task) => match task.await {
                        Ok(Some(query)) => {
                            sql_content = Some(query.content);
                            if let Some(db_name) = query.database_name {
                                resolved_database = Some(db_name);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => eprintln!("Failed to get query: {}", e),
                    },
                    Err(e) => eprintln!("Failed to enqueue query load: {}", e),
                }
            }

            if let Some(ref db) = resolved_database {
                instance.update_schema_for_db(global_state, db, cx).await;
            }

            let selected_name = resolved_database.clone().or_else(|| databases.get(0).cloned());

            if let Some(sql) = sql_content {
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        cx.update_window(window_id, |_entity, window, cx| {
                            editor.update(cx, |e, cx| {
                                e.set_value(sql.clone(), window, cx);
                            });
                        })
                    } else {
                        Err(anyhow::anyhow!("No active window"))
                    }
                });
            }

            let update_result = cx.update(|cx| {
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
                    })
                } else {
                    Err(anyhow::anyhow!("No active window"))
                }
            });

            if let Err(e) = update_result {
                eprintln!("Failed to update dropdown: {:?}", e);
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
                eprintln!("Failed to get connection: {}", e);
                return;
            }
        };

        // Get database-specific completion info
        let db_completion_info = global_state.get_completion_info(cx, connection_id.clone()).await.unwrap();

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
        cx.update(|cx| {
            if let Some(window_id) = cx.active_window() {
                let _ = cx.update_window(window_id, |_entity, window, cx| {
                    editor.update(cx, |e, cx| {
                        e.set_db_completion_info(db_completion_info, schema, window, cx);
                    });
                });
            }
        }).ok();
    }

    fn get_sql_text(&self, cx: &App) -> String {
        self.editor.read(cx).get_text_from_app(cx)
    }

    fn handle_run_query(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let sql = self.get_sql_text(cx);
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let sql_result_tab_container = self.sql_result_tab_container.clone();

        let current_database = self.database_select.read(cx).selected_value();

        let mut current_database_value = None;
        if let Some(database) = current_database {
            current_database_value = Some(database.clone());
        }else {
            window.push_notification("Please select a database",cx);
            return;
        }


        if sql.trim().is_empty() {
            window.push_notification("Please enter a query",cx);
            return;
        }

        cx.spawn(async move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let result = global_state.execute_script(cx, connection_id, sql.clone(), current_database_value, None).await;
            match result {
                Ok(results) => {
                    // Update result tabs
                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                sql_result_tab_container.update(cx, |state, cx| {
                                    state.set_result(&sql, results, window, cx);
                                });
                            });
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to execute script: {}", e)
                }
            };
        }).detach();
    }

    fn handle_format_query(&self, _: &ClickEvent, window: &mut Window, cx: &mut App) {
        let text = self.get_sql_text(cx);
        let formatted = text
            .split('\n')
            .map(|l| l.trim().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        self.editor
            .update(cx, |s, cx| s.set_value(formatted, window, cx));
    }

    fn handle_save_query(&self, _: &ClickEvent, window: &mut Window, cx: &mut App) {
        use one_core::storage::query_model::Query;
        use one_core::storage::traits::Repository;
        use std::time::{SystemTime, UNIX_EPOCH};

        let sql = self.get_sql_text(cx);
        let connection_id = self.connection_id.clone();
        let storage_manager = cx.global::<GlobalStorageState>().storage.clone();
        let current_db = self.database_select.read(cx).selected_value();

        // Generate a default name for the query
        let start = SystemTime::now();
        let since_epoch = start.duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let query_name = format!("Query_{}", since_epoch.as_secs());

        // Create the query object
        let mut query = Query::new(
            query_name,
            sql,
            connection_id,
            if let Some (db) = current_db { Some(db.clone())  } else { None }
        );

        // Spawn the async task
        cx.spawn(async move |cx: &mut AsyncApp| {
            let storage = storage_manager.clone();
            match Tokio::spawn_result(cx, async move {
                let storage = storage.clone();
                let query_repo = storage.get::<QueryRepository>().await
                    .ok_or_else(|| anyhow::anyhow!("Query repository not found"))?;
                let pool = storage.get_pool().await?;
                query_repo.insert(&pool, &mut query).await?;
                Ok::<Option<i64>, anyhow::Error>(query.id)
            }) {
                Ok(task) => match task.await {
                    Ok(_) => {
                        let _ = cx.update(|cx| {
                            if let Some(window_id) = cx.active_window() {
                                cx.update_window(window_id, |_entity, window, cx| {
                                    window.push_notification("Query saved", cx);

                                    // TODO - 刷新树
                                })
                            } else {
                                Err(anyhow::anyhow!("No active window"))
                            }
                        });
                    }
                    Err(e) => error!("Failed to save query: {}", e),
                },
                Err(e) => error!("Failed to enqueue query save: {}", e),
            }

            Ok::<(), anyhow::Error>(())
        }).detach();

        // TODO - 刷新树
    }
}


impl Render for SqlEditorTab {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let editor = self.editor.clone();
        let database_select = self.database_select.clone();

        // Build the main layout with resizable panels
        // Wrap in v_flex().size_full() to ensure proper containment within tab
        v_flex()
            .size_full()
            .child(v_resizable("sql-editor-resizable")
                .child(
                    // Top panel: Toolbar and Editor
                    resizable_panel()
                        .size(px(400.))
                        .size_range(px(200.)..px(800.))
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
                                                .label("Run (⌘+Enter)")
                                                .icon(IconName::ArrowRight)
                                                .on_click(cx.listener(Self::handle_run_query)),
                                        )
                                        .child(
                                            Button::new("format-query")
                                                .with_size(Size::Small)
                                                .ghost()
                                                .label("Format")
                                                .icon(IconName::Star)
                                                .on_click({
                                                    let this = self.clone();
                                                    move |e, w, cx| this.handle_format_query(e, w, cx)
                                                }),
                                        )
                                        .child(
                                            Button::new("save-query")
                                                .with_size(Size::Small)
                                                .ghost()
                                                .label("Save Query")
                                                .icon(IconName::Plus)
                                                .on_click({
                                                    let this = self.clone();
                                                    move |e, w, cx| this.handle_save_query(e, w, cx)
                                                }),
                                        )
                                        .child(
                                            Button::new("compress-query")
                                                .with_size(Size::Small)
                                                .ghost()
                                                .label("Compress")
                                                .on_click({
                                                    let this = self.clone();
                                                    move |_e, w, cx| {
                                                        let text = this.get_sql_text(cx);
                                                        let compressed = text.lines()
                                                            .map(|l| l.trim())
                                                            .filter(|l| !l.is_empty())
                                                            .collect::<Vec<_>>()
                                                            .join(" ");
                                                        this.editor.update(cx, |e, cx| e.set_value(compressed, w, cx));
                                                    }
                                                }),
                                        )
                                        .child(
                                            Button::new("export-query")
                                                .with_size(Size::Small)
                                                .ghost()
                                                .label("Export")
                                                .on_click({
                                                    move |_, _, _| {
                                                        // TODO: Implement export functionality
                                                    }
                                                }),
                                        )
                                )
                                .child(
                                    // Editor
                                    v_flex()
                                        .flex_1()
                                        .child(editor)
                                )
                        )
                )
                .child(
                    // Bottom panel: Results with tabs
                    resizable_panel()
                        .child(self.sql_result_tab_container.clone())
                )
                .into_any_element())
            .into_any_element()
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

    fn icon(&self) -> Option<IconName> {
        Some(IconName::File)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::SqlEditor
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn render_content(&self, _window: &mut Window, cx: &mut App) -> AnyElement {
        self.sql_editor_tab.clone().into_any_element()
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