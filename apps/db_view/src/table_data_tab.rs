use std::any::Any;

use gpui::{actions, div, AnyElement, App, AppContext, Context, Corner, Entity, FocusHandle, Focusable, InteractiveElement, IntoElement, ParentElement, Render, SharedString, Styled, Subscription, Window};
use gpui_component::{h_flex, table::Column, v_flex, ActiveTheme, IconName, Sizable, Size};

use crate::data_grid::{DataGrid, DataGridConfig};
use crate::filter_editor::{ColumnSchema, FilterEditorEvent, TableFilterEditor, TableSchema};
use db::{GlobalDbState, TableDataRequest};
use gpui_component::button::Button;
use gpui_component::menu::DropdownMenu;
use one_core::tab_container::{TabContent, TabContentType};

// ============================================================================
// Table Data Tab Content - Display table rows
// ============================================================================

actions!([Page500, Page1000, Page2000, PageAll]);

pub struct TableData {
    database_name: String,
    table_name: String,
    connection_id: String,
    database_type: one_core::storage::DatabaseType,
    /// DataGrid组件用于显示和编辑数据
    data_grid: Entity<DataGrid>,
    status_msg: Entity<String>,
    focus_handle: FocusHandle,
    /// Current page (1-based)
    current_page: Entity<usize>,
    /// Page size
    page_size: Entity<usize>,
    /// Total row count
    total_count: Entity<usize>,

    /// Query duration in milliseconds
    query_duration: Entity<u128>,
    /// Current query SQL
    current_sql: Entity<String>,
    /// Filter editor with WHERE and ORDER BY inputs
    filter_editor: Entity<TableFilterEditor>,

    _filter_sub: Option<Subscription>
}

impl TableData {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        connection_id: impl Into<String>,
        database_type: one_core::storage::DatabaseType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let database_name = database_name.into();
        let table_name = table_name.into();
        let connection_id = connection_id.into();

        // 创建DataGrid配置
        let config = DataGridConfig::new(
            database_name.clone(),
            table_name.clone(),
            connection_id.clone(),
            database_type,
        )
        .editable(true)
        .show_toolbar(true);

        let data_grid = cx.new(|cx|  DataGrid::new(config, window, cx));
        let status_msg = cx.new(|_| "Loading...".to_string());
        let focus_handle = cx.focus_handle();

        // Create filter editor with empty schema initially
        let filter_editor = cx.new(|cx| {
            TableFilterEditor::new(window, cx)
        });

        let current_page = cx.new(|_| 1usize);
        let page_size = cx.new(|_| 500usize);
        let total_count = cx.new(|_| 0usize);

        let query_duration = cx.new(|_| 0u128);
        let current_sql = cx.new(|_| String::new());

        let mut result = Self {
            database_name: database_name.clone(),
            table_name: table_name.clone(),
            connection_id,
            database_type,
            data_grid,
            status_msg,
            focus_handle,
            filter_editor,
            _filter_sub: None,
            current_page,
            page_size,
            total_count,
            query_duration,
            current_sql,
        };

        result.bind_query_apply(window, cx);

        // Load data initially
        eprintln!("TableDataTabContent::new - about to load data for table: {}.{}", database_name, table_name);
        result.load_data_with_clauses(1, cx);



        result
    }


    pub fn bind_query_apply(&mut self, window: &mut Window, cx: &mut Context<Self>) -> &mut Self{
        let _filter_sub = cx.subscribe_in(&self.filter_editor, window, |this: &mut TableData,_, evt: &FilterEditorEvent, _, cx|{
            match evt {
                FilterEditorEvent::QueryApply => {
                    this.handle_apply_query(cx);
                    cx.notify()
                },
            }
        });

        self._filter_sub = Some(_filter_sub);
        self
    }

    fn update_status(&self, message: String, cx: &mut App) {
        self.status_msg.update(cx, |s, cx| {
            *s = message;
            cx.notify();
        });
    }

    fn load_data_with_clauses(&self, page: usize, cx: &mut App) {
        eprintln!("load_data_with_clauses called for page: {}, table: {}.{}", page, self.database_name, self.table_name);
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let table_name = self.table_name.clone();
        let database_name = self.database_name.clone();
        let status_msg = self.status_msg.clone();
        let data_grid = self.data_grid.clone();
        let page_size = *self.page_size.read(cx);
        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);
        let filter_editor = self.filter_editor.clone();

        let this = self.clone();

        cx.spawn(async move |cx| {
            let start_time = std::time::Instant::now();

            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to get connection: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            // Build request with raw where/order by clauses
            let request = if page_size == 0 {
                TableDataRequest::new(&database_name, &table_name)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            } else {
                TableDataRequest::new(&database_name, &table_name)
                    .with_page(page, page_size)
                    .with_where_clause(where_clause.clone())
                    .with_order_by_clause(order_by_clause.clone())
            };

            match plugin.query_table_data(&**conn, &request).await {
                Ok(response) => {
                    let columns: Vec<Column> = response
                        .columns
                        .iter()
                        .map(|col| Column::new(col.name.clone(), col.name.clone()))
                        .collect();

                    let rows: Vec<Vec<String>> = response
                        .rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| cell.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()))
                                .collect()
                        })
                        .collect();

                    let row_count = rows.len();
                    let total = response.total_count;
                    let total_pages = if page_size == 0 { 1 } else { (total + page_size - 1) / page_size };
                    let pk_columns = response.primary_key_indices;
                    let duration = start_time.elapsed().as_millis();
                    let sql_str = response.executed_sql;

                    // Build column schema for completion providers
                    let column_schemas: Vec<ColumnSchema> = response
                        .columns
                        .iter()
                        .map(|col| ColumnSchema {
                            name: col.name.clone(),
                            data_type: col.db_type.clone(),
                            is_nullable: col.nullable,
                        })
                        .collect();

                    cx.update(|cx| {
                        // Update filter editor schema
                        filter_editor.update(cx, |editor, cx| {
                            editor.set_schema(TableSchema {
                                table_name: table_name.clone(),
                                columns: column_schemas,
                            }, cx);
                        });

                        // Debug: 打印数据信息
                        eprintln!("Loading data: {} columns, {} rows", columns.len(), rows.len());
                        if !columns.is_empty() {
                            eprintln!("First column: {}", columns[0].name);
                        }
                        if !rows.is_empty() && !rows[0].is_empty() {
                            eprintln!("First row first cell: {}", rows[0][0]);
                        }

                        // Update DataGrid
                        data_grid.update(cx, |grid, cx| {
                            grid.update_data(columns, rows, pk_columns, cx)
                        });
                        this.current_page.update(cx, |p, cx| {
                            *p = page;
                            cx.notify();
                        });
                        this.total_count.update(cx, |t, cx| {
                            *t = total;
                            cx.notify();
                        });
                        this.query_duration.update(cx, |d, cx| {
                            *d = duration;
                            cx.notify();
                        });
                        this.current_sql.update(cx, |s, cx| {
                            *s = sql_str;
                            cx.notify();
                        });
                        data_grid.update(cx, |grid, cx| {
                            grid.update_status(
                                format!("Page {}/{} ({} rows, {} total)", page, total_pages.max(1), row_count, total),
                                cx,
                            );
                        });

                        status_msg.update(cx, |s, cx| {
                            *s = format!("Page {}/{} ({} rows, {} total)", page, total_pages.max(1), row_count, total);
                            cx.notify();
                        });
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Query failed: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }

    fn handle_refresh(&self, cx: &mut App) {
        let page = *self.current_page.read(cx);
        self.load_data_with_clauses(page, cx);
    }

    fn handle_prev_page(&self, cx: &mut App) {
        let page = *self.current_page.read(cx);
        if page > 1 {
            self.load_data_with_clauses(page - 1, cx);
        }
    }

    fn handle_next_page(&self, cx: &mut App) {
        let page = *self.current_page.read(cx);
        let total = *self.total_count.read(cx);
        let page_size = *self.page_size.read(cx);
        if page_size == 0 {
            return;
        }
        let total_pages = (total + page_size - 1) / page_size;
        if page < total_pages {
            self.load_data_with_clauses(page + 1, cx);
        }
    }

    fn handle_page_change_500(&mut self,_:&Page500 , _: &mut Window, cx: &mut Context<Self>){
        self.handle_page_size_change(500, cx)
    }
    fn handle_page_change_1000(&mut self,_:&Page1000 , _: &mut Window, cx: &mut Context<Self>){
        self.handle_page_size_change(1000, cx)
    }
    fn handle_page_change_2000(&mut self,_:&Page2000 , _: &mut Window, cx: &mut Context<Self>){
        self.handle_page_size_change(2000, cx)
    }
    fn handle_page_change_all(&mut self,_:&PageAll , _: &mut Window, cx: &mut Context<Self>){
        self.handle_page_size_change(0, cx)
    }
    fn handle_page_size_change(&self, new_size: usize, cx: &mut App) {
        self.page_size.update(cx, |size, cx| {
            *size = new_size;
            cx.notify();
        });
        self.load_data_with_clauses(1, cx);
    }

    fn handle_apply_query(&self, cx: &mut App) {
        self.load_data_with_clauses(1, cx);
    }

    fn handle_save_changes(&self, cx: &mut App) {
        let save_request = match self.data_grid.read(cx).create_save_request(cx) {
            Some(req) => req,
            None => {
                self.update_status("No changes to save".to_string(), cx);
                return;
            }
        };

        let change_count = save_request.changes.len();
        self.update_status(format!("Saving {} changes...", change_count), cx);

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let status_msg = self.status_msg.clone();
        let data_grid = self.data_grid.clone();

        cx.spawn(async move |cx| {
            let (plugin, conn_arc) = match global_state.get_plugin_and_connection(&connection_id).await {
                Ok(result) => result,
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to get connection: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let conn = conn_arc.read().await;

            match plugin.apply_table_changes(&**conn, save_request).await {
                Ok(response) => {
                    cx.update(|cx| {
                        if response.errors.is_empty() {
                            data_grid.update(cx, |grid, cx| {
                                grid.clear_changes(cx);
                            });
                            status_msg.update(cx, |s, cx| {
                                *s = format!("Successfully saved {} changes", response.success_count);
                                cx.notify();
                            });
                        } else {
                            status_msg.update(cx, |s, cx| {
                                *s = format!(
                                    "Saved {} changes, {} errors: {}",
                                    response.success_count,
                                    response.errors.len(),
                                    response.errors.first().unwrap_or(&String::new())
                                );
                                cx.notify();
                            });
                        }
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status_msg.update(cx, |s, cx| {
                            *s = format!("Failed to save changes: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }
}


impl Render for TableData {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {

        let this_refresh = self.clone();
        let this_save = self.clone();
        let this_prev = self.clone();
        let this_next = self.clone();

        v_flex()
            .on_action(cx.listener(Self::handle_page_change_500))
            .on_action(cx.listener(Self::handle_page_change_1000))
            .on_action(cx.listener(Self::handle_page_change_2000))
            .on_action(cx.listener(Self::handle_page_change_all))
            .size_full()
            .gap_2()
            .pt_2()
            // 工具栏
            .child(
                self.data_grid.read(cx).render_toolbar(
                    move |cx| this_refresh.handle_refresh(cx),
                    move |cx| this_save.handle_save_changes(cx),
                    window,
                    cx,
                )
            )
            // 过滤器栏
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .w_full()
                    .px_2()
                    .child(self.filter_editor.clone()),
            )
            // 表格区域
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(self.data_grid.read(cx).render_table_area(window, cx))
            )
            .child(
                h_flex()
                    .gap_3()
                    .items_center()
                    .px_2()
                    .py_1()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    // 记录数显示
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().foreground)
                            .child({
                                let filtered_count = self.data_grid.read(cx).table.read(cx).delegate().filtered_row_count();
                                let total_rows = self.data_grid.read(cx).table.read(cx).delegate().rows.len();
                                if filtered_count < total_rows {
                                    format!(
                                        "显示 {} 条（共 {} 条，总计 {} 条）",
                                        filtered_count,
                                        total_rows,
                                        self.total_count.read(cx)
                                    )
                                } else {
                                    format!(
                                        "第 {} 页（共 {} 条）",
                                        self.current_page.read(cx),
                                        self.total_count.read(cx)
                                    )
                                }
                            }),
                    )
                    // 查询耗时
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("查询耗时 {}ms", self.query_duration.read(cx))),
                    )
                    // SQL显示
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .flex_1()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(format!("SQL: {}", self.current_sql.read(cx))),
                    )
                    // 分页控件
                    .child(
                        h_flex()
                            .gap_1()
                            .items_center()
                            .child(
                                Button::new("prev-page")
                                    .with_size(Size::Small)
                                    .icon(IconName::ChevronLeft)
                                    .on_click(move |_, _, cx| {
                                        this_prev.handle_prev_page(cx)
                                    }),
                            )
                            .child({
                                let current_page_size = *self.page_size.read(cx);
                                let label = match current_page_size {
                                    0 => "全部".to_string(),
                                    n => format!("{}", n),
                                };

                                Button::new("page-size-selector")
                                    .with_size(Size::Small)
                                    .label(label)
                                    .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
                                        menu.menu("500", Box::new(Page500))
                                            .menu("1000", Box::new(Page1000))
                                            .menu("2000", Box::new(Page2000))
                                            .menu("全部", Box::new(PageAll))
                                    })
                            })
                            .child(
                                Button::new("next-page")
                                    .with_size(Size::Small)
                                    .icon(IconName::ChevronRight)
                                    .on_click(move |_, _, cx| {
                                        this_next.handle_next_page(cx)
                                    }),
                            ),
                    )
            )
    }
}

impl Clone for TableData {
    fn clone(&self) -> Self {
        Self {
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
            connection_id: self.connection_id.clone(),
            database_type: self.database_type,
            data_grid: self.data_grid.clone(),
            status_msg: self.status_msg.clone(),
            focus_handle: self.focus_handle.clone(),
            filter_editor: self.filter_editor.clone(),
            _filter_sub: None,
            current_sql: self.current_sql.clone(),
            current_page: self.current_page.clone(),
            page_size: self.page_size.clone(),
            total_count: self.total_count.clone(),
            query_duration: self.query_duration.clone(),

        }
    }
}

impl Focusable for TableData {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

pub struct TableDataTabContent {
    pub table_data: Entity<TableData>,
    database_name: String,
    table_name: String,
}

impl TableDataTabContent {
    pub fn new(
        database_name: String,
        table_name: String,
        connection_id: impl Into<String>,
        database_type: one_core::storage::DatabaseType,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {

        let database_name_clone = database_name.clone();
        let table_name_clone = table_name.clone();

        let table_data = cx.new(|cx| TableData::new(database_name, table_name, connection_id, database_type, window, cx));

       Self {
            table_data,
            database_name: database_name_clone,
            table_name: table_name_clone,
        }
    }
}

impl TabContent for TableDataTabContent {
    fn title(&self) -> SharedString {
        format!("{}.{} - Data", self.database_name, self.table_name).into()
    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::Folder)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, _: &mut Window, _: &mut App) -> AnyElement {
        self.table_data.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::TableData(format!("{}.{}", self.database_name, self.table_name))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for TableDataTabContent {
    fn clone(&self) -> Self {
        Self {
            table_data: self.table_data.clone(),
            database_name: self.database_name.clone(),
            table_name: self.table_name.clone(),
        }
    }
}




