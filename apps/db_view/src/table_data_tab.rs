use std::any::Any;
use gpui::{actions, div, AnyElement, App, AppContext, AsyncApp, Context, Corner, Entity, FocusHandle, Focusable, InteractiveElement, IntoElement, ParentElement, Render, SharedString, Styled, Subscription, Window};
use gpui_component::{h_flex, table::Column, v_flex, ActiveTheme, IconName, Sizable, Size};

use crate::data_grid::{notification, DataGrid, DataGridConfig};
use crate::filter_editor::{ColumnSchema, FilterEditorEvent, TableFilterEditor, TableSchema};
use db::{GlobalDbState, TableDataRequest};
use gpui_component::button::Button;
use gpui_component::menu::DropdownMenu;
use one_core::gpui_tokio::Tokio;
use one_core::tab_container::{TabContent, TabContentType};

// ============================================================================
// Table Data Tab Content - Display table rows
// ============================================================================

actions!([Page500, Page1000, Page2000, PageAll]);


#[derive(Clone)]
pub struct TableDataInfo {
    pub current_page: usize,
    pub page_size: usize,
    pub total_count: usize,
    pub duration: u128,
    pub current_sql: String,
}

impl Default for TableDataInfo {
    fn default() -> Self {
        Self {
            current_page: 1,
            page_size: 500,
            total_count: 0,
            duration: 0,
            current_sql: String::new(),
        }
    }
}

pub struct TableData {
    database_name: String,
    table_name: String,
    connection_id: String,
    database_type: one_core::storage::DatabaseType,
    /// DataGrid组件用于显示和编辑数据
    data_grid: Entity<DataGrid>,
    status_msg: Entity<String>,
    focus_handle: FocusHandle,
    /// Table data info - 统一的状态管理
    table_data_info: Entity<TableDataInfo>,
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
        
        let table_data_info = cx.new(|_| TableDataInfo::default());

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
            table_data_info,
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

    fn load_data_with_clauses(&self, page: usize, cx: &mut App) {
        eprintln!("load_data_with_clauses called for page: {}, table: {}.{}", page, self.database_name, self.table_name);
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let table_name = self.table_name.clone();
        let database_name = self.database_name.clone();
        let data_grid = self.data_grid.clone();
        let table_data_info = self.table_data_info.clone();
        let where_clause = self.filter_editor.read(cx).get_where_clause(cx);
        let order_by_clause = self.filter_editor.read(cx).get_order_by_clause(cx);
        let filter_editor = self.filter_editor.clone();
        
        // 在进入异步块前获取 page_size
        let page_size = self.table_data_info.read(cx).page_size;

        cx.spawn(async move |cx: &mut AsyncApp| {
            let table_name_for_schema = table_name.clone();
            
            let result = Tokio::spawn_result(cx, async move {
                let (plugin, conn_arc) = global_state.get_plugin_and_connection(&connection_id).await?;
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
                plugin.query_table_data(&**conn, &request).await
            }).ok();
            
            
            match result {
                None => {
                    cx.update(|cx| {
                        notification(cx, "Failed to get connection".to_string());
                    }).ok();
                }
                Some(task) => {
                    match task.await {
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

                            let pk_columns = response.primary_key_indices;
                            
                            // 更新统一的状态信息
                            cx.update(|cx| {
                                table_data_info.update(cx, |info, cx| {
                                    info.total_count = response.total_count;
                                    info.current_sql = response.executed_sql;
                                    info.duration = response.duration;
                                    info.current_page = response.page;
                                    cx.notify();
                                });
                            }).ok();
                            
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
                                        table_name: table_name_for_schema.clone(),
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
                            }).ok();
                        }
                        Err(e) => {
                            cx.update(|cx| {
                                notification(cx, format!("Query failed: {}", e))
                            }).ok();
                        }
                    }
                }
            }
        }).detach();
    }

    fn handle_refresh(&self, cx: &mut App) {
        let page = self.table_data_info.read(cx).current_page;
        self.load_data_with_clauses(page, cx);
    }

    fn handle_prev_page(&self, cx: &mut App) {
        let page = self.table_data_info.read(cx).current_page;
        if page > 1 {
            self.load_data_with_clauses(page - 1, cx);
        }
    }

    fn handle_next_page(&self, cx: &mut App) {
        let info = self.table_data_info.read(cx);
        let page = info.current_page;
        let total = info.total_count;
        let page_size = info.page_size;
        
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
        self.table_data_info.update(cx, |info, cx| {
            info.page_size = new_size;
            cx.notify();
        });
        self.load_data_with_clauses(1, cx);
    }

    fn handle_apply_query(&self, cx: &mut App) {
        self.load_data_with_clauses(1, cx);
    }

}


impl Render for TableData {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {

        let this_refresh = self.clone();
        let this_prev = self.clone();
        let this_next = self.clone();
        let table_data_info = self.table_data_info.read(cx);
        let data_grid = self.data_grid.read(cx);

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
                    .child(data_grid.render_table_area(window, cx))
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
                                let filtered_count = data_grid.table.read(cx).delegate().filtered_row_count();
                                let total_rows = data_grid.table.read(cx).delegate().rows.len();
                                if filtered_count < total_rows {
                                    format!(
                                        "显示 {} 条（共 {} 条，总计 {} 条）",
                                        filtered_count,
                                        total_rows,
                                        table_data_info.total_count
                                    )
                                } else {
                                    format!(
                                        "第 {} 页（共 {} 条）",
                                        table_data_info.current_page,
                                        table_data_info.total_count
                                    )
                                }
                            }),
                    )
                    // 查询耗时
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("查询耗时 {}ms", table_data_info.duration)),
                    )
                    // SQL显示
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .flex_1()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(table_data_info.current_sql.clone()),
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
                                let current_page_size = self.table_data_info.read(cx).page_size;
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
            table_data_info: self.table_data_info.clone(),
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




