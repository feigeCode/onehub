use std::sync::Arc;
// 2. 外部 crate 导入（按字母顺序）
use gpui::{div, px, AnyElement, App, AppContext, AsyncApp, Context, Entity, IntoElement, ParentElement, Render, Styled, Window};
use tracing::log::error;
use gpui_component::{button::ButtonVariants, h_flex, list::ListItem, tab::{Tab, TabBar}, table::Column, v_flex, ActiveTheme, IconName, Sizable, Size, StyledExt};

use crate::data_grid::{DataGrid, DataGridConfig, DataGridUsage};
// 3. 当前 crate 导入（按模块分组）
use db::{GlobalDbState, SqlResult};

// Structure to hold a single SQL result with its metadata
#[derive(Clone)]
pub struct SqlResultTab {
    pub sql: String,
    pub result: SqlResult,
    pub execution_time: String,
    pub rows_count: String,
    pub data_grid: Option<Entity<DataGrid>>,
}


#[derive(Clone)]
pub struct SqlResultTabContainer {
    pub result_tabs: Entity<Vec<SqlResultTab>>,
    pub active_result_tab: Entity<Arc<usize>>,
    // Store all results for summary view (including non-query results)
    pub all_results: Entity<Vec<SqlResult>>,
    // 控制结果面板的显示/隐藏状态
    pub is_visible: Entity<bool>,
}

impl SqlResultTabContainer {
    pub(crate) fn new(cx: &mut Context<Self>) -> SqlResultTabContainer {
        let result_tabs = cx.new(|_| vec![]);
        let active_result_tab = cx.new(|_| Arc::new(0));
        let all_results = cx.new(|_| vec![]);
        let is_visible = cx.new(|_| false); // 默认隐藏
        SqlResultTabContainer {
            result_tabs,
            active_result_tab,
            all_results,
            is_visible,
        }
    }
}

impl SqlResultTabContainer {

    pub fn handle_run_query(&mut self, sql: String, connection_id: String, current_database_value: Option<String> , _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let mut clone_self = self.clone();
        let connection_id_clone = connection_id.clone();
        let database_clone = current_database_value.clone();
        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;
            let database_type = config.map(|c| c.database_type).unwrap_or(one_core::storage::DatabaseType::MySQL);

            let result = global_state
                .execute_script(cx, connection_id_clone.clone(), sql.clone(), current_database_value, None)
                .await;
            match result {
                Ok(results) => {
                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                clone_self.set_result(results, connection_id_clone.clone(), database_clone.clone(), database_type, window, cx);
                            });
                        }
                    });
                }
                Err(e) => {
                    error!("Error executing query: {:?}", e)
                }
            };
        }).detach();
    }

    pub fn set_result(
        &mut self,
        results: Vec<SqlResult>,
        connection_id: String,
        database: Option<String>,
        database_type: one_core::storage::DatabaseType,
        window: &mut Window,
        cx: &mut App,
    ) {
        // Create tabs only for query results, store all results for summary
        let mut query_tabs: Vec<SqlResultTab> = vec![];
        let mut all_result_tabs: Vec<SqlResult> = vec![];

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                SqlResult::Query(query_result) => {
                    let clone_query_result = query_result.clone();
                    // 使用真实的数据库名和表名
                    let db_name = database.clone().unwrap_or_default();
                    let table_name = query_result.table_name.clone().unwrap_or_else(|| format!("result_{}", idx));
                    let config = DataGridConfig::new(
                        db_name,
                        table_name,
                        &connection_id,
                        database_type,
                    )
                    .editable(query_result.editable)
                    .show_toolbar(true)
                    .usage(DataGridUsage::SqlResult)
                    .sql(query_result.sql.clone());

                    let data_grid = cx.new(|cx| DataGrid::new(config, window, cx));

                    // 准备数据
                    let columns = query_result.columns.iter()
                        .map(|h| Column::new(h.clone(), h.clone()))
                        .collect();
                    let rows = query_result.rows.iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| cell.clone().unwrap_or_else(|| "NULL".to_string()))
                                .collect()
                        })
                        .collect();

                    // 更新DataGrid数据
                    data_grid.update(cx, |this, cx|{
                        this.update_data(columns, rows, cx);
                        this.set_filter_schema(query_result.columns.clone(), cx);
                    });

                    let tab = SqlResultTab {
                        sql: query_result.sql.clone(),
                        result: SqlResult::Query(query_result.clone()),
                        execution_time: format!("{}ms", query_result.elapsed_ms),
                        rows_count: format!("{} rows", query_result.rows.len()),
                        data_grid: Some(data_grid),
                    };

                    // Add to both query tabs and all results
                    query_tabs.push(tab.clone());
                    all_result_tabs.push(SqlResult::Query(clone_query_result));
                }
                re => all_result_tabs.push(re)
            }
        }

        // 保存是否有结果页签
        let has_query_tabs = !query_tabs.is_empty();

        // 更新实体数据
        self.result_tabs.update(cx, |tabs, cx| {
            *tabs = query_tabs;
            cx.notify();
        });

        self.all_results.update(cx, |results, cx| {
            *results = all_result_tabs;
            cx.notify();
        });

        // 如果有结果页签，默认打开第一个结果页签；否则打开摘要页
        self.active_result_tab.update(cx, |active, cx| {
            *active = Arc::new(if has_query_tabs { 1 } else { 0 });
            cx.notify();
        });

        // 有新结果时自动显示面板
        self.is_visible.update(cx, |visible, cx| {
            *visible = true;
            cx.notify();
        });
    }

    /// 切换结果面板的显示/隐藏状态
    pub fn toggle_visibility(&mut self, cx: &mut App) {
        self.is_visible.update(cx, |visible, cx| {
            *visible = !*visible;
            cx.notify();
        });
    }

    /// 显示结果面板
    pub fn show(&mut self, cx: &mut App) {
        self.is_visible.update(cx, |visible, cx| {
            *visible = true;
            cx.notify();
        });
    }

    /// 隐藏结果面板
    pub fn hide(&mut self, cx: &mut App) {
        self.is_visible.update(cx, |visible, cx| {
            *visible = false;
            cx.notify();
        });
    }

    /// 检查是否有结果数据
    pub fn has_results(&self, cx: &App) -> bool {
        !self.all_results.read(cx).is_empty()
    }

    /// 检查面板是否可见
    pub fn is_visible(&self, cx: &App) -> bool {
        *self.is_visible.read(cx)
    }

}

impl Render for SqlResultTabContainer {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let clone_self = self.clone();
        let query_tabs = self.result_tabs.read(cx);
        let all_results = self.all_results.read(cx);
        let active_idx = **self.active_result_tab.read(cx);
        let is_visible = *self.is_visible.read(cx);

        // 如果面板不可见，返回空的 div
        if !is_visible {
            return div().size_full();
        }

        if all_results.is_empty() {
            // Show empty state
            v_flex()
                .size_full()
                .bg(cx.theme().background)
                .border_1()
                .border_color(cx.theme().border)
                .rounded_md()
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_color(cx.theme().muted_foreground)
                        .child("Execute a query to see results")
                )
        } else {
            // Show tabs with results
            v_flex()
                .size_full()
                .gap_0()
                .child(
                    // Tab bar container with close button
                    h_flex()
                        .w_full()
                        .items_center()
                        .child(
                            // Tab bar for result tabs (摘要 + individual results)
                            TabBar::new("result-tabs")
                                .flex_1()
                                .underline()
                                .with_size(Size::Small)
                                .selected_index(active_idx)
                                .on_click({
                                    let clone_self = clone_self.clone();
                                    move |ix: &usize, _w, cx| {
                                        clone_self.active_result_tab.update(cx, |active, cx| {
                                            *active = Arc::new(*ix);
                                            cx.notify();
                                        });
                                    }
                                })
                                .child(
                                    // Summary tab
                                    Tab::new().label("摘要")
                                )
                                .children(query_tabs.iter().enumerate().map(|(idx, tab)| {
                                    Tab::new().label(format!("结果{} ({}, {})", idx + 1, tab.rows_count, tab.execution_time))
                                }))
                        )
                        .child(
                            // Close button
                            gpui_component::button::Button::new("close-results")
                                .with_size(Size::Small)
                                .ghost()
                                .icon(IconName::Close)
                                .tooltip("隐藏结果面板")
                                .on_click({
                                    let close_self = clone_self.clone();
                                    move |_, _, cx| {
                                        close_self.clone().hide(cx);
                                    }
                                })
                        )
                )
                .child(
                    // Active tab content - 优化布局以支持大文本编辑器
                    if active_idx == 0 {
                        // Show summary view
                        div()
                            .flex_1()
                            .bg(cx.theme().background)
                            .border_1()
                            .border_color(cx.theme().border)
                            .rounded_md()
                            .overflow_hidden()
                            .child(render_summary_view(all_results, cx))
                            .into_any_element()
                    } else {
                        // Show individual result table with toolbar - 给DataGrid完整的空间
                        query_tabs.get(active_idx - 1)
                            .and_then(|tab| tab.data_grid.as_ref())
                            .map(|data_grid| {
                                data_grid.clone().into_any_element()
                            })
                            .unwrap_or_else(|| {
                                div()
                                    .flex_1()
                                    .bg(cx.theme().background)
                                    .border_1()
                                    .border_color(cx.theme().border)
                                    .rounded_md()
                                    .into_any_element()
                            })
                    }
                )
        }
    }
}

// Render summary view function
fn render_summary_view(tabs: &Vec<SqlResult>, cx: &App) -> AnyElement {
    let mut total_rows = 0;
    let mut total_time = 0.0;
    let mut success_count = 0;
    let mut error_count = 0;

    for tab in tabs {
        match tab {
            SqlResult::Query(q) => {
                total_rows += q.rows.len();
                total_time += q.elapsed_ms as f64;
                success_count += 1;
            }
            SqlResult::Exec(e) => {
                total_rows += e.rows_affected as usize;
                total_time += e.elapsed_ms as f64;
                success_count += 1;
            }
            SqlResult::Error(_) => {
                error_count += 1;
            }
        }
    }

    v_flex()
        .size_full()
        .p_4()
        .gap_3()
        .child(
            // Summary header
            h_flex()
                .gap_4()
                .items_center()
                .child(
                    div()
                        .text_lg()
                        .font_semibold()
                        .child("执行摘要")
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(format!("共 {} 条语句", tabs.len()))
                )
        )
        .child(
            // Statistics
            h_flex()
                .gap_6()
                .child(
                    v_flex()
                        .gap_1()
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("成功")
                        )
                        .child(
                            div()
                                .text_xl()
                                .font_semibold()
                                .text_color(cx.theme().success)
                                .child(format!("{}", success_count))
                        )
                )
                .child(
                    v_flex()
                        .gap_1()
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("失败")
                        )
                        .child(
                            div()
                                .text_xl()
                                .font_semibold()
                                .text_color(cx.theme().danger)
                                .child(format!("{}", error_count))
                        )
                )
                .child(
                    v_flex()
                        .gap_1()
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("总耗时")
                        )
                        .child(
                            div()
                                .text_xl()
                                .font_semibold()
                                .child(format!("{:.2}ms", total_time))
                        )
                )
                .child(
                    v_flex()
                        .gap_1()
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("影响行数")
                        )
                        .child(
                            div()
                                .text_xl()
                                .font_semibold()
                                .child(format!("{}", total_rows))
                        )
                )
        )
        .child(
            // Divider
            div()
                .h(px(1.))
                .w_full()
                .bg(cx.theme().border)
        )
        .child(
            // Statement list
            v_flex()
                .gap_2()
                .flex_1()
                .overflow_y_hidden()
                .children(tabs.iter().enumerate().map(|(idx, tab)| {
                    let (sql,elapsed_ms, status_icon, status_color, status_text) = match tab {
                        SqlResult::Query(q) => (
                            q.sql.clone(),
                            q.elapsed_ms,
                            IconName::Check,
                            cx.theme().success,
                            format!("{} rows", q.rows.len())
                        ),
                        SqlResult::Exec(e) => (
                            e.sql.clone(),
                            e.elapsed_ms,
                            IconName::Check,
                            cx.theme().success,
                            format!("{} rows affected", e.rows_affected)
                        ),
                        SqlResult::Error(e) => (
                            e.sql.clone(),
                            0,
                            IconName::Close,
                            cx.theme().danger,
                            e.message.clone()
                        ),
                    };

                    ListItem::new(idx)
                        .child(
                            h_flex()
                                .gap_3()
                                .items_center()
                                .w_full()
                                .child(
                                    // Status icon
                                    div()
                                        .flex_shrink_0()
                                        .text_color(status_color)
                                        .child(status_icon)
                                )
                                .child(
                                    // SQL preview
                                    div()
                                        .flex_1()
                                        .text_sm()
                                        .truncate()
                                        .child(format!("语句{}: {}", idx + 1, sql))
                                )
                                .child(
                                    // Execution time
                                    div()
                                        .flex_shrink_0()
                                        .text_xs()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(elapsed_ms.to_string())
                                )
                                .child(
                                    // Status text
                                    div()
                                        .flex_shrink_0()
                                        .text_xs()
                                        .text_color(status_color)
                                        .child(status_text)
                                )
                        )
                }))
        )
        .into_any_element()
}
