use std::sync::{Arc, RwLock};
use gpui::{div, px, AnyElement, App, AppContext, Context, Entity, IntoElement, ParentElement, Render, Styled, Window};
use db::SqlResult;
use gpui_component::table::{Column, Table, TableState};
use gpui_component::{h_flex, v_flex, ActiveTheme, IconName, Sizable, Size, StyledExt};
use gpui_component::list::ListItem;
use gpui_component::tab::{Tab, TabBar};
use crate::results_delegate::ResultsDelegate;

// Structure to hold a single SQL result with its metadata
#[derive(Clone)]
pub struct SqlResultTab {
    pub sql: String,
    pub result: SqlResult,
    pub execution_time: String,
    pub rows_count: String,
    pub table: Entity<TableState<ResultsDelegate>>,
}


#[derive(Clone)]
pub struct SqlResultTabContainer {
    pub result_tabs: Arc<RwLock<Vec<SqlResultTab>>>,
    pub active_result_tab: Arc<RwLock<usize>>,
}

impl SqlResultTabContainer {
    pub(crate) fn  new(result_tabs: Arc<RwLock<Vec<SqlResultTab>>>, active_result_tab: Arc<RwLock<usize>>, _cx: &mut Context<Self>) -> SqlResultTabContainer {
        SqlResultTabContainer {
            result_tabs,
            active_result_tab,
        }
    }
}

impl SqlResultTabContainer {

    pub fn set_result(&mut self, sql: &str, results: Vec<SqlResult>, window: &mut Window, cx: &mut Context<Self>) {
        // Split SQL into individual statements for labeling
        let sql_statements: Vec<String> = sql
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Create tabs for each result
        let mut new_tabs = Vec::new();

        for (idx, result) in results.iter().enumerate() {
            let sql_text = sql_statements.get(idx)
                .map(|s| {
                    if s.len() > 50 {
                        format!("{}...", &s[..50])
                    } else {
                        s.clone()
                    }
                })
                .unwrap_or_else(|| format!("Statement {}", idx + 1));

            match result {
                SqlResult::Query(query_result) => {
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
                    let delegate = ResultsDelegate::new(columns, rows);
                    let table = cx.new(|cx| TableState::new(delegate, window, cx));
                    new_tabs.push(SqlResultTab {
                        sql: sql_text,
                        result: result.clone(),
                        execution_time: format!("{}ms", query_result.elapsed_ms),
                        rows_count: format!("{} rows", query_result.rows.len()),
                        table,
                    });
                }
                SqlResult::Exec(exec_result) => {
                    let columns = vec![
                        Column::new("Status", "Status"),
                        Column::new("Rows Affected", "Rows Affected"),
                    ];
                    let rows = vec![vec![
                        exec_result.message.clone().unwrap_or_else(|| "Success".to_string()),
                        format!("{}", exec_result.rows_affected),
                    ]];
                    let delegate = ResultsDelegate::new(columns, rows);
                    let table = cx.new(|cx| TableState::new(delegate, window, cx));
                    new_tabs.push(SqlResultTab {
                        sql: sql_text,
                        result: result.clone(),
                        execution_time: format!("{}ms", exec_result.elapsed_ms),
                        rows_count: format!("{} rows affected", exec_result.rows_affected),
                        table,
                    });
                }
                SqlResult::Error(error) => {
                    let columns = vec![Column::new("Error", "Error")];
                    let rows = vec![vec![error.message.clone()]];
                    let delegate = ResultsDelegate::new(columns, rows);
                    let table = cx.new(|cx| TableState::new(delegate, window, cx));
                    new_tabs.push(SqlResultTab {
                        sql: sql_text,
                        result: result.clone(),
                        execution_time: "Error".to_string(),
                        rows_count: "Error".to_string(),
                        table,
                    });
                }
            }
        }

        // Update result tabs
        if let Ok(mut tabs) = self.result_tabs.write() {
            *tabs = new_tabs;
        }
        // Reset active tab to summary
        if let Ok(mut active) = self.active_result_tab.write() {
            *active = 0;
        }

        cx.notify();
    }

}

impl Render for SqlResultTabContainer {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tabs = self.result_tabs.read().unwrap();
        let active_idx = *self.active_result_tab.read().unwrap();

        if tabs.is_empty() {
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
                    // Tab bar for result tabs (摘要 + individual results)
                    TabBar::new("result-tabs")
                        .w_full()
                        .pill()
                        .with_size(Size::Small)
                        .selected_index(active_idx)
                        .on_click({
                            let active_tab = self.active_result_tab.clone();
                            move |_ix: &usize, _w, _cx| {
                                *active_tab.write().unwrap() = *_ix;
                            }
                        })
                        .child(
                            // Summary tab
                            Tab::new().label("摘要")
                        )
                        .children(tabs.iter().enumerate().map(|(idx, tab)| {
                            Tab::new().label(format!("结果{} ({}, {})", idx + 1, tab.rows_count, tab.execution_time))
                        }))
                )
                .child(
                    // Active tab content
                    v_flex()
                        .flex_1()
                        .bg(cx.theme().background)
                        .border_1()
                        .border_color(cx.theme().border)
                        .rounded_md()
                        .overflow_hidden()
                        .child(
                            if active_idx == 0 {
                                // Show summary view
                                render_summary_view(&tabs, cx)
                            } else {
                                // Show individual result table
                                tabs.get(active_idx - 1)
                                    .map(|tab| {
                                        div()
                                            .size_full()
                                            .child(Table::new(&tab.table.clone()))
                                            .into_any_element()
                                    })
                                    .unwrap_or_else(|| div().into_any_element())
                            }
                        )
                )
        }
    }
}

// Render summary view function
fn render_summary_view(tabs: &[SqlResultTab], cx: &App) -> AnyElement {
    let mut total_rows = 0;
    let mut total_time = 0.0;
    let mut success_count = 0;
    let mut error_count = 0;

    for tab in tabs {
        match &tab.result {
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
                    let (status_icon, status_color, status_text) = match &tab.result {
                        SqlResult::Query(q) => (
                            IconName::Check,
                            cx.theme().success,
                            format!("{} rows", q.rows.len())
                        ),
                        SqlResult::Exec(e) => (
                            IconName::Check,
                            cx.theme().success,
                            format!("{} rows affected", e.rows_affected)
                        ),
                        SqlResult::Error(e) => (
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
                                        .child(format!("语句{}: {}", idx + 1, tab.sql))
                                )
                                .child(
                                    // Execution time
                                    div()
                                        .flex_shrink_0()
                                        .text_xs()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(tab.execution_time.clone())
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
