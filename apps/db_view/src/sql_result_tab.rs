use std::sync::Arc;
// 2. 外部 crate 导入（按字母顺序）
use gpui::{div, px, AnyElement, App, AppContext, AsyncApp, Context, Entity, InteractiveElement, IntoElement, ParentElement, Render, SharedString, StatefulInteractiveElement, Styled, Task, Window};
use gpui::prelude::FluentBuilder;
use tracing::log::error;
use gpui_component::{button::ButtonVariants, h_flex, list::{List, ListDelegate, ListItem, ListState}, progress::Progress, tab::{Tab, TabBar}, table::Column, v_flex, ActiveTheme, IconName, IndexPath, Sizable, Size, StyledExt};

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

/// 执行状态
#[derive(Clone, Debug, PartialEq)]
pub enum ExecutionState {
    Idle,
    Executing { current: usize, total: usize },
    Completed,
}

/// 语句列表项 - 用于虚拟滚动列表
#[derive(Clone)]
pub struct StatementListItem {
    pub idx: usize,
    pub sql: String,
    pub elapsed_ms: u128,
    pub is_error: bool,
    pub status_text: String,
}

/// 语句列表委托 - 实现虚拟滚动
pub struct StatementListDelegate {
    all_items: Vec<StatementListItem>,
    filtered_items: Vec<StatementListItem>,
    selected_index: Option<IndexPath>,
    show_errors_only: bool,
}

impl StatementListDelegate {
    pub fn new() -> Self {
        Self {
            all_items: Vec::new(),
            filtered_items: Vec::new(),
            selected_index: None,
            show_errors_only: false,
        }
    }

    pub fn set_items(&mut self, results: &[SqlResult]) {
        self.all_items = results.iter().enumerate().map(|(idx, result)| {
            match result {
                SqlResult::Query(q) => StatementListItem {
                    idx,
                    sql: q.sql.clone(),
                    elapsed_ms: q.elapsed_ms,
                    is_error: false,
                    status_text: format!("{} rows", q.rows.len()),
                },
                SqlResult::Exec(e) => StatementListItem {
                    idx,
                    sql: e.sql.clone(),
                    elapsed_ms: e.elapsed_ms,
                    is_error: false,
                    status_text: format!("{} rows affected", e.rows_affected),
                },
                SqlResult::Error(e) => StatementListItem {
                    idx,
                    sql: e.sql.clone(),
                    elapsed_ms: 0,
                    is_error: true,
                    status_text: e.message.clone(),
                },
            }
        }).collect();
        self.apply_filter();
    }

    pub fn set_show_errors_only(&mut self, show_errors_only: bool) {
        self.show_errors_only = show_errors_only;
        self.apply_filter();
    }

    fn apply_filter(&mut self) {
        if self.show_errors_only {
            self.filtered_items = self.all_items.iter().filter(|item| item.is_error).cloned().collect();
        } else {
            self.filtered_items = self.all_items.clone();
        }
    }

    pub fn clear(&mut self) {
        self.all_items.clear();
        self.filtered_items.clear();
        self.selected_index = None;
        self.show_errors_only = false;
    }
}

impl ListDelegate for StatementListDelegate {
    type Item = ListItem;

    fn sections_count(&self, _cx: &App) -> usize {
        1
    }

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.filtered_items.len()
    }

    fn perform_search(&mut self, _query: &str, _window: &mut Window, _cx: &mut Context<ListState<Self>>) -> Task<()> {
        Task::ready(())
    }

    fn confirm(&mut self, _secondary: bool, _window: &mut Window, _cx: &mut Context<ListState<Self>>) {}

    fn set_selected_index(&mut self, ix: Option<IndexPath>, _window: &mut Window, cx: &mut Context<ListState<Self>>) {
        self.selected_index = ix;
        cx.notify();
    }

    fn render_item(&mut self, ix: IndexPath, _window: &mut Window, cx: &mut Context<ListState<Self>>) -> Option<Self::Item> {
        let item = self.filtered_items.get(ix.row)?;
        let selected = Some(ix) == self.selected_index;

        let status_color = if item.is_error {
            cx.theme().danger
        } else {
            cx.theme().success
        };

        let full_sql = item.sql.clone();
        let sql_display = item.sql.replace('\n', " ").replace('\r', "");
        let status_text = item.status_text.clone();
        let status_text_for_tooltip = item.status_text.clone();

        Some(
            ListItem::new(ix)
                .h(px(36.))
                .py_2()
                .px_3()
                .selected(selected)
                .child(
                    h_flex()
                        .items_center()
                        .gap_4()
                        .w_full()
                        .h_full()
                        .child(
                            div()
                                .id(SharedString::from(format!("sql-{}", ix.row)))
                                .w(px(300.))
                                .flex_shrink_0()
                                .text_sm()
                                .text_color(status_color)
                                .overflow_hidden()
                                .whitespace_nowrap()
                                .text_ellipsis()
                                .tooltip(move |window, cx| gpui_component::tooltip::Tooltip::new(full_sql.clone()).build(window, cx))
                                .child(sql_display)
                        )
                        .child(
                            div()
                                .id(SharedString::from(format!("msg-{}", ix.row)))
                                .flex_1()
                                .min_w(px(0.))
                                .text_sm()
                                .text_color(status_color)
                                .overflow_hidden()
                                .whitespace_nowrap()
                                .text_ellipsis()
                                .tooltip(move |window, cx| gpui_component::tooltip::Tooltip::new(status_text_for_tooltip.clone()).build(window, cx))
                                .child(status_text)
                        )
                        .child(
                            div()
                                .w(px(80.))
                                .flex_shrink_0()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(format!("{:.3}s", item.elapsed_ms as f64 / 1000.0))
                        )
                )
        )
    }
}

#[derive(Clone)]
pub struct SqlResultTabContainer {
    pub result_tabs: Entity<Vec<SqlResultTab>>,
    pub active_result_tab: Entity<Arc<usize>>,
    pub all_results: Entity<Vec<SqlResult>>,
    pub is_visible: Entity<bool>,
    pub execution_state: Entity<ExecutionState>,
    pub statement_list: Entity<ListState<StatementListDelegate>>,
    pub show_errors_only: Entity<bool>,
    pub total_elapsed_ms: Entity<f64>,
}

impl SqlResultTabContainer {
    pub(crate) fn new(window: &mut Window, cx: &mut Context<Self>) -> SqlResultTabContainer {
        let result_tabs = cx.new(|_| vec![]);
        let active_result_tab = cx.new(|_| Arc::new(0));
        let all_results = cx.new(|_| vec![]);
        let is_visible = cx.new(|_| false);
        let execution_state = cx.new(|_| ExecutionState::Idle);
        let statement_list = cx.new(|cx| {
            ListState::new(StatementListDelegate::new(), window, cx)
        });
        let show_errors_only = cx.new(|_| false);
        let total_elapsed_ms = cx.new(|_| 0.0);
        SqlResultTabContainer {
            result_tabs,
            active_result_tab,
            all_results,
            is_visible,
            execution_state,
            statement_list,
            show_errors_only,
            total_elapsed_ms,
        }
    }
}

impl SqlResultTabContainer {

    pub fn handle_run_query(&mut self, sql: String, connection_id: String, current_database_value: Option<String>, _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let clone_self = self.clone();
        let connection_id_clone = connection_id.clone();
        let database_clone = current_database_value.clone();

        self.clear_results(cx);

        self.execution_state.update(cx, |state, cx| {
            *state = ExecutionState::Executing { current: 0, total: 0 };
            cx.notify();
        });

        self.active_result_tab.update(cx, |active, cx| {
            *active = Arc::new(0);
            cx.notify();
        });

        self.is_visible.update(cx, |visible, cx| {
            *visible = true;
            cx.notify();
        });

        let execution_start = std::time::Instant::now();

        cx.spawn(async move |cx: &mut AsyncApp| {
            let config = global_state.get_config_async(&connection_id).await;
            let database_type = config.map(|c| c.database_type).unwrap_or(one_core::storage::DatabaseType::MySQL);

            // 设置stop_on_error为false，确保即使某条语句失败也继续执行后续语句
            let exec_opts = db::ExecOptions {
                stop_on_error: false,
                ..Default::default()
            };
            let mut rx = match global_state
                .execute_script_streaming(cx, connection_id_clone.clone(), sql.clone(), current_database_value, Some(exec_opts))
            {
                Ok(receiver) => receiver,
                Err(e) => {
                    error!("Error starting streaming execution: {:?}", e);
                    let _ = cx.update(|cx| {
                        clone_self.execution_state.update(cx, |state, cx| {
                            *state = ExecutionState::Idle;
                            cx.notify();
                        });
                    });
                    return;
                }
            };

            let mut has_query_result = false;
            let mut first_query_index: Option<usize> = None;
            let mut query_count = 0usize;

            let mut pending_results: Vec<SqlResult> = Vec::new();
            let mut last_ui_update = std::time::Instant::now();
            const UI_UPDATE_INTERVAL_MS: u128 = 100;
            const BATCH_SIZE: usize = 50;

            loop {
                let progress = match rx.recv().await {
                    Some(p) => p,
                    None => break,
                };

                let (current, total) = (progress.current, progress.total);
                let result = progress.result;

                let is_query = matches!(&result, SqlResult::Query(_));
                if is_query {
                    if first_query_index.is_none() {
                        first_query_index = Some(query_count);
                    }
                    has_query_result = true;
                    query_count += 1;
                }

                pending_results.push(result);

                let should_update_list = pending_results.len() >= BATCH_SIZE
                    || last_ui_update.elapsed().as_millis() >= UI_UPDATE_INTERVAL_MS;

                if should_update_list && !pending_results.is_empty() {
                    let results_to_send = std::mem::take(&mut pending_results);
                    let _ = cx.update(|cx| {
                        if let Some(window_id) = cx.active_window() {
                            let _ = cx.update_window(window_id, |_entity, window, cx| {
                                clone_self.execution_state.update(cx, |state, cx| {
                                    *state = ExecutionState::Executing { current, total };
                                    cx.notify();
                                });

                                clone_self.add_streaming_results_batch(
                                    results_to_send,
                                    connection_id_clone.clone(),
                                    database_clone.clone(),
                                    database_type,
                                    window,
                                    cx,
                                );
                            });
                        }
                    });
                    last_ui_update = std::time::Instant::now();
                }
            }

            if !pending_results.is_empty() {
                let results_to_send = pending_results;
                let _ = cx.update(|cx| {
                    if let Some(window_id) = cx.active_window() {
                        let _ = cx.update_window(window_id, |_entity, window, cx| {
                            clone_self.add_streaming_results_batch(
                                results_to_send,
                                connection_id_clone.clone(),
                                database_clone.clone(),
                                database_type,
                                window,
                                cx,
                            );
                        });
                    }
                });
            }

            // 最终状态更新
            let total_elapsed = execution_start.elapsed().as_secs_f64();
            let _ = cx.update(|cx| {
                clone_self.execution_state.update(cx, |state, cx| {
                    *state = ExecutionState::Completed;
                    cx.notify();
                });

                clone_self.total_elapsed_ms.update(cx, |t, cx| {
                    *t = total_elapsed * 1000.0;
                    cx.notify();
                });

                if has_query_result {
                    if let Some(idx) = first_query_index {
                        clone_self.active_result_tab.update(cx, |active, cx| {
                            *active = Arc::new(idx + 1);
                            cx.notify();
                        });
                    }
                }
            });
        }).detach();
    }

    fn clear_results(&mut self, cx: &mut App) {
        self.result_tabs.update(cx, |tabs, cx| {
            tabs.clear();
            cx.notify();
        });
        self.all_results.update(cx, |results, cx| {
            results.clear();
            cx.notify();
        });
        self.statement_list.update(cx, |list, cx| {
            list.delegate_mut().clear();
            cx.notify();
        });
        self.total_elapsed_ms.update(cx, |t, cx| {
            *t = 0.0;
            cx.notify();
        });
        self.show_errors_only.update(cx, |s, cx| {
            *s = false;
            cx.notify();
        });
    }

    /// 批量添加streaming结果并滚动到最新位置
    fn add_streaming_results_batch(
        &self,
        results: Vec<SqlResult>,
        connection_id: String,
        database: Option<String>,
        database_type: one_core::storage::DatabaseType,
        _window: &mut Window,
        cx: &mut App,
    ) {
        let mut new_all_results = Vec::new();
        let mut new_tabs = Vec::new();
        let base_idx = self.all_results.read(cx).len();

        let global_state = cx.global::<GlobalDbState>().clone();
        let plugin = global_state.db_manager.get_plugin(&database_type).ok();

        for (i, result) in results.into_iter().enumerate() {
            let idx = base_idx + i;
            new_all_results.push(result.clone());

            if let SqlResult::Query(query_result) = result {
                let db_name = database.clone().unwrap_or_default();

                let (editable, table_name) = if let Some(ref plugin) = plugin {
                    match plugin.analyze_select_editability(&query_result.sql) {
                        Some(parsed_table_name) => (true, parsed_table_name),
                        None => (false, query_result.table_name.clone().unwrap_or_else(|| format!("result_{}", idx))),
                    }
                } else {
                    (query_result.editable, query_result.table_name.clone().unwrap_or_else(|| format!("result_{}", idx)))
                };

                let config = DataGridConfig::new(
                    db_name,
                    table_name,
                    &connection_id,
                    database_type,
                )
                .editable(editable)
                .show_toolbar(true)
                .usage(DataGridUsage::SqlResult)
                .sql(query_result.sql.clone());

                let data_grid = cx.new(|cx| DataGrid::new(config, _window, cx));

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

                data_grid.update(cx, |this, cx| {
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

                new_tabs.push(tab);
            }
        }

        let final_idx = base_idx + new_all_results.len().saturating_sub(1);

        self.all_results.update(cx, |all_results, _cx| {
            all_results.extend(new_all_results);
        });

        self.result_tabs.update(cx, |tabs, _cx| {
            tabs.extend(new_tabs);
        });

        let all_results_clone: Vec<SqlResult> = self.all_results.read(cx).clone();
        let statement_list = self.statement_list.clone();
        statement_list.update(cx, |list, cx| {
            list.delegate_mut().set_items(&all_results_clone);
            list.scroll_handle().scroll_to_item(final_idx, gpui::ScrollStrategy::Center);
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
        let execution_state = self.execution_state.read(cx).clone();
        let statement_list = self.statement_list.clone();
        let show_errors_only = *self.show_errors_only.read(cx);
        let total_elapsed_ms = *self.total_elapsed_ms.read(cx);

        if !is_visible {
            return div().size_full();
        }

        let is_executing = matches!(execution_state, ExecutionState::Executing { .. });

        if all_results.is_empty() && !is_executing {
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
            const MAX_VISIBLE_TABS: usize = 20;
            let visible_query_tabs: Vec<_> = query_tabs.iter().take(MAX_VISIBLE_TABS).collect();
            let has_more_tabs = query_tabs.len() > MAX_VISIBLE_TABS;

            v_flex()
                .size_full()
                .gap_0()
                .child(
                    h_flex()
                        .w_full()
                        .items_center()
                        .justify_center()
                        .child(
                            TabBar::new("result-tabs")
                                .underline()
                                .justify_center()
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
                                    Tab::new().label(match &execution_state {
                                        ExecutionState::Executing { current, total } => {
                                            format!("摘要 ({}/{})", current, total)
                                        }
                                        _ => "摘要".to_string()
                                    })
                                )
                                .children(visible_query_tabs.iter().enumerate().map(|(idx, tab)| {
                                    Tab::new().label(format!("结果{} ({}, {})", idx + 1, tab.rows_count, tab.execution_time))
                                }))
                                .when(has_more_tabs, |this| {
                                    this.child(Tab::new().label(format!("+{} more", query_tabs.len() - MAX_VISIBLE_TABS)))
                                })
                        )
                        .child(
                            div().flex_1()
                        )
                        .child(
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
                    if active_idx == 0 {
                        div()
                            .flex_1()
                            .bg(cx.theme().background)
                            .border_1()
                            .border_color(cx.theme().border)
                            .rounded_md()
                            .overflow_hidden()
                            .child(render_summary_view(
                                all_results,
                                &execution_state,
                                &statement_list,
                                show_errors_only,
                                total_elapsed_ms,
                                clone_self.clone(),
                                cx,
                            ))
                            .into_any_element()
                    } else {
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
fn render_summary_view(
    tabs: &Vec<SqlResult>,
    execution_state: &ExecutionState,
    statement_list: &Entity<ListState<StatementListDelegate>>,
    show_errors_only: bool,
    total_elapsed_ms: f64,
    container: SqlResultTabContainer,
    cx: &App,
) -> AnyElement {
    let mut success_count = 0;
    let mut error_count = 0;

    for tab in tabs {
        match tab {
            SqlResult::Query(_) => {
                success_count += 1;
            }
            SqlResult::Exec(_) => {
                success_count += 1;
            }
            SqlResult::Error(_) => {
                error_count += 1;
            }
        }
    }

    let is_executing = matches!(execution_state, ExecutionState::Executing { .. });
    let (current, total) = match execution_state {
        ExecutionState::Executing { current, total } => (*current, *total),
        _ => (0, 0),
    };

    v_flex()
        .size_full()
        .child(
            h_flex()
                .w_full()
                .p_4()
                .gap_8()
                .justify_between()
                .child(
                    h_flex()
                        .gap_8()
                        .child(
                            v_flex()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child("已处理的查询:")
                                )
                                .child(
                                    div()
                                        .text_lg()
                                        .font_semibold()
                                        .child(format!("{}", tabs.len()))
                                )
                        )
                        .child(
                            v_flex()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child("成功:")
                                )
                                .child(
                                    div()
                                        .text_lg()
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
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child("错误:")
                                )
                                .child(
                                    div()
                                        .text_lg()
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
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child("运行时间:")
                                )
                                .child(
                                    div()
                                        .text_lg()
                                        .font_semibold()
                                        .child(if is_executing {
                                            "执行中...".to_string()
                                        } else {
                                            format!("{:.3}s", total_elapsed_ms / 1000.0)
                                        })
                                )
                        )
                )
                .child(
                    h_flex()
                        .gap_2()
                        .items_center()
                        .child(
                            gpui_component::checkbox::Checkbox::new("show-errors-only")
                                .label("仅显示错误")
                                .checked(show_errors_only)
                                .on_click({
                                    let container = container.clone();
                                    move |checked, _, cx| {
                                        container.show_errors_only.update(cx, |s, cx| {
                                            *s = *checked;
                                            cx.notify();
                                        });
                                        container.statement_list.update(cx, |list, cx| {
                                            list.delegate_mut().set_show_errors_only(*checked);
                                            cx.notify();
                                        });
                                    }
                                })
                        )
                )
        )
        .when(is_executing && total > 0, |this| {
            let progress_percent = if total > 0 { (current as f32 / total as f32) * 100.0 } else { 0.0 };
            this.child(
                div()
                    .px_4()
                    .child(
                        Progress::new()
                            .h(px(4.))
                            .value(progress_percent)
                    )
            )
        })
        .child(
            div()
                .mx_4()
                .h(px(1.))
                .w_full()
                .bg(cx.theme().border)
        )
        .child(
            h_flex()
                .w_full()
                .px_4()
                .py_2()
                .gap_4()
                .bg(cx.theme().muted)
                .child(
                    div()
                        .w(px(300.))
                        .flex_shrink_0()
                        .text_sm()
                        .font_semibold()
                        .child("查询")
                )
                .child(
                    div()
                        .flex_1()
                        .text_sm()
                        .font_semibold()
                        .child("消息")
                )
                .child(
                    div()
                        .w(px(80.))
                        .flex_shrink_0()
                        .text_sm()
                        .font_semibold()
                        .child("执行时间")
                )
        )
        .child(
            List::new(statement_list)
                .flex_1()
                .w_full()
        )
        .into_any_element()
}
