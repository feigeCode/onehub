use gpui::{div, px, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, InteractiveElement, IntoElement, ParentElement, Render, StatefulInteractiveElement, Styled, Window, prelude::FluentBuilder};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex, v_flex, ActiveTheme, WindowExt, VirtualListScrollHandle,
};

use db::{DataFormat, ExportConfig, ExportProgressEvent, GlobalDbState};
use crate::db_tree_view::SqlDumpMode;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct LogEntry {
    table: String,
    message: String,
}

pub struct SqlDumpView {
    connection_id: String,
    server_info: String,
    database: String,
    output_path: PathBuf,
    mode: SqlDumpMode,
    tables: Vec<String>,

    logs: Entity<Vec<LogEntry>>,
    scroll_handle: VirtualListScrollHandle,

    processed_records: Entity<u64>,
    error_count: Entity<u32>,
    transferred_records: Entity<u64>,
    elapsed_time: Entity<String>,
    progress: Entity<f32>,

    is_running: Entity<bool>,
    is_finished: Entity<bool>,
    start_time: Option<Instant>,

    focus_handle: FocusHandle,
}

impl SqlDumpView {
    pub fn new(
        connection_id: impl Into<String>,
        server_info: impl Into<String>,
        database: impl Into<String>,
        output_path: PathBuf,
        tables: Vec<String>,
        mode: SqlDumpMode,
        _window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            Self {
                connection_id: connection_id.into(),
                server_info: server_info.into(),
                database: database.into(),
                output_path,
                mode,
                tables,

                logs: cx.new(|_| Vec::new()),
                scroll_handle: VirtualListScrollHandle::new(),

                processed_records: cx.new(|_| 0),
                error_count: cx.new(|_| 0),
                transferred_records: cx.new(|_| 0),
                elapsed_time: cx.new(|_| "0.00s".to_string()),
                progress: cx.new(|_| 0.0),

                is_running: cx.new(|_| false),
                is_finished: cx.new(|_| false),
                start_time: None,

                focus_handle: cx.focus_handle(),
            }
        })
    }

    fn start_dump(&mut self, _window: &mut Window, cx: &mut App) {
        if *self.is_running.read(cx) {
            return;
        }

        self.is_running.update(cx, |r, cx| {
            *r = true;
            cx.notify();
        });

        self.start_time = Some(Instant::now());

        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let database = self.database.clone();
        let tables = self.tables.clone();
        let output_path = self.output_path.clone();

        let (include_structure, include_data) = match self.mode {
            SqlDumpMode::StructureOnly => (true, false),
            SqlDumpMode::DataOnly => (false, true),
            SqlDumpMode::StructureAndData => (true, true),
        };

        let logs = self.logs.clone();
        let scroll_handle = self.scroll_handle.clone();
        let processed_records = self.processed_records.clone();
        let error_count = self.error_count.clone();
        let transferred_records = self.transferred_records.clone();
        let elapsed_time = self.elapsed_time.clone();
        let progress = self.progress.clone();
        let is_running = self.is_running.clone();
        let is_finished = self.is_finished.clone();
        let start_time = self.start_time;

        cx.spawn(async move |cx| {
            let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ExportProgressEvent>();

            let export_config = ExportConfig {
                format: DataFormat::Sql,
                database: database.clone(),
                tables: tables.clone(),
                include_schema: include_structure,
                include_data,
                where_clause: None,
                limit: None,
            };

            let global_state_clone = global_state.clone();
            let connection_id_clone = connection_id.clone();

            let export_handle = cx.background_spawn(async move {
                global_state_clone
                    .export_data_with_progress_sync(connection_id_clone, export_config, Some(progress_tx))
                    .await
            });

            while let Some(event) = progress_rx.recv().await {
                let event_clone = event.clone();
                let logs_clone = logs.clone();
                let scroll_handle_clone = scroll_handle.clone();
                let processed_records_clone = processed_records.clone();
                let error_count_clone = error_count.clone();
                let transferred_records_clone = transferred_records.clone();
                let elapsed_time_clone = elapsed_time.clone();
                let progress_clone = progress.clone();

                let _ = cx.update(|cx| {
                    let elapsed = start_time
                        .map(|t| t.elapsed().as_secs_f64())
                        .unwrap_or(0.0);

                    elapsed_time_clone.update(cx, |t, cx| {
                        *t = format!("{:.2}s", elapsed);
                        cx.notify();
                    });

                    match event_clone {
                        ExportProgressEvent::TableStart { table, table_index, total_tables } => {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: format!("Starting ({}/{})", table_index + 1, total_tables),
                                });
                                cx.notify();
                            });
                            let p = (table_index as f32 / total_tables as f32) * 100.0;
                            progress_clone.update(cx, |pr, cx| {
                                *pr = p;
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::GettingStructure { table } => {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: "Getting table structure".to_string(),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::StructureExported { table } => {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: "Create table".to_string(),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::FetchingData { table } => {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: "Fetching records".to_string(),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::DataExported { table, rows } => {
                            transferred_records_clone.update(cx, |r, cx| {
                                *r += rows;
                                cx.notify();
                            });
                            processed_records_clone.update(cx, |r, cx| {
                                *r += rows;
                                cx.notify();
                            });
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: format!("Transferring records ({})", rows),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::TableFinished { table } => {
                            let elapsed = start_time
                                .map(|t| t.elapsed().as_secs_f64())
                                .unwrap_or(0.0);
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: format!("Finished ({:.3} s)", elapsed),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::Error { table, message } => {
                            error_count_clone.update(cx, |e, cx| {
                                *e += 1;
                                cx.notify();
                            });
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: table.clone(),
                                    message: format!("Error: {}", message),
                                });
                                cx.notify();
                            });
                        }
                        ExportProgressEvent::Finished { total_rows, elapsed_ms } => {
                            progress_clone.update(cx, |p, cx| {
                                *p = 100.0;
                                cx.notify();
                            });
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: "".to_string(),
                                    message: format!("Export completed: {} rows in {}ms", total_rows, elapsed_ms),
                                });
                                cx.notify();
                            });
                        }
                    }

                    scroll_handle_clone.scroll_to_bottom();
                });
            }

            let result = export_handle.await;

            let _ = cx.update(|cx| {
                is_running.update(cx, |r, cx| {
                    *r = false;
                    cx.notify();
                });
                is_finished.update(cx, |f, cx| {
                    *f = true;
                    cx.notify();
                });

                match result {
                    Ok(export_result) => {
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let filename = format!("{}_{}.sql", database, timestamp);
                        let full_path = output_path.join(&filename);

                        if let Err(e) = std::fs::write(&full_path, export_result.output) {
                            logs.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: "".to_string(),
                                    message: format!("File write error: {}", e),
                                });
                                cx.notify();
                            });
                            error_count.update(cx, |e, cx| {
                                *e += 1;
                                cx.notify();
                            });
                        } else {
                            logs.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    table: "".to_string(),
                                    message: format!("File saved: {}", full_path.display()),
                                });
                                cx.notify();
                            });
                        }
                    }
                    Err(e) => {
                        logs.update(cx, |l, cx| {
                            l.push(LogEntry {
                                table: "".to_string(),
                                message: format!("Export error: {}", e),
                            });
                            cx.notify();
                        });
                        error_count.update(cx, |e, cx| {
                            *e += 1;
                            cx.notify();
                        });
                    }
                }

                scroll_handle.scroll_to_bottom();
            });
        }).detach();
    }
}

impl Focusable for SqlDumpView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for SqlDumpView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            server_info: self.server_info.clone(),
            database: self.database.clone(),
            output_path: self.output_path.clone(),
            mode: self.mode.clone(),
            tables: self.tables.clone(),
            logs: self.logs.clone(),
            scroll_handle: self.scroll_handle.clone(),
            processed_records: self.processed_records.clone(),
            error_count: self.error_count.clone(),
            transferred_records: self.transferred_records.clone(),
            elapsed_time: self.elapsed_time.clone(),
            progress: self.progress.clone(),
            is_running: self.is_running.clone(),
            is_finished: self.is_finished.clone(),
            start_time: self.start_time,
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for SqlDumpView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_running = *self.is_running.read(cx);
        let is_finished = *self.is_finished.read(cx);
        let progress_value = *self.progress.read(cx);
        let processed = *self.processed_records.read(cx);
        let errors = *self.error_count.read(cx);
        let transferred = *self.transferred_records.read(cx);
        let elapsed = self.elapsed_time.read(cx).clone();
        let logs = self.logs.read(cx).clone();

        v_flex()
            .w_full()
            .h(px(450.0))
            .gap_3()
            .p_4()
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().w_24().text_color(cx.theme().muted_foreground).child("服务器:"))
                            .child(div().child(self.server_info.clone())),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().w_24().text_color(cx.theme().muted_foreground).child("数据库:"))
                            .child(div().child(self.database.clone())),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().w_24().text_color(cx.theme().muted_foreground).child("转储到:"))
                            .child(div().child(self.output_path.display().to_string())),
                    ),
            )
            .child(
                div()
                    .h_px()
                    .bg(cx.theme().border)
            )
            .child(
                h_flex()
                    .gap_6()
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().text_color(cx.theme().muted_foreground).child("已处理记录:"))
                            .child(div().child(processed.to_string())),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().text_color(cx.theme().muted_foreground).child("错误:"))
                            .child(div().child(errors.to_string())),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().text_color(cx.theme().muted_foreground).child("已传输记录:"))
                            .child(div().child(transferred.to_string())),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().text_color(cx.theme().muted_foreground).child("时间:"))
                            .child(div().child(elapsed)),
                    ),
            )
            .child(
                div()
                    .h_px()
                    .bg(cx.theme().border)
            )
            .child(
                div()
                    .id("logs-container")
                    .flex_1()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded_md()
                    .overflow_y_scroll()
                    .track_scroll(&self.scroll_handle)
                    .bg(cx.theme().background)
                    .p_2()
                    .children(
                        logs.iter().enumerate().map(|(idx, entry)| {
                            let text = if entry.table.is_empty() {
                                format!("[DMP] {}", entry.message)
                            } else {
                                format!("[DMP] {}> {}", entry.table, entry.message)
                            };
                            div()
                                .id(("log-entry", idx))
                                .text_xs()
                                .py_0p5()
                                .child(text)
                        })
                    ),
            )
            .child(
                div()
                    .h_2()
                    .w_full()
                    .rounded_full()
                    .bg(cx.theme().primary.opacity(0.2))
                    .child(
                        div()
                            .h_full()
                            .rounded_full()
                            .bg(cx.theme().primary)
                            .w(gpui::relative(progress_value / 100.0))
                    ),
            )
            .child(
                h_flex()
                    .pt_2()
                    .gap_2()
                    .justify_end()
                    .when(!is_running && !is_finished, |this| {
                        this.child(
                            Button::new("start")
                                .primary()
                                .child("开始")
                                .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                    view.start_dump(window, cx);
                                }))
                        )
                    })
                    .when(is_running, |this| {
                        this.child(
                            Button::new("running")
                                .loading(true)
                                .child("导出中...")
                        )
                    })
                    .when(is_finished, |this| {
                        this.child(
                            Button::new("close")
                                .child("关闭")
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                        )
                    }),
            )
    }
}
