use gpui::{div, px, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, InteractiveElement, IntoElement, ParentElement, Render, StatefulInteractiveElement, Styled, Window, prelude::FluentBuilder};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex, v_flex, ActiveTheme, WindowExt, VirtualListScrollHandle,
};

use db::{DataFormat, ImportConfig, ImportProgressEvent, GlobalDbState};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct LogEntry {
    file: String,
    message: String,
}

pub struct SqlImportView {
    connection_id: String,
    server_info: String,
    database: String,
    file_paths: Vec<PathBuf>,

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

impl SqlImportView {
    pub fn new(
        connection_id: impl Into<String>,
        server_info: impl Into<String>,
        database: impl Into<String>,
        file_paths: Vec<PathBuf>,
        _window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            Self {
                connection_id: connection_id.into(),
                server_info: server_info.into(),
                database: database.into(),
                file_paths,

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

    fn start_import(&mut self, _window: &mut Window, cx: &mut App) {
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
        let file_paths = self.file_paths.clone();

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
            let total_files = file_paths.len();
            let mut total_rows_imported = 0u64;
            let mut total_errors = 0u32;

            for (file_index, file_path) in file_paths.iter().enumerate() {
                let file_name = file_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                let logs_clone = logs.clone();
                let scroll_handle_clone = scroll_handle.clone();
                let file_name_clone = file_name.clone();
                let _ = cx.update(|cx| {
                    logs_clone.update(cx, |l, cx| {
                        l.push(LogEntry {
                            file: file_name_clone.clone(),
                            message: format!("Starting ({}/{})", file_index + 1, total_files),
                        });
                        cx.notify();
                    });

                    let p = (file_index as f32 / total_files as f32) * 100.0;
                    progress.update(cx, |pr, cx| {
                        *pr = p;
                        cx.notify();
                    });

                    scroll_handle_clone.scroll_to_bottom();
                });

                let logs_clone = logs.clone();
                let scroll_handle_clone = scroll_handle.clone();
                let file_name_clone = file_name.clone();
                let _ = cx.update(|cx| {
                    logs_clone.update(cx, |l, cx| {
                        l.push(LogEntry {
                            file: file_name_clone,
                            message: "Reading file...".to_string(),
                        });
                        cx.notify();
                    });
                    scroll_handle_clone.scroll_to_bottom();
                });

                let data = match std::fs::read_to_string(&file_path) {
                    Ok(d) => d,
                    Err(e) => {
                        let logs_clone = logs.clone();
                        let scroll_handle_clone = scroll_handle.clone();
                        let error_count_clone = error_count.clone();
                        let file_name_clone = file_name.clone();
                        let _ = cx.update(|cx| {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    file: file_name_clone,
                                    message: format!("Error: Failed to read file: {}", e),
                                });
                                cx.notify();
                            });
                            error_count_clone.update(cx, |e, cx| {
                                *e += 1;
                                cx.notify();
                            });
                            scroll_handle_clone.scroll_to_bottom();
                        });
                        total_errors += 1;
                        continue;
                    }
                };

                let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ImportProgressEvent>();

                let import_config = ImportConfig {
                    format: DataFormat::Sql,
                    database: database.clone(),
                    table: None,
                    stop_on_error: false,
                    use_transaction: false,
                    truncate_before_import: false,
                    csv_config: None,
                };

                let global_state_clone = global_state.clone();
                let connection_id_clone = connection_id.clone();
                let file_name_for_import = file_name.clone();

                let import_handle = cx.background_spawn(async move {
                    global_state_clone
                        .import_data_with_progress_sync(
                            connection_id_clone,
                            import_config,
                            data,
                            &file_name_for_import,
                            Some(progress_tx),
                        )
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

                    let _ = cx.update(|cx| {
                        let elapsed = start_time
                            .map(|t| t.elapsed().as_secs_f64())
                            .unwrap_or(0.0);

                        elapsed_time_clone.update(cx, |t, cx| {
                            *t = format!("{:.2}s", elapsed);
                            cx.notify();
                        });

                        match event_clone {
                            ImportProgressEvent::ParsingFile { file } => {
                                logs_clone.update(cx, |l, cx| {
                                    l.push(LogEntry {
                                        file: file.clone(),
                                        message: "Parsing SQL statements...".to_string(),
                                    });
                                    cx.notify();
                                });
                            }
                            ImportProgressEvent::ExecutingStatement { file, statement_index, total_statements } => {
                                if statement_index % 100 == 0 || statement_index == total_statements - 1 {
                                    logs_clone.update(cx, |l, cx| {
                                        l.push(LogEntry {
                                            file: file.clone(),
                                            message: format!("Executing statement ({}/{})", statement_index + 1, total_statements),
                                        });
                                        cx.notify();
                                    });
                                }
                            }
                            ImportProgressEvent::StatementExecuted { file: _, rows_affected } => {
                                transferred_records_clone.update(cx, |r, cx| {
                                    *r += rows_affected;
                                    cx.notify();
                                });
                                processed_records_clone.update(cx, |r, cx| {
                                    *r += rows_affected;
                                    cx.notify();
                                });
                            }
                            ImportProgressEvent::FileFinished { file, rows_imported } => {
                                logs_clone.update(cx, |l, cx| {
                                    l.push(LogEntry {
                                        file: file.clone(),
                                        message: format!("Finished ({} rows imported)", rows_imported),
                                    });
                                    cx.notify();
                                });
                            }
                            ImportProgressEvent::Error { file, message } => {
                                error_count_clone.update(cx, |e, cx| {
                                    *e += 1;
                                    cx.notify();
                                });
                                logs_clone.update(cx, |l, cx| {
                                    l.push(LogEntry {
                                        file: file.clone(),
                                        message: format!("Error: {}", message),
                                    });
                                    cx.notify();
                                });
                            }
                            _ => {}
                        }

                        scroll_handle_clone.scroll_to_bottom();
                    });
                }

                match import_handle.await {
                    Ok(result) => {
                        total_rows_imported += result.rows_imported;
                        total_errors += result.errors.len() as u32;
                    }
                    Err(e) => {
                        let logs_clone = logs.clone();
                        let scroll_handle_clone = scroll_handle.clone();
                        let error_count_clone = error_count.clone();
                        let file_name_clone = file_name.clone();
                        let _ = cx.update(|cx| {
                            logs_clone.update(cx, |l, cx| {
                                l.push(LogEntry {
                                    file: file_name_clone,
                                    message: format!("Error: {}", e),
                                });
                                cx.notify();
                            });
                            error_count_clone.update(cx, |e, cx| {
                                *e += 1;
                                cx.notify();
                            });
                            scroll_handle_clone.scroll_to_bottom();
                        });
                        total_errors += 1;
                    }
                }
            }

            let _ = cx.update(|cx| {
                is_running.update(cx, |r, cx| {
                    *r = false;
                    cx.notify();
                });
                is_finished.update(cx, |f, cx| {
                    *f = true;
                    cx.notify();
                });

                progress.update(cx, |p, cx| {
                    *p = 100.0;
                    cx.notify();
                });

                let elapsed = start_time
                    .map(|t| t.elapsed().as_millis())
                    .unwrap_or(0);

                logs.update(cx, |l, cx| {
                    l.push(LogEntry {
                        file: "".to_string(),
                        message: format!(
                            "Import completed: {} rows imported, {} errors in {}ms",
                            total_rows_imported, total_errors, elapsed
                        ),
                    });
                    cx.notify();
                });

                scroll_handle.scroll_to_bottom();
            });
        }).detach();
    }
}

impl Focusable for SqlImportView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for SqlImportView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            server_info: self.server_info.clone(),
            database: self.database.clone(),
            file_paths: self.file_paths.clone(),
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

impl Render for SqlImportView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_running = *self.is_running.read(cx);
        let is_finished = *self.is_finished.read(cx);
        let progress_value = *self.progress.read(cx);
        let processed = *self.processed_records.read(cx);
        let errors = *self.error_count.read(cx);
        let transferred = *self.transferred_records.read(cx);
        let elapsed = self.elapsed_time.read(cx).clone();
        let logs = self.logs.read(cx).clone();

        let files_display = self.file_paths.iter()
            .map(|p| p.file_name().and_then(|n| n.to_str()).unwrap_or("unknown"))
            .collect::<Vec<_>>()
            .join(", ");

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
                            .child(div().w_24().text_color(cx.theme().muted_foreground).child("文件:"))
                            .child(div().overflow_hidden().text_ellipsis().child(files_display)),
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
                            let text = if entry.file.is_empty() {
                                format!("[IMP] {}", entry.message)
                            } else {
                                format!("[IMP] {}> {}", entry.file, entry.message)
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
                                    view.start_import(window, cx);
                                }))
                        )
                    })
                    .when(is_running, |this| {
                        this.child(
                            Button::new("running")
                                .loading(true)
                                .child("导入中...")
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
