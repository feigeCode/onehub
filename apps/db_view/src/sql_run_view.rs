// 1. 标准库导入
// (无)

// 2. 外部 crate 导入（按字母顺序）
use gpui::{
    div, App, AppContext, AsyncApp, ClickEvent, Context, Entity, FocusHandle, Focusable,
    IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window,
};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

// 3. 当前 crate 导入（按模块分组）
use db::{ExecOptions, GlobalDbState, SqlResult, StreamingProgress};

pub struct SqlRunView {
    connection_id: String,
    database: Option<String>,
    file_path: Entity<InputState>,
    pending_file_path: Entity<Option<String>>,
    stop_on_error: Entity<bool>,
    use_transaction: Entity<bool>,
    status: Entity<String>,
    focus_handle: FocusHandle,
}

impl SqlRunView {
    pub fn new(
        connection_id: impl Into<String>,
        database: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            Self {
                connection_id: connection_id.into(),
                database,
                file_path: cx.new(|cx| InputState::new(window, cx)),
                pending_file_path: cx.new(|_| None),
                stop_on_error: cx.new(|_| true),
                use_transaction: cx.new(|_| false),
                status: cx.new(|_| String::new()),
                focus_handle: cx.focus_handle(),
            }
        })
    }

    fn update_status(cx: &AsyncApp, status: &Entity<String>, message: &str) {
        let _ = cx.update(|cx| {
            status.update(cx, |s, cx| {
                *s = message.to_string();
                cx.notify();
            });
        });
    }

    fn select_file(&mut self, _window: &mut Window, cx: &mut App) {
        let pending = self.pending_file_path.clone();
        let status = self.status.clone();
        let future = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            multiple: true,
            directories: false,
            prompt: Some("选择SQL文件".into()),
        });

        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                let mut path = String::new();
                for (i, path_buf) in paths.iter().enumerate() {
                    path.push_str(path_buf.to_str().unwrap_or(""));
                    if i < paths.len() - 1 {
                        path.push(';');
                    }
                }
                let _ = cx.update(|cx| {
                    pending.update(cx, |p, cx| {
                        *p = Some(path.clone());
                        cx.notify();
                    });
                    status.update(cx, |s, cx| {
                        *s = format!("已选择: {}", path);
                        cx.notify();
                    });
                });
            }
        })
        .detach();
    }

    fn start_run(&mut self, _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let database = self.database.clone();
        let file_path_str = self.file_path.read(cx).text().to_string();
        let stop_on_error = *self.stop_on_error.read(cx);
        let transactional = *self.use_transaction.read(cx);
        let status = self.status.clone();
        if file_path_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "请选择SQL文件".to_string();
                cx.notify();
            });
            return;
        }

        status.update(cx, |s, cx| {
            *s = "正在执行...".to_string();
            cx.notify();
        });

        cx.spawn(async move |cx: &mut AsyncApp| {
            let files: Vec<&str> = file_path_str.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();

            let mut total_success = 0;
            let mut total_errors = 0;
            let mut error_messages = Vec::new();

            for file_path in files {
                let sql_content = match std::fs::read_to_string(file_path) {
                    Ok(content) => content,
                    Err(e) => {
                        let error_msg = format!("文件读取错误 [{}]: {}", file_path, e);
                        error_messages.push(error_msg.clone());
                        total_errors += 1;

                        if stop_on_error {
                            Self::update_status(&cx, &status, &error_msg);
                            return;
                        }
                        continue;
                    }
                };

                let conn_id = connection_id.clone();
                let opts = ExecOptions {
                    stop_on_error,
                    transactional,
                    max_rows: None,
                };

                let rx_result = global_state.execute_script_streaming(
                    cx,
                    conn_id,
                    sql_content,
                    database.clone(),
                    Some(opts),
                );

                let mut rx = match rx_result {
                    Ok(rx) => rx,
                    Err(e) => {
                        let error_msg = format!("执行失败 [{}]: {}", file_path, e);
                        error_messages.push(error_msg.clone());
                        total_errors += 1;

                        if stop_on_error {
                            Self::update_status(&cx, &status, &error_msg);
                            return;
                        }
                        continue;
                    }
                };

                while let Some(progress) = rx.recv().await {
                    let is_error = progress.result.is_error();

                    if is_error {
                        if let SqlResult::Error(e) = &progress.result {
                            error_messages.push(format!("[{}]: {}", file_path, e.message));
                        }
                        total_errors += 1;
                    } else {
                        total_success += 1;
                    }

                    let status_msg = Self::format_progress_status(
                        file_path,
                        &progress,
                        total_success,
                        total_errors,
                    );
                    Self::update_status(&cx, &status, &status_msg);

                    if is_error && stop_on_error {
                        let error_msg = format!(
                            "执行错误 [{}/{}]: {}",
                            progress.current,
                            progress.total,
                            error_messages.last().unwrap_or(&"未知错误".to_string())
                        );
                        Self::update_status(&cx, &status, &error_msg);
                        return;
                    }
                }
            }

            let final_message = if total_errors == 0 {
                format!("执行完成: {} 条语句全部成功", total_success)
            } else {
                let error_summary = if error_messages.len() <= 3 {
                    error_messages.join("\n")
                } else {
                    format!(
                        "{}...\n(共{}个错误)",
                        error_messages[..3].join("\n"),
                        error_messages.len()
                    )
                };
                format!(
                    "执行完成: {} 条成功, {} 条失败\n错误详情:\n{}",
                    total_success, total_errors, error_summary
                )
            };

            Self::update_status(&cx, &status, &final_message);
        }).detach();
    }

    fn format_progress_status(
        file_path: &str,
        progress: &StreamingProgress,
        total_success: usize,
        total_errors: usize,
    ) -> String {
        let result_indicator = if progress.result.is_error() { "✗" } else { "✓" };
        format!(
            "[{}] 执行进度: {}/{} {} | 成功: {} 失败: {}",
            file_path,
            progress.current,
            progress.total,
            result_indicator,
            total_success,
            total_errors
        )
    }
}

impl Focusable for SqlRunView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for SqlRunView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            database: self.database.clone(),
            file_path: self.file_path.clone(),
            pending_file_path: self.pending_file_path.clone(),
            stop_on_error: self.stop_on_error.clone(),
            use_transaction: self.use_transaction.clone(),
            status: self.status.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for SqlRunView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 检查是否有待更新的文件路径
        if let Some(path) = self.pending_file_path.read(cx).clone() {
            self.file_path.update(cx, |state, cx| {
                state.replace(path, window, cx);
            });
            self.pending_file_path.update(cx, |p, _| *p = None);
        }

        let status_text = self.status.read(cx).clone();

        v_flex()
            .gap_3()
            .p_4()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("SQL文件:"))
                    .child(Input::new(&self.file_path).w_full())
                    .child(
                        Button::new("select_file")
                            .small()
                            .child("浏览")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.select_file(window, cx);
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_4()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("stop_on_error")
                                    .checked(*self.stop_on_error.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.stop_on_error.update(cx, |value, cx| {
                                            *value = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("遇错停止"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("use_transaction")
                                    .checked(*self.use_transaction.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.use_transaction.update(cx, |value, cx| {
                                            *value = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("使用事务"),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("run")
                            .primary()
                            .child("执行")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.start_run(window, cx);
                            })),
                    )
                    .child(
                        Button::new("clear_status")
                            .small()
                            .child("清除状态")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, _window, cx| {
                                view.status.update(cx, |s, cx| {
                                    s.clear();
                                    cx.notify();
                                });
                            })),
                    ),
            )
            .child(
                div()
                    .mt_4()
                    .p_2()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded_md()
                    .min_h_16()
                    .child(status_text),
            )
    }
}
