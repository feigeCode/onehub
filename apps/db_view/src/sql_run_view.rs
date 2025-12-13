use gpui::{div, App, AppContext, AsyncApp, ClickEvent, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

use db::GlobalDbState;
use one_core::gpui_tokio::Tokio;

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
        let use_transaction = *self.use_transaction.read(cx);
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
            let files: Vec<String> = file_path_str.split(';').map(|s| s.to_string()).collect();
            let mut success_count = 0;
            let mut error_count = 0;
            let mut last_error = String::new();

            for file_path in files {
                let file_path = file_path.trim();
                if file_path.is_empty() {
                    continue;
                }

                // 读取文件
                let sql_content = match std::fs::read_to_string(file_path) {
                    Ok(content) => content,
                    Err(e) => {
                        let _ = cx.update(|cx| {
                            status.update(cx, |s, cx| {
                                *s = format!("文件读取错误: {}", e);
                                cx.notify();
                            });
                        });
                        if stop_on_error {
                            return;
                        }
                        continue;
                    }
                };

                // 分割SQL语句
                let statements: Vec<String> = sql_content
                    .split(';')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                let conn_id = connection_id.clone();
                let db = database.clone().unwrap_or("".to_string());
                let global = global_state.clone();

                let result = Tokio::spawn_result(cx, async move {
                    let (plugin, conn_arc) = global.get_plugin_and_connection(&conn_id).await?;
                    let conn = conn_arc.read().await;

                    if use_transaction {
                        plugin.execute_query(&**conn, &db, "BEGIN", None).await?;
                    }

                    let mut stmt_success = 0;
                    let mut stmt_error = 0;
                    let mut err_msg = String::new();

                    for stmt in &statements {
                        let sql = if stmt.ends_with(';') {
                            stmt.to_string()
                        } else {
                            format!("{};", stmt)
                        };

                        match plugin.execute_query(&**conn, &db, &sql, None).await {
                            Ok(_) => stmt_success += 1,
                            Err(e) => {
                                stmt_error += 1;
                                err_msg = e.to_string();
                                if stop_on_error {
                                    if use_transaction {
                                        let _ = plugin.execute_query(&**conn, &db, "ROLLBACK", None).await;
                                    }
                                    anyhow::bail!(err_msg);
                                }
                            }
                        }
                    }

                    if use_transaction {
                        plugin.execute_query(&**conn, &db, "COMMIT", None).await?;
                    }

                    Ok((stmt_success, stmt_error, err_msg))
                }).unwrap().await;

                match result {
                    Ok((s, e, err)) => {
                        success_count += s;
                        error_count += e;
                        if !err.is_empty() {
                            last_error = err;
                        }
                    }
                    Err(e) => {
                        last_error = e.to_string();
                        if stop_on_error {
                            let _ = cx.update(|cx| {
                                status.update(cx, |s, cx| {
                                    *s = format!("执行错误: {}", last_error);
                                    cx.notify();
                                });
                            });
                            return;
                        }
                    }
                }
            }

            let _ = cx.update(|cx| {
                status.update(cx, |s, cx| {
                    if error_count == 0 {
                        *s = format!("执行完成: {} 条语句全部成功", success_count);
                    } else {
                        *s = format!(
                            "执行完成: {} 条成功, {} 条失败\n最后错误: {}",
                            success_count, error_count, last_error
                        );
                    }
                    cx.notify();
                });
            });
        }).detach();
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
                            .child(Switch::new("stop_on_error").checked(*self.stop_on_error.read(cx)))
                            .child("遇错停止"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(Switch::new("use_transaction").checked(*self.use_transaction.read(cx)))
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
