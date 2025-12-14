use gpui::{div, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

use db::{DataFormat, DataImporter, GlobalDbState, ImportConfig};

pub struct DataImportView {
    connection_id: String,
    database: Entity<InputState>,
    pub table: Entity<InputState>,
    format: Entity<DataFormat>,
    stop_on_error: Entity<bool>,
    use_transaction: Entity<bool>,
    truncate_before: Entity<bool>,
    file_path: Entity<InputState>,
    pending_file_path: Entity<Option<String>>,
    status: Entity<String>,
    focus_handle: FocusHandle,
}

impl DataImportView {
    pub fn new(
        connection_id: impl Into<String>,
        database: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let database_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value(database, window, cx);
                state
            });
            let table_input = cx.new(|cx| InputState::new(window, cx));

            Self {
                connection_id: connection_id.into(),
                database: database_input,
                table: table_input,
                format: cx.new(|_| DataFormat::Sql),
                stop_on_error: cx.new(|_| true),
                use_transaction: cx.new(|_| true),
                truncate_before: cx.new(|_| false),
                file_path: cx.new(|cx| InputState::new(window, cx)),
                pending_file_path: cx.new(|_| None),
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
            prompt: Some("选择导入文件".into()),
        });
        // 使用异步文件选择器
        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                let mut path = String::new();
                let mut i : usize = 0;
                for path_buf in paths.iter() {
                    path.push_str(path_buf.to_str().unwrap());
                    i += 1;
                    if i < paths.len() {
                        path.push_str(";");
                    }
                }
                let _ = cx.update(|cx| {
                    pending.update(cx, |p, cx| {
                        *p = Some(path.clone());
                        cx.notify();
                    });
                    status.update(cx, |s, cx| {
                        *s = format!("Selected: {}", path);
                        cx.notify();
                    });
                });
            }
        })
        .detach();
    }

    fn start_import(&mut self, _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();

        let database = self.database.read(cx).text().to_string();
        let table = self.table.read(cx).text().to_string();
        let format = *self.format.read(cx);
        let stop_on_error = *self.stop_on_error.read(cx);
        let use_transaction = *self.use_transaction.read(cx);
        let truncate_before = *self.truncate_before.read(cx);
        let file_path_str = self.file_path.read(cx).text().to_string();

        let status = self.status.clone();

        if file_path_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "Please select a file".to_string();
                cx.notify();
            });
            return;
        }

        status.update(cx, |s, cx| {
            *s = "Importing...".to_string();
            cx.notify();
        });

        cx.spawn(async move |cx| {
            let config = match global_state.get_config_async(&connection_id).await {
                Some(cfg) => cfg,
                None => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = "Connection not found".to_string();
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let plugin = match global_state.db_manager.get_plugin(&config.database_type) {
                Ok(p) => p,
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("Error: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let connection = match plugin.create_connection(config).await {
                Ok(c) => c,
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("Connection error: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            // 读取文件
            let data = match std::fs::read_to_string(&file_path_str) {
                Ok(d) => d,
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("File read error: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let import_config = ImportConfig {
                format,
                database,
                table: if table.is_empty() { None } else { Some(table) },
                stop_on_error,
                use_transaction,
                truncate_before_import: truncate_before,
            };

            match DataImporter::import(connection.as_ref(), import_config, data).await {
                Ok(result) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            if result.success {
                                *s = format!(
                                    "Success: {} rows imported in {}ms",
                                    result.rows_imported, result.elapsed_ms
                                );
                            } else {
                                *s = format!(
                                    "Partial success: {} rows imported, {} errors",
                                    result.rows_imported,
                                    result.errors.len()
                                );
                            }
                            cx.notify();
                        });
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("Import error: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }
}

impl Focusable for DataImportView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for DataImportView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            database: self.database.clone(),
            table: self.table.clone(),
            format: self.format.clone(),
            stop_on_error: self.stop_on_error.clone(),
            use_transaction: self.use_transaction.clone(),
            truncate_before: self.truncate_before.clone(),
            file_path: self.file_path.clone(),
            pending_file_path: self.pending_file_path.clone(),
            status: self.status.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for DataImportView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 检查是否有待更新的文件路径
        if let Some(path) = self.pending_file_path.read(cx).clone() {
            self.file_path.update(cx, |state, cx| {
                state.replace(path, window, cx);
            });
            self.pending_file_path.update(cx, |p, _| *p = None);
        }

        let status_text = self.status.read(cx).clone();
        let current_format = *self.format.read(cx);

        v_flex()
            .gap_3()
            .p_4()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("Database:"))
                    .child(Input::new(&self.database).w_64()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("Table:"))
                    .child(Input::new(&self.table).w_64())
                    .child(div().text_xs().text_color(cx.theme().muted_foreground).child("(Optional for SQL)")),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("Format:"))
                    .child(
                        h_flex()
                            .gap_1()
                            .child({
                                let mut btn = Button::new("format_sql").child("SQL");
                                if current_format == DataFormat::Sql {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Sql;
                                        cx.notify();
                                    });
                                }))
                            })
                            .child({
                                let mut btn = Button::new("format_json").child("JSON");
                                if current_format == DataFormat::Json {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Json;
                                        cx.notify();
                                    });
                                }))
                            })
                            .child({
                                let mut btn = Button::new("format_csv").child("CSV");
                                if current_format == DataFormat::Csv {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Csv;
                                        cx.notify();
                                    });
                                }))
                            })
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("File Path:"))
                    .child(Input::new(&self.file_path).w_full())
                    .child(
                        Button::new("select_file")
                            .small()
                            .child("Browse")
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
                            .child("Stop on error"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(Switch::new("use_transaction").checked(*self.use_transaction.read(cx)))
                            .child("Use transaction"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(Switch::new("truncate_before").checked(*self.truncate_before.read(cx)))
                            .child("Truncate before import"),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("import")
                            .primary()
                            .child("Import")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.start_import(window, cx);
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
