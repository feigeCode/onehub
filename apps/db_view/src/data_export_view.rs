use gpui::{div, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

use db::{DataExporter, DataFormat, ExportConfig, GlobalDbState};

pub struct DataExportView {
    connection_id: String,
    database: Entity<InputState>,
    pub tables: Entity<InputState>,
    format: Entity<DataFormat>,
    include_schema: Entity<bool>,
    include_data: Entity<bool>,
    where_clause: Entity<InputState>,
    limit: Entity<InputState>,
    output_path: Entity<InputState>,
    pending_output_path: Entity<Option<String>>,
    status: Entity<String>,
    focus_handle: FocusHandle,
}

impl DataExportView {
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
            let tables_input = cx.new(|cx| InputState::new(window, cx));
            let where_input = cx.new(|cx| InputState::new(window, cx));
            let limit_input = cx.new(|cx| InputState::new(window, cx));

            Self {
                connection_id: connection_id.into(),
                database: database_input,
                tables: tables_input,
                format: cx.new(|_| DataFormat::Sql),
                include_schema: cx.new(|_| true),
                include_data: cx.new(|_| true),
                where_clause: where_input,
                limit: limit_input,
                output_path: cx.new(|cx| InputState::new(window, cx)),
                pending_output_path: cx.new(|_| None),
                status: cx.new(|_| String::new()),
                focus_handle: cx.focus_handle(),
            }
        })
    }

    fn select_output(&mut self, _window: &mut Window, cx: &mut App) {
        let pending = self.pending_output_path.clone();
        let status = self.status.clone();
        let future = cx.prompt_for_paths(PathPromptOptions {
            files: false,
            multiple: false,
            directories: true,
            prompt: Some("选择导出目录".into()),
        });
        // 使用异步文件选择器
        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                let path = paths.first().unwrap().to_string_lossy().to_string();
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

    fn start_export(&mut self, _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();

        let database = self.database.read(cx).text().to_string();
        let tables_str = self.tables.read(cx).text().to_string();
        let format = *self.format.read(cx);
        let include_schema = *self.include_schema.read(cx);
        let include_data = *self.include_data.read(cx);
        let where_clause_str = self.where_clause.read(cx).text().to_string();
        let limit_str = self.limit.read(cx).text().to_string();
        let output_path_str = self.output_path.read(cx).text().to_string();

        let status = self.status.clone();

        if tables_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "Please enter table names (comma separated)".to_string();
                cx.notify();
            });
            return;
        }

        if output_path_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "Please enter output file path".to_string();
                cx.notify();
            });
            return;
        }

        let tables: Vec<String> = tables_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let where_clause = if where_clause_str.is_empty() {
            None
        } else {
            Some(where_clause_str)
        };

        let limit = if limit_str.is_empty() {
            None
        } else {
            limit_str.parse::<usize>().ok()
        };

        status.update(cx, |s, cx| {
            *s = "Exporting...".to_string();
            cx.notify();
        });

        cx.spawn(async move |cx| {
            let config = match global_state.get_config(&connection_id).await {
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

            let export_config = ExportConfig {
                format,
                database,
                tables,
                include_schema,
                include_data,
                where_clause,
                limit,
            };

            match DataExporter::export(connection.as_ref(), export_config).await {
                Ok(result) => {
                    // 写入文件
                    if let Err(e) = std::fs::write(&output_path_str, result.output) {
                        cx.update(|cx| {
                            status.update(cx, |s, cx| {
                                *s = format!("File write error: {}", e);
                                cx.notify();
                            });
                        }).ok();
                        return;
                    }

                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!(
                                "Success: {} rows exported to {} in {}ms",
                                result.rows_exported, output_path_str, result.elapsed_ms
                            );
                            cx.notify();
                        });
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("Export error: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }
}

impl Focusable for DataExportView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for DataExportView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            database: self.database.clone(),
            tables: self.tables.clone(),
            format: self.format.clone(),
            include_schema: self.include_schema.clone(),
            include_data: self.include_data.clone(),
            where_clause: self.where_clause.clone(),
            limit: self.limit.clone(),
            output_path: self.output_path.clone(),
            pending_output_path: self.pending_output_path.clone(),
            status: self.status.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for DataExportView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 检查是否有待更新的输出路径
        if let Some(path) = self.pending_output_path.read(cx).clone() {
            self.output_path.update(cx, |state, cx| {
                state.replace(path, window, cx);
            });
            self.pending_output_path.update(cx, |p, _| *p = None);
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
                    .child(div().w_24().child("Tables:"))
                    .child(Input::new(&self.tables).w_96())
                    .child(div().text_xs().text_color(cx.theme().muted_foreground).child("(Comma separated)")),
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
                    .child(div().w_24().child("WHERE:"))
                    .child(Input::new(&self.where_clause).w_96())
                    .child(div().text_xs().text_color(cx.theme().muted_foreground).child("(Optional)")),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("LIMIT:"))
                    .child(Input::new(&self.limit).w_32())
                    .child(div().text_xs().text_color(cx.theme().muted_foreground).child("(Optional)")),
            )
            .child(
                h_flex()
                    .gap_4()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(Switch::new("include_schema").checked(*self.include_schema.read(cx)))
                            .child("Include schema"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(Switch::new("include_data").checked(*self.include_data.read(cx)))
                            .child("Include data"),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("Output:"))
                    .child(Input::new(&self.output_path).w_full())
                    .child(
                        Button::new("select_output")
                            .small()
                            .child("Browse")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.select_output(window, cx);
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("export")
                            .primary()
                            .child("Export")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.start_export(window, cx);
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
