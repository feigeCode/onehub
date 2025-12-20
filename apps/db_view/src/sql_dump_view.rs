use gpui::{div, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    switch::Switch,
    v_flex, ActiveTheme, Sizable,
};

use db::{DataFormat, ExportConfig, GlobalDbState};
use crate::db_tree_view::SqlDumpMode;

pub struct SqlDumpView {
    connection_id: String,
    database: Entity<InputState>,
    tables: Entity<InputState>,
    include_structure: Entity<bool>,
    include_data: Entity<bool>,
    output_path: Entity<InputState>,
    pending_output_path: Entity<Option<String>>,
    status: Entity<String>,
    focus_handle: FocusHandle,
}

impl SqlDumpView {
    pub fn new(
        connection_id: impl Into<String>,
        database: String,
        mode: SqlDumpMode,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let database_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value(database, window, cx);
                state
            });

            let (structure, data) = match mode {
                SqlDumpMode::StructureOnly => (true, false),
                SqlDumpMode::DataOnly => (false, true),
                SqlDumpMode::StructureAndData => (true, true),
            };

            Self {
                connection_id: connection_id.into(),
                database: database_input,
                tables: cx.new(|cx| InputState::new(window, cx)),
                include_structure: cx.new(|_| structure),
                include_data: cx.new(|_| data),
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

        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                let path = paths.first().unwrap().to_string_lossy().to_string();
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

    fn start_dump(&mut self, _window: &mut Window, cx: &mut App) {
        let global_state = cx.global::<GlobalDbState>().clone();
        let connection_id = self.connection_id.clone();
        let database = self.database.read(cx).text().to_string();
        let tables_str = self.tables.read(cx).text().to_string();
        let include_structure = *self.include_structure.read(cx);
        let include_data = *self.include_data.read(cx);
        let output_path_str = self.output_path.read(cx).text().to_string();
        let status = self.status.clone();

        if database.is_empty() {
            status.update(cx, |s, cx| {
                *s = "请输入数据库名".to_string();
                cx.notify();
            });
            return;
        }

        if output_path_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "请选择输出目录".to_string();
                cx.notify();
            });
            return;
        }

        if !include_structure && !include_data {
            status.update(cx, |s, cx| {
                *s = "请至少选择导出结构或数据".to_string();
                cx.notify();
            });
            return;
        }

        status.update(cx, |s, cx| {
            *s = "正在导出...".to_string();
            cx.notify();
        });

        cx.spawn(async move |mut cx| {
            let tables: Vec<String> = if tables_str.is_empty() {
                match global_state.list_tables(&mut cx, connection_id.clone(), database.clone()).await {
                    Ok(t) => t.into_iter().map(|info| info.name).collect(),
                    Err(e) => {
                        cx.update(|cx| {
                            status.update(cx, |s, cx| {
                                *s = format!("获取表列表失败: {}", e);
                                cx.notify();
                            });
                        }).ok();
                        return;
                    }
                }
            } else {
                tables_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            };

            if tables.is_empty() {
                cx.update(|cx| {
                    status.update(cx, |s, cx| {
                        *s = "数据库中没有表".to_string();
                        cx.notify();
                    });
                }).ok();
                return;
            }

            let export_config = ExportConfig {
                format: DataFormat::Sql,
                database: database.clone(),
                tables,
                include_schema: include_structure,
                include_data,
                where_clause: None,
                limit: None,
            };

            match global_state.export_data(&mut cx, connection_id.clone(), export_config).await {
                Ok(result) => {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let filename = format!("{}_{}.sql", database, timestamp);
                    let full_path = std::path::Path::new(&output_path_str).join(&filename);

                    if let Err(e) = std::fs::write(&full_path, result.output) {
                        cx.update(|cx| {
                            status.update(cx, |s, cx| {
                                *s = format!("文件写入错误: {}", e);
                                cx.notify();
                            });
                        }).ok();
                        return;
                    }

                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!(
                                "导出成功: {} 行数据已导出到 {}\n耗时: {}ms",
                                result.rows_exported,
                                full_path.display(),
                                result.elapsed_ms
                            );
                            cx.notify();
                        });
                    }).ok();
                }
                Err(e) => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = format!("导出错误: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
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
            database: self.database.clone(),
            tables: self.tables.clone(),
            include_structure: self.include_structure.clone(),
            include_data: self.include_data.clone(),
            output_path: self.output_path.clone(),
            pending_output_path: self.pending_output_path.clone(),
            status: self.status.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for SqlDumpView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 检查是否有待更新的输出路径
        if let Some(path) = self.pending_output_path.read(cx).clone() {
            self.output_path.update(cx, |state, cx| {
                state.replace(path, window, cx);
            });
            self.pending_output_path.update(cx, |p, _| *p = None);
        }

        let status_text = self.status.read(cx).clone();

        v_flex()
            .gap_3()
            .p_4()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("数据库:"))
                    .child(Input::new(&self.database).w_64()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("表:"))
                    .child(Input::new(&self.tables).w_96())
                    .child(div().text_xs().text_color(cx.theme().muted_foreground).child("(逗号分隔，留空导出所有表)")),
            )
            .child(
                h_flex()
                    .gap_4()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("include_structure")
                                    .checked(*self.include_structure.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.include_structure.update(cx, |state, cx| {
                                            *state = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("导出结构"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("include_data")
                                    .checked(*self.include_data.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.include_data.update(cx, |state, cx| {
                                            *state = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("导出数据"),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_24().child("输出目录:"))
                    .child(Input::new(&self.output_path).w_full())
                    .child(
                        Button::new("select_output")
                            .small()
                            .child("浏览")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.select_output(window, cx);
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("dump")
                            .primary()
                            .child("导出")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.start_dump(window, cx);
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
