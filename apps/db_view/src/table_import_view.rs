use gpui::{div, App, AppContext, ClickEvent, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    radio::Radio,
    select::{Select, SelectItem, SelectState},
    switch::Switch,
    v_flex, ActiveTheme, IndexPath, Sizable,
};

use db::{DataFormat, DataImporter, GlobalDbState, ImportConfig};

// 记录分隔符选项
#[derive(Clone, Debug, PartialEq)]
pub enum RecordSeparator {
    Lf,        // LF
    CrLf,      // CR+LF
    Custom(String),
}

impl RecordSeparator {
    fn to_string(&self) -> String {
        match self {
            RecordSeparator::Lf => "\n".to_string(),
            RecordSeparator::CrLf => "\r\n".to_string(),
            RecordSeparator::Custom(s) => s.clone(),
        }
    }
}

// 字段分隔符选项
#[derive(Clone, Debug, PartialEq)]
pub enum FieldSeparator {
    Comma,     // 逗号
    Tab,       // 制表符
    Semicolon, // 分号
    Space,     // 空格
    Custom(String),
}

impl FieldSeparator {
    fn to_string(&self) -> String {
        match self {
            FieldSeparator::Comma => ",".to_string(),
            FieldSeparator::Tab => "\t".to_string(),
            FieldSeparator::Semicolon => ";".to_string(),
            FieldSeparator::Space => " ".to_string(),
            FieldSeparator::Custom(s) => s.clone(),
        }
    }
}

// 文本识别符选项
#[derive(Clone, Debug)]
struct TextQualifierItem {
    name: String,
    value: String,
}

impl TextQualifierItem {
    fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { 
            name: name.into(),
            value: value.into(),
        }
    }
}

impl SelectItem for TextQualifierItem {
    type Value = String;

    fn title(&self) -> gpui::SharedString {
        self.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

pub struct TableImportView {
    connection_id: String,
    database: Entity<InputState>,
    pub table: Entity<InputState>,
    format: Entity<DataFormat>,
    format_display: Entity<String>, // "TXT", "CSV", "JSON"
    
    // 文件选择
    file_path: Entity<InputState>,
    pending_file_path: Entity<Option<String>>,
    
    // 分隔符配置
    record_separator: Entity<RecordSeparator>,
    record_separator_mode: Entity<String>, // "auto" 或 "fixed"
    field_separator: Entity<FieldSeparator>,
    field_separator_custom: Entity<InputState>,
    text_qualifier: Entity<SelectState<Vec<TextQualifierItem>>>,
    
    // 导入选项
    has_header: Entity<bool>,
    stop_on_error: Entity<bool>,
    use_transaction: Entity<bool>,
    truncate_before: Entity<bool>,
    
    status: Entity<String>,
    focus_handle: FocusHandle,
}

impl TableImportView {
    pub fn new(
        connection_id: impl Into<String>,
        database: String,
        table: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let database_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value(database, window, cx);
                state
            });
            
            let table_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                if let Some(t) = table {
                    state.set_value(t, window, cx);
                }
                state
            });

            // 创建文本识别符选择器
            let text_qualifier_items = vec![
                TextQualifierItem::new("无", ""),
                TextQualifierItem::new("双引号 (\")", "\""),
                TextQualifierItem::new("单引号 (')", "'"),
            ];
            let text_qualifier = cx.new(|cx| {
                SelectState::new(text_qualifier_items, Some(IndexPath::default()), window, cx)
            });

            Self {
                connection_id: connection_id.into(),
                database: database_input,
                table: table_input,
                format: cx.new(|_| DataFormat::Csv),
                format_display: cx.new(|_| "TXT".to_string()),
                
                file_path: cx.new(|cx| InputState::new(window, cx)),
                pending_file_path: cx.new(|_| None),
                
                record_separator: cx.new(|_| RecordSeparator::Lf),
                record_separator_mode: cx.new(|_| "auto".to_string()),
                field_separator: cx.new(|_| FieldSeparator::Comma),
                field_separator_custom: cx.new(|cx| InputState::new(window, cx)),
                text_qualifier,
                
                has_header: cx.new(|_| true),
                stop_on_error: cx.new(|_| true),
                use_transaction: cx.new(|_| true),
                truncate_before: cx.new(|_| false),
                
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
            multiple: false,
            directories: false,
            prompt: Some("选择导入文件".into()),
        });

        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                if let Some(path_buf) = paths.first() {
                    let path = path_buf.to_string_lossy().to_string();
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
        let file_path_str = self.file_path.read(cx).text().to_string();
        let status = self.status.clone();

        if file_path_str.is_empty() {
            status.update(cx, |s, cx| {
                *s = "请选择文件".to_string();
                cx.notify();
            });
            return;
        }

        if table.is_empty() {
            status.update(cx, |s, cx| {
                *s = "请输入表名".to_string();
                cx.notify();
            });
            return;
        }

        status.update(cx, |s, cx| {
            *s = "正在导入...".to_string();
            cx.notify();
        });

        let stop_on_error = *self.stop_on_error.read(cx);
        let use_transaction = *self.use_transaction.read(cx);
        let truncate_before = *self.truncate_before.read(cx);

        cx.spawn(async move |cx| {
            let config = match global_state.get_config_async(&connection_id).await {
                Some(cfg) => cfg,
                None => {
                    cx.update(|cx| {
                        status.update(cx, |s, cx| {
                            *s = "连接未找到".to_string();
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
                            *s = format!("错误: {}", e);
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
                            *s = format!("连接错误: {}", e);
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
                            *s = format!("文件读取错误: {}", e);
                            cx.notify();
                        });
                    }).ok();
                    return;
                }
            };

            let import_config = ImportConfig {
                format,
                database,
                table: Some(table),
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
                                    "导入成功: {} 行数据，耗时 {}ms",
                                    result.rows_imported, result.elapsed_ms
                                );
                            } else {
                                *s = format!(
                                    "部分成功: {} 行导入，{} 个错误",
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
                            *s = format!("导入错误: {}", e);
                            cx.notify();
                        });
                    }).ok();
                }
            }
        }).detach();
    }
}

impl Focusable for TableImportView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Clone for TableImportView {
    fn clone(&self) -> Self {
        Self {
            connection_id: self.connection_id.clone(),
            database: self.database.clone(),
            table: self.table.clone(),
            format: self.format.clone(),
            format_display: self.format_display.clone(),
            
            file_path: self.file_path.clone(),
            pending_file_path: self.pending_file_path.clone(),
            
            record_separator: self.record_separator.clone(),
            record_separator_mode: self.record_separator_mode.clone(),
            field_separator: self.field_separator.clone(),
            field_separator_custom: self.field_separator_custom.clone(),
            text_qualifier: self.text_qualifier.clone(),
            
            has_header: self.has_header.clone(),
            stop_on_error: self.stop_on_error.clone(),
            use_transaction: self.use_transaction.clone(),
            truncate_before: self.truncate_before.clone(),
            
            status: self.status.clone(),
            focus_handle: self.focus_handle.clone(),
        }
    }
}

impl Render for TableImportView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // 检查是否有待更新的文件路径
        if let Some(path) = self.pending_file_path.read(cx).clone() {
            self.file_path.update(cx, |state, cx| {
                state.replace(path, window, cx);
            });
            self.pending_file_path.update(cx, |p, _| *p = None);
        }

        let status_text = self.status.read(cx).clone();
        let _current_format = *self.format.read(cx);
        let current_format_display = self.format_display.read(cx).clone();
        let record_sep_mode = self.record_separator_mode.read(cx).clone();

        v_flex()
            .gap_4()
            .p_4()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_20().child("数据库:"))
                    .child(Input::new(&self.database).w_48()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_20().child("表名:"))
                    .child(Input::new(&self.table).w_48()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w_20().child("格式:"))
                    .child(
                        h_flex()
                            .gap_1()
                            .child({
                                let mut btn = Button::new("format_txt").child("TXT");
                                if current_format_display == "TXT" {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Csv;
                                        cx.notify();
                                    });
                                    view.format_display.update(cx, |d, cx| {
                                        *d = "TXT".to_string();
                                        cx.notify();
                                    });
                                }))
                            })
                            .child({
                                let mut btn = Button::new("format_csv").child("CSV");
                                if current_format_display == "CSV" {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Csv;
                                        cx.notify();
                                    });
                                    view.format_display.update(cx, |d, cx| {
                                        *d = "CSV".to_string();
                                        cx.notify();
                                    });
                                }))
                            })
                            .child({
                                let mut btn = Button::new("format_json").child("JSON");
                                if current_format_display == "JSON" {
                                    btn = btn.primary();
                                }
                                btn.on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                    view.format.update(cx, |f, cx| {
                                        *f = DataFormat::Json;
                                        cx.notify();
                                    });
                                    view.format_display.update(cx, |d, cx| {
                                        *d = "JSON".to_string();
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
                    .child(div().w_20().child("文件:"))
                    .child(Input::new(&self.file_path).flex_1())
                    .child(
                        Button::new("select_file")
                            .small()
                            .child("浏览")
                            .on_click(window.listener_for(&cx.entity(), |view, _: &ClickEvent, window, cx| {
                                view.select_file(window, cx);
                            })),
                    ),
            )
            // 分隔符配置（仅对 CSV/TXT 显示）
            .child(
                if current_format_display != "JSON" {
                    v_flex()
                        .gap_3()
                        .p_3()
                        .border_1()
                        .border_color(cx.theme().border)
                        .rounded_md()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(gpui::FontWeight::SEMIBOLD)
                                .child("你的字段要用什么分隔符来分隔？请选择合适的分隔符。")
                        )
                        .child(
                            v_flex()
                                .gap_2()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .items_center()
                                        .child(div().w_24().child("记录分隔符:"))
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .child(
                                                    Radio::new("record_sep_auto")
                                                        .checked(record_sep_mode == "auto")
                                                        .on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                                            view.record_separator_mode.update(cx, |mode, cx| {
                                                                *mode = "auto".to_string();
                                                                cx.notify();
                                                            });
                                                        }))
                                                )
                                                .child("分隔符 - 字符识别适号制符号，用来界定每个字段")
                                        )
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .items_center()
                                        .child(div().w_24())
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .child(
                                                    Radio::new("record_sep_fixed")
                                                        .checked(record_sep_mode == "fixed")
                                                        .on_click(window.listener_for(&cx.entity(), |view, _, _, cx| {
                                                            view.record_separator_mode.update(cx, |mode, cx| {
                                                                *mode = "fixed".to_string();
                                                                cx.notify();
                                                            });
                                                        }))
                                                )
                                                .child("固定宽度 - 每个列内容段对齐，用空格分隔")
                                        )
                                )
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(div().w_24().child("字段分隔符:"))
                                .child(Input::new(&self.field_separator_custom).w_32())
                        )
                        .child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .child(div().w_24().child("文本识别符:"))
                                .child(Select::new(&self.text_qualifier).w_32())
                        )
                        .into_any_element()
                } else {
                    div().into_any_element()
                }
            )
            .child(
                h_flex()
                    .gap_4()
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("has_header")
                                    .checked(*self.has_header.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.has_header.update(cx, |state, cx| {
                                            *state = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("包含标题行"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("stop_on_error")
                                    .checked(*self.stop_on_error.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.stop_on_error.update(cx, |state, cx| {
                                            *state = *checked;
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
                                        view.use_transaction.update(cx, |state, cx| {
                                            *state = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("使用事务"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Switch::new("truncate_before")
                                    .checked(*self.truncate_before.read(cx))
                                    .on_click(cx.listener(|view, checked, _, cx| {
                                        view.truncate_before.update(cx, |state, cx| {
                                            *state = *checked;
                                            cx.notify();
                                        });
                                    }))
                            )
                            .child("导入前清空表"),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("import")
                            .primary()
                            .child("开始导入")
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