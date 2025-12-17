use gpui::{App, AppContext, Context, Entity, EventEmitter, IntoElement, Render, SharedString, Styled as _, Window};
use serde_json::from_str;
use gpui_component::highlighter::Language;
use gpui_component::input::{Input, InputEvent, InputState, TabSize};
use gpui_component::v_flex;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EditorTab {
    Text,
    Json,
}

impl EditorTab {
    pub fn language(&self) -> Language {
        match self {
            EditorTab::Text => Language::from_str("text"),
            EditorTab::Json => Language::from_str("json"),
        }
    }

    pub fn label(&self) -> &str {
        match self {
            EditorTab::Text => "Text",
            EditorTab::Json => "JSON",
        }
    }
}

impl EventEmitter<InputEvent> for MultiTextEditor {}

pub struct MultiTextEditor {
    active_tab: EditorTab,
    text_editor: Entity<InputState>,
    json_editor: Entity<InputState>,
}

impl MultiTextEditor {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let text_editor = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(EditorTab::Text.language())
                .line_number(true)
                .searchable(true)
                .indent_guides(true)
                .tab_size(TabSize { tab_size: 2, hard_tabs: false })
                .soft_wrap(false)
                .placeholder("Enter your text here...")
        });

        let json_editor = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(EditorTab::Json.language())
                .line_number(true)
                .searchable(true)
                .indent_guides(true)
                .tab_size(TabSize { tab_size: 2, hard_tabs: false })
                .soft_wrap(false)
                .placeholder("Enter JSON here...")
        });

        Self {
            active_tab: EditorTab::Text,
            text_editor,
            json_editor,
        }
    }

    pub fn switch_tab(&mut self, tab: EditorTab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        cx.notify();
    }

    pub fn active_tab(&self) -> EditorTab {
        self.active_tab
    }

    fn get_active_editor(&self) -> &Entity<InputState> {
        match self.active_tab {
            EditorTab::Text => &self.text_editor,
            EditorTab::Json => &self.json_editor,
        }
    }

    fn get_active_editor_mut(&mut self) -> &mut Entity<InputState> {
        match self.active_tab {
            EditorTab::Text => &mut self.text_editor,
            EditorTab::Json => &mut self.json_editor,
        }
    }

    pub fn get_active_text(&self, cx: &App) -> Result<String, serde_json::error::Error> {
        let value = self.get_active_editor().read(cx).text().to_string();
        if self.active_tab == EditorTab::Json {
            return match from_str::<serde_json::Value>(&value) {
                Ok(v) => Ok(v.to_string()),
                Err(e) => Err(e)
            }
        }
        Ok(value)
    }

    pub fn set_active_text(&mut self, text: String, window: &mut Window, cx: &mut Context<Self>) {
        // Set text editor
        self.text_editor.update(cx, |s, cx| {
            s.set_value(text.clone(), window, cx);
        });
        
        // Try to parse and format as JSON for json editor
        let json_text = match from_str::<serde_json::Value>(&text) {
            Ok(value) => {
                serde_json::to_string_pretty(&value).unwrap_or(text.clone())
            }
            Err(_) => text.clone(),
        };
        
        self.json_editor.update(cx, |s, cx| {
            s.set_value(json_text, window, cx);
        });
    }



    pub fn format_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let text = self.json_editor.read(cx).text().to_string();
        match from_str::<serde_json::Value>(&text) {
            Ok(value) => {
                if let Ok(formatted) = serde_json::to_string_pretty(&value) {
                    self.json_editor.update(cx, |s, cx| {
                        s.set_value(formatted, window, cx);
                    });
                }
            }
            Err(_) => {
                // JSON解析失败，不做处理
            }
        }
    }

    pub fn minify_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let text = self.json_editor.read(cx).text().to_string();
        match from_str::<serde_json::Value>(&text) {
            Ok(value) => {
                if let Ok(minified) = serde_json::to_string(&value) {
                    self.json_editor.update(cx, |s, cx| {
                        s.set_value(minified, window, cx);
                    });
                }
            }
            Err(_) => {
                // JSON解析失败，不做处理
            }
        }
    }
}

impl Render for MultiTextEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{div, px, ElementId, InteractiveElement, ParentElement};
        use gpui::prelude::{FluentBuilder, StatefulInteractiveElement};
        use gpui_component::{button::Button, h_flex, ActiveTheme, IconName, Sizable, Size};

        let active_tab = self.active_tab;
        let is_json_tab = active_tab == EditorTab::Json;

        v_flex()
            .size_full()
            .child(
                h_flex()
                    .h(px(40.0))
                    .bg(cx.theme().tab)
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        h_flex()
                            .gap_1()
                            .pl_2()
                            .items_center()
                            .child(
                                div()
                                    .id(ElementId::Name(SharedString::from("tab-text")))
                                    .px_3()
                                    .py_2()
                                    .rounded_md()
                                    .cursor_pointer()
                                    .when(active_tab == EditorTab::Text, |this| {
                                        this.bg(cx.theme().tab_active)
                                    })
                                    .when(active_tab != EditorTab::Text, |this| {
                                        this.hover(|style| style.bg(cx.theme().tab.opacity(0.8)))
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.switch_tab(EditorTab::Text, cx);
                                    }))
                                    .child("Text"),
                            )
                            .child(
                                div()
                                    .id(ElementId::Name(SharedString::from("tab-json")))
                                    .px_3()
                                    .py_2()
                                    .rounded_md()
                                    .cursor_pointer()
                                    .when(active_tab == EditorTab::Json, |this| {
                                        this.bg(cx.theme().tab_active)
                                    })
                                    .when(active_tab != EditorTab::Json, |this| {
                                        this.hover(|style| style.bg(cx.theme().tab.opacity(0.8)))
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.switch_tab(EditorTab::Json, cx);
                                    }))
                                    .child("JSON"),
                            ),
                    )
                    .child(
                        h_flex()
                            .flex_1()
                            .justify_end()
                            .gap_2()
                            .pr_2()
                            .when(is_json_tab, |this| {
                                this.child(
                                    Button::new("format-json")
                                        .with_size(Size::Small)
                                        .label("Format")
                                        .icon(IconName::Star)
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.format_json(window, cx);
                                        })),
                                )
                                .child(
                                    Button::new("minify-json")
                                        .with_size(Size::Small)
                                        .label("Minify")
                                        .icon(IconName::File)
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.minify_json(window, cx);
                                        })),
                                )
                            }),
                    ),
            )
            .child(
                v_flex()
                    .flex_1()
                    .child(match active_tab {
                        EditorTab::Text => Input::new(&self.text_editor).size_full(),
                        EditorTab::Json => Input::new(&self.json_editor).size_full(),
                    }),
            )
    }
}

pub fn create_multi_text_editor_with_content(
    initial_content: Option<String>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<MultiTextEditor> {
    cx.new(|cx| {
        let mut editor = MultiTextEditor::new(window, cx);
        if let Some(content) = initial_content {
            editor.set_active_text(content, window, cx);
        }
        editor
    })
}