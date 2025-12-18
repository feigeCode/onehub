use db::{GlobalDbState, plugin::DatabaseOperationRequest, SqlResult};
use gpui::{div, AnyView, App, AppContext, AsyncApp, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, Styled, Subscription, Window};
use gpui_component::{
    button::{Button, ButtonVariants},
    h_flex, v_flex,
    highlighter::Language,
    input::{Input, InputState},
    notification::Notification,
    WindowExt,
};
use one_core::storage::DatabaseType;
use crate::db_tree_view::DbTreeView;
use super::DatabaseFormEvent;

pub struct DatabaseEditorView {
    focus_handle: FocusHandle,
    form: AnyView,
    sql_preview: Entity<InputState>,
    current_tab: EditorTab,
    is_edit_mode: bool,
    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Copy, PartialEq)]
enum EditorTab {
    Form,
    SqlPreview,
}

impl DatabaseEditorView {
    pub fn new<F>(
        form: Entity<F>,
        database_type: DatabaseType,
        is_edit_mode: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self
    where
        F: Render + EventEmitter<DatabaseFormEvent> + 'static,
    {
        let sql_preview = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(Language::from_str("sql"))
                .multi_line(true)
        });
        let focus_handle = cx.focus_handle();

        let is_edit = is_edit_mode;

        let form_subscription = cx.subscribe_in(&form, window, move |this, _form, event, window, cx| {
            match event {
                DatabaseFormEvent::FormChanged(request) => {
                    this.update_sql_preview(request, database_type, is_edit, window, cx);
                }
            }
        });

        Self {
            focus_handle,
            form: form.into(),
            sql_preview,
            current_tab: EditorTab::Form,
            is_edit_mode,
            _subscriptions: vec![form_subscription],
        }
    }

    fn update_sql_preview(
        &mut self,
        request: &DatabaseOperationRequest,
        database_type: DatabaseType,
        is_edit_mode: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let global_db_state = cx.global::<GlobalDbState>();
        if let Ok(plugin) = global_db_state.get_plugin(&database_type) {
            let sql = if is_edit_mode {
                plugin.build_modify_database_sql(request)
            } else {
                plugin.build_create_database_sql(request)
            };
            self.sql_preview.update(cx, |state, cx| {
                state.set_value(sql, window, cx);
            });
        }
    }

    pub fn trigger_save(
        &self,
        connection_id: String,
        global_state: GlobalDbState,
        tree_view: Entity<DbTreeView>,
        cx: &mut Context<Self>,
    ) {
        let sql = self.sql_preview.read(cx).text().to_string();
        let is_edit = self.is_edit_mode;

        cx.spawn(async move |_this, cx: &mut AsyncApp| {
            let result = global_state.execute_single(
                cx,
                connection_id.clone(),
                sql,
                None,
                None,
            ).await;

            match result {
                Ok(sql_result) => {
                    let _ = cx.update(|cx| {
                        match sql_result {
                            SqlResult::Query(_) => {}
                            SqlResult::Exec(_) => {
                                tree_view.update(cx, |tree, cx| {
                                    tree.refresh_tree(connection_id.clone(), cx);
                                });
                                if is_edit {
                                    Self::show_success_async(cx, "数据库修改成功");
                                } else {
                                    Self::show_success_async(cx, "数据库创建成功");
                                }
                            }
                            SqlResult::Error(err) => {
                                let msg = if is_edit {
                                    format!("修改数据库失败: {}", err.message)
                                } else {
                                    format!("创建数据库失败: {}", err.message)
                                };
                                Self::show_error_async(cx, msg);
                            }
                        }
                    });
                }
                Err(e) => {
                    let _ = cx.update(|cx| {
                        let msg = if is_edit {
                            format!("修改数据库失败: {}", e)
                        } else {
                            format!("创建数据库失败: {}", e)
                        };
                        Self::show_error_async(cx, msg);
                    });
                }
            }
        }).detach();
    }

    fn show_success_async(cx: &mut App, message: impl Into<String>) {
        if let Some(window) = cx.active_window() {
            let _ = window.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::success(message.into()).autohide(true),
                    cx
                );
            });
        }
    }

    fn show_error_async(cx: &mut App, message: impl Into<String>) {
        if let Some(window) = cx.active_window() {
            let _ = window.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::error(message.into()).autohide(true),
                    cx
                );
            });
        }
    }

    pub fn is_edit_mode(&self) -> bool {
        self.is_edit_mode
    }
}

impl Focusable for DatabaseEditorView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DatabaseEditorView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let form_button = if self.current_tab == EditorTab::Form {
            Button::new("tab_form")
                .label("表单")
                .primary()
        } else {
            Button::new("tab_form")
                .label("表单")
                .ghost()
        };

        let sql_button = if self.current_tab == EditorTab::SqlPreview {
            Button::new("tab_sql")
                .label("SQL 预览")
                .primary()
        } else {
            Button::new("tab_sql")
                .label("SQL 预览")
                .ghost()
        };

        let main_content = if self.current_tab == EditorTab::Form {
            div()
                .flex_1()
                .w_full()
                .child(self.form.clone())
        } else {
            div()
                .flex_1()
                .w_full()
                .min_h_48()
                .p_4()
                .child(
                    Input::new(&self.sql_preview)
                        .size_full()
                        .disabled(true)
                )
        };

        v_flex()
            .size_full()
            .child(
                h_flex()
                    .gap_2()
                    .p_2()
                    .border_b_1()
                    .border_color(gpui::rgb(0xe0e0e0))
                    .child(
                        form_button.on_click(cx.listener(|this, _, _, cx| {
                            this.current_tab = EditorTab::Form;
                            cx.notify();
                        }))
                    )
                    .child(
                        sql_button.on_click(cx.listener(|this, _, _, cx| {
                            this.current_tab = EditorTab::SqlPreview;
                            cx.notify();
                        }))
                    )
            )
            .child(main_content)
    }
}
