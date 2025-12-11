use gpui::{div, prelude::*, px, App, AppContext, Axis, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, Window};
use gpui_component::{
    form::{field, v_form}
    ,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectItem, SelectState},
    tab::{Tab, TabBar},
    v_flex, ActiveTheme, Sizable, Size,
};
use one_core::storage::{DatabaseType, DbConnectionConfig, StoredConnection, Workspace};

/// Workspace select item for dropdown
#[derive(Clone, Debug)]
pub struct WorkspaceSelectItem {
    pub id: Option<i64>,
    pub name: String,
}

impl WorkspaceSelectItem {
    pub fn none() -> Self {
        Self {
            id: None,
            name: "无".to_string(),
        }
    }

    pub fn from_workspace(ws: &Workspace) -> Self {
        Self {
            id: ws.id,
            name: ws.name.clone(),
        }
    }
}

impl SelectItem for WorkspaceSelectItem {
    type Value = Option<i64>;

    fn title(&self) -> SharedString {
        self.name.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.id
    }
}

/// Represents a tab group containing multiple fields
#[derive(Clone, Debug)]
pub struct TabGroup {
    pub name: String,
    pub label: String,
    pub fields: Vec<FormField>,
}

impl TabGroup {
    pub fn new(name: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, field: FormField) -> Self {
        self.fields.push(field);
        self
    }

    pub fn fields(mut self, fields: Vec<FormField>) -> Self {
        self.fields = fields;
        self
    }
}

/// Represents a field in the connection form
#[derive(Clone, Debug)]
pub struct FormField {
    pub name: String,
    pub label: String,
    pub placeholder: String,
    pub field_type: FormFieldType,
    pub required: bool,
    pub default_value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FormFieldType {
    Text,
    Number,
    Password,
}

impl FormField {
    pub fn new(
        name: impl Into<String>,
        label: impl Into<String>,
        field_type: FormFieldType,
    ) -> Self {
        let name = name.into();
        Self {
            placeholder: format!("Enter {}", name.to_lowercase()),
            name,
            label: label.into(),
            field_type,
            required: true,
            default_value: String::new(),
        }
    }

    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = placeholder.into();
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default_value = value.into();
        self
    }
}

/// Database connection form configuration for different database types
pub struct DbFormConfig {
    pub db_type: DatabaseType,
    pub title: String,
    pub tab_groups: Vec<TabGroup>,
}

impl DbFormConfig {
    /// MySQL form configuration
    pub fn mysql() -> Self {
        Self {
            db_type: DatabaseType::MySQL,
            title: "新建连接 (MySQL)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My MySQL Database")
                        .default("Local MySQL"),
                    FormField::new("host", "主机", FormFieldType::Text)
                        .placeholder("localhost")
                        .default("localhost"),
                    FormField::new("port", "端口", FormFieldType::Number)
                        .placeholder("3306")
                        .default("3306"),
                    FormField::new("username", "用户名", FormFieldType::Text)
                        .placeholder("root")
                        .default("root"),
                    FormField::new("password", "密码", FormFieldType::Password)
                        .placeholder("Enter password")
                        .default("hf123456"),
                    FormField::new("database", "数据库", FormFieldType::Text)
                        .optional()
                        .placeholder("database name (optional)")
                        .default("ai_app"),
                ]),
                TabGroup::new("advanced", "高级"),
                TabGroup::new("ssl", "SSL"),
                TabGroup::new("ssh", "SSH"),
                TabGroup::new("notes", "备注"),
            ],
        }
    }

    /// PostgreSQL form configuration
    pub fn postgres() -> Self {
        Self {
            db_type: DatabaseType::PostgreSQL,
            title: "新建连接 (PostgreSQL)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My PostgreSQL Database")
                        .default("Local PostgreSQL"),
                    FormField::new("host", "主机", FormFieldType::Text)
                        .placeholder("localhost")
                        .default("localhost"),
                    FormField::new("port", "端口", FormFieldType::Number)
                        .placeholder("5432")
                        .default("5432"),
                    FormField::new("username", "用户名", FormFieldType::Text)
                        .placeholder("postgres")
                        .default("postgres"),
                    FormField::new("password", "密码", FormFieldType::Password)
                        .placeholder("Enter password"),
                    FormField::new("database", "数据库", FormFieldType::Text)
                        .optional()
                        .placeholder("database name (optional)"),
                ]),
                TabGroup::new("advanced", "高级"),
                TabGroup::new("ssl", "SSL"),
                TabGroup::new("ssh", "SSH"),
                TabGroup::new("notes", "备注"),
            ],
        }
    }
}

pub enum DbConnectionFormEvent {
    TestConnection(DatabaseType, DbConnectionConfig),
    Save(DatabaseType, DbConnectionConfig),
    Cancel,
}

/// Database connection form modal
pub struct DbConnectionForm {
    config: DbFormConfig,
    current_db_type: Entity<DatabaseType>,
    focus_handle: FocusHandle,
    active_tab: usize,
    // Field values stored as Entity<String> for reactivity
    field_values: Vec<(String, Entity<String>)>,
    field_inputs: Vec<Entity<InputState>>,
    is_testing: Entity<bool>,
    test_result: Entity<Option<Result<bool, String>>>,
    workspace_select: Entity<SelectState<Vec<WorkspaceSelectItem>>>,
}

impl DbConnectionForm {
    pub fn new(config: DbFormConfig, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let current_db_type = cx.new(|_| config.db_type);

        // Initialize field values and inputs
        let mut field_values = Vec::new();
        let mut field_inputs = Vec::new();

        for tab_group in &config.tab_groups {
            for field in &tab_group.fields {
                let value = cx.new(|_| field.default_value.clone());
                field_values.push((field.name.clone(), value.clone()));

                let input = cx.new(|cx| {
                    let mut input_state = InputState::new(window, cx)
                        .placeholder(&field.placeholder);

                    // Set password mode if needed
                    if field.field_type == FormFieldType::Password {
                        input_state = input_state.masked(true);
                    }

                    input_state.set_value(field.default_value.clone(), window, cx);
                    input_state
                });

                // Subscribe to input changes
                let value_clone = value.clone();
                cx.subscribe_in(&input, window, move |_form, _input, event, _window, cx| {
                    if let InputEvent::Change = event {
                        value_clone.update(cx, |v, cx| {
                            // Get the new text from the input
                            *v = _input.read(cx).text().to_string();
                            cx.notify();
                        });
                    }
                })
                .detach();

                field_inputs.push(input);
            }
        }

        let is_testing = cx.new(|_| false);
        let test_result = cx.new(|_| None);
        
        let workspace_items = vec![WorkspaceSelectItem::none()];
        let workspace_select = cx.new(|cx| {
            SelectState::new(workspace_items, Some(Default::default()), window, cx)
        });

        Self {
            config,
            current_db_type,
            focus_handle,
            active_tab: 0,
            field_values,
            field_inputs,
            is_testing,
            test_result,
            workspace_select,
        }
    }

    pub fn set_workspaces(&mut self, workspaces: Vec<Workspace>, window: &mut Window, cx: &mut Context<Self>) {
        let mut items = vec![WorkspaceSelectItem::none()];
        items.extend(workspaces.iter().map(WorkspaceSelectItem::from_workspace));
        
        self.workspace_select.update(cx, |select, cx| {
            select.set_items(items, window, cx);
        });
        cx.notify();
    }

    pub fn load_connection(&mut self, connection: &StoredConnection, window: &mut Window, cx: &mut Context<Self>) {
        // Update form values from connection
        self.set_field_value("name", &connection.name, window, cx);
        
        // Parse database params
        if let Ok(params) = connection.to_database_params() {
            self.set_field_value("host", &params.host, window, cx);
            self.set_field_value("port", &params.port.to_string(), window, cx);
            self.set_field_value("username", &params.username, window, cx);
            self.set_field_value("password", &params.password, window, cx);
            if let Some(db) = &params.database {
                self.set_field_value("database", db, window, cx);
            }
        }
        
        if let Some(ws_id) = connection.workspace_id {
            self.workspace_select.update(cx, |select, cx| {
                select.set_selected_value(&Some(ws_id), window, cx);
            });
        } else {
            self.workspace_select.update(cx, |select, cx| {
                select.set_selected_value(&None, window, cx);
            });
        }
    }

    fn set_field_value(&mut self, field_name: &str, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some((idx, _)) = self.field_values.iter().enumerate().find(|(_, (name, _))| name == field_name) {
            self.field_values[idx].1.update(cx, |v, cx| {
                *v = value.to_string();
                cx.notify();
            });
            self.field_inputs[idx].update(cx, |input, cx| {
                input.set_value(value.to_string(), window, cx);
            });
        }
    }

    fn get_field_value(&self, field_name: &str, cx: &App) -> String {
        self.field_values
            .iter()
            .find(|(name, _)| name == field_name)
            .map(|(_, value)| value.read(cx).clone())
            .unwrap_or_default()
    }

    fn build_connection(&self, cx: &App) -> DbConnectionConfig {
        let workspace_id = self.workspace_select.read(cx)
            .selected_value()
            .cloned()
            .flatten();
        
        DbConnectionConfig {
            id: String::new(),
            database_type: self.current_db_type.read(cx).clone(),
            name: self.get_field_value("name", cx),
            host: self.get_field_value("host", cx),
            port: self
                .get_field_value("port", cx)
                .parse()
                .unwrap_or(3306),
            username: self.get_field_value("username", cx),
            password: self.get_field_value("password", cx),
            database: {
                let db = self.get_field_value("database", cx);
                if db.is_empty() {
                    None
                } else {
                    Some(db)
                }
            },
            workspace_id,
        }
    }

    fn validate(&self, cx: &App) -> Result<(), String> {
        for tab_group in &self.config.tab_groups {
            for field in &tab_group.fields {
                if field.required {
                    let value = self.get_field_value(&field.name, cx);
                    if value.trim().is_empty() {
                        return Err(format!("{} is required", field.label));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn trigger_test_connection(&mut self, cx: &mut Context<Self>) {
        if let Err(e) = self.validate(cx) {
            self.test_result.update(cx, |result, cx| {
                *result = Some(Err(e));
                cx.notify();
            });
            return;
        }

        let connection = self.build_connection(cx);
        let db_type = *self.current_db_type.read(cx);

        self.is_testing.update(cx, |testing, cx| {
            *testing = true;
            cx.notify();
        });

        cx.emit(DbConnectionFormEvent::TestConnection(db_type, connection));
    }

    pub fn trigger_save(&mut self, cx: &mut Context<Self>) {
        if let Err(e) = self.validate(cx) {
            self.test_result.update(cx, |result, cx| {
                *result = Some(Err(e));
                cx.notify();
            });
            return;
        }

        let connection = self.build_connection(cx);
        let db_type = *self.current_db_type.read(cx);
        cx.emit(DbConnectionFormEvent::Save(db_type, connection));
    }

    pub fn trigger_cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DbConnectionFormEvent::Cancel);
    }

    pub fn is_testing(&self, cx: &App) -> bool {
        *self.is_testing.read(cx)
    }

    pub fn set_test_result(&mut self, result: Result<bool, String>, cx: &mut Context<Self>) {
        self.is_testing.update(cx, |testing, cx| {
            *testing = false;
            cx.notify();
        });
        self.test_result.update(cx, |test_result, cx| {
            *test_result = Some(result);
            cx.notify();
        });
    }
}

impl EventEmitter<DbConnectionFormEvent> for DbConnectionForm {}

impl Focusable for DbConnectionForm {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DbConnectionForm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let test_result_msg = self.test_result.read(cx).as_ref().map(|r| match r {
            Ok(true) => "✓ 连接成功!".to_string(),
            Ok(false) => "✗ 连接失败".to_string(),
            Err(e) => format!("✗ {}", e),
        });

        // Calculate field input indices for current tab
        let mut field_input_offset = 0;
        for (tab_idx, tab_group) in self.config.tab_groups.iter().enumerate() {
            if tab_idx < self.active_tab {
                field_input_offset += tab_group.fields.len();
            }
        }

        let current_tab_fields = &self.config.tab_groups[self.active_tab].fields;

        v_flex()
            .gap_4()
            .size_full()
            .child(
                // Tab bar
                div()
                    .flex()
                    .justify_center()
                    .child(
                        TabBar::new("connection-tabs")
                            .with_size(Size::Large)
                            .underline()
                            .selected_index(self.active_tab)
                            .on_click(cx.listener(|this, ix: &usize, _window, cx| {
                                this.active_tab = *ix;
                                cx.notify();
                            }))
                            .children(
                                self.config
                                    .tab_groups
                                    .iter()
                                    .map(|tab| Tab::new().label(tab.label.clone())),
                            ),
                    ),
            )
            .child(
                // Form fields for active tab
                div()
                    .flex_1()
                    .min_h(px(250.))
                    .when(!current_tab_fields.is_empty(), |this| {
                        let is_general_tab = self.active_tab == 0;

                        this.child(
                            v_form()
                                .layout(Axis::Horizontal)
                                .with_size(Size::Small)
                                .columns(1)
                                .label_width(px(100.))
                                .children(
                                    current_tab_fields
                                        .iter()
                                        .enumerate()
                                        .map(|(i, field_info)| {
                                            let input_idx = field_input_offset + i;
                                            field()
                                                .label(field_info.label.clone())
                                                .required(field_info.required)
                                                .items_center()
                                                .label_justify_end()
                                                .child(Input::new(&self.field_inputs[input_idx]).w_full())
                                        }),
                                )
                                .when(is_general_tab, |form| {
                                    form.child(
                                        field()
                                            .label("工作区")
                                            .items_center()
                                            .label_justify_end()
                                            .child(Select::new(&self.workspace_select).w_full())
                                    )
                                }),
                        )
                    })
                    .when(current_tab_fields.is_empty(), |this| {
                        this.child(
                            div()
                                .flex()
                                .items_center()
                                .justify_center()
                                .h_full()
                                .text_color(cx.theme().muted_foreground)
                                .child("此页签暂无配置项"),
                        )
                    }),
            )
            .child(
                // Test result message area
                div()
                    .h(px(40.))
                    .flex()
                    .items_center()
                    .when_some(test_result_msg, |this, msg| {
                        let is_success = msg.starts_with("✓");
                        this.child(
                            div()
                                .w_full()
                                .px_3()
                                .py_2()
                                .rounded_md()
                                .bg(if is_success {
                                    gpui::rgb(0xdcfce7)
                                } else {
                                    gpui::rgb(0xfee2e2)
                                })
                                .text_color(if is_success {
                                    gpui::rgb(0x166534)
                                } else {
                                    gpui::rgb(0x991b1b)
                                })
                                .child(msg),
                        )
                    }),
            )
    }
}
