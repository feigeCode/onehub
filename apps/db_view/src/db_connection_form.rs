use anyhow::Error;
use db::GlobalDbState;
use gpui::{div, prelude::*, px, App, AsyncApp, Axis, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement, PathPromptOptions, Render, SharedString, Styled, Window};
use gpui_component::{
    button::{Button, ButtonVariants as _},
    form::{field, v_form},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectItem, SelectState},
    tab::{Tab, TabBar},
    v_flex, ActiveTheme, IconName, Sizable, Size,
};
use one_core::gpui_tokio::Tokio;
use one_core::storage::{get_config_dir, DatabaseType, DbConnectionConfig, StoredConnection, Workspace};

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
    TextArea,
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
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
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
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
            ],
        }
    }

    /// MSSQL (SQL Server) form configuration
    pub fn mssql() -> Self {
        Self {
            db_type: DatabaseType::MSSQL,
            title: "新建连接 (SQL Server)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My SQL Server Database")
                        .default("Local SQL Server"),
                    FormField::new("host", "主机", FormFieldType::Text)
                        .placeholder("localhost")
                        .default("localhost"),
                    FormField::new("port", "端口", FormFieldType::Number)
                        .placeholder("1433")
                        .default("1433"),
                    FormField::new("username", "用户名", FormFieldType::Text)
                        .placeholder("sa")
                        .default("sa"),
                    FormField::new("password", "密码", FormFieldType::Password)
                        .placeholder("Enter password"),
                    FormField::new("database", "数据库", FormFieldType::Text)
                        .optional()
                        .placeholder("database name (optional)"),
                ]),
                TabGroup::new("advanced", "高级"),
                TabGroup::new("ssl", "SSL"),
                TabGroup::new("ssh", "SSH"),
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
            ],
        }
    }

    /// Oracle form configuration
    pub fn oracle() -> Self {
        Self {
            db_type: DatabaseType::Oracle,
            title: "新建连接 (Oracle)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My Oracle Database")
                        .default("Local Oracle"),
                    FormField::new("host", "主机", FormFieldType::Text)
                        .placeholder("localhost")
                        .default("localhost"),
                    FormField::new("port", "端口", FormFieldType::Number)
                        .placeholder("1521")
                        .default("1521"),
                    FormField::new("username", "用户名", FormFieldType::Text)
                        .placeholder("system")
                        .default("system"),
                    FormField::new("password", "密码", FormFieldType::Password)
                        .placeholder("Enter password"),
                    FormField::new("service_name", "Service Name", FormFieldType::Text)
                        .optional()
                        .placeholder("ORCL (或使用 SID)"),
                    FormField::new("sid", "SID", FormFieldType::Text)
                        .optional()
                        .placeholder("orcl (或使用 Service Name)"),
                ]),
                TabGroup::new("advanced", "高级"),
                TabGroup::new("ssl", "SSL"),
                TabGroup::new("ssh", "SSH"),
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
            ],
        }
    }

    /// ClickHouse form configuration
    pub fn clickhouse() -> Self {
        Self {
            db_type: DatabaseType::ClickHouse,
            title: "新建连接 (ClickHouse)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My ClickHouse Database")
                        .default("Local ClickHouse"),
                    FormField::new("host", "主机", FormFieldType::Text)
                        .placeholder("localhost")
                        .default("localhost"),
                    FormField::new("port", "端口", FormFieldType::Number)
                        .placeholder("9000")
                        .default("9000"),
                    FormField::new("username", "用户名", FormFieldType::Text)
                        .placeholder("default")
                        .default("default"),
                    FormField::new("password", "密码", FormFieldType::Password)
                        .placeholder("Enter password"),
                    FormField::new("database", "数据库", FormFieldType::Text)
                        .optional()
                        .placeholder("database name (optional)"),
                ]),
                TabGroup::new("advanced", "高级"),
                TabGroup::new("ssl", "SSL"),
                TabGroup::new("ssh", "SSH"),
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
            ],
        }
    }

    /// SQLite form configuration
    pub fn sqlite() -> Self {
        let default_db_path = get_config_dir()
            .map(|p| p.join("onehub_default.db").to_string_lossy().to_string())
            .unwrap_or_else(|_| "onehub_default.db".to_string());

        Self {
            db_type: DatabaseType::SQLite,
            title: "新建连接 (SQLite)".to_string(),
            tab_groups: vec![
                TabGroup::new("general", "常规").fields(vec![
                    FormField::new("name", "连接名称", FormFieldType::Text)
                        .placeholder("My SQLite Database")
                        .default("Local SQLite"),
                    FormField::new("host", "数据库文件路径", FormFieldType::Text)
                        .placeholder("/path/to/database.db")
                        .default(default_db_path),
                ]),
                TabGroup::new("notes", "备注").fields(vec![
                    FormField::new("remark", "备注", FormFieldType::TextArea)
                        .optional()
                        .placeholder("输入连接备注信息...")
                        .default(""),
                ]),
            ],
        }
    }
}

/// Database connection form modal
pub struct DbConnectionForm {
    config: DbFormConfig,
    current_db_type: Entity<DatabaseType>,
    focus_handle: FocusHandle,
    active_tab: usize,
    field_values: Vec<(String, Entity<String>)>,
    field_inputs: Vec<Entity<InputState>>,
    is_testing: Entity<bool>,
    test_result: Entity<Option<Result<bool, String>>>,
    workspace_select: Entity<SelectState<Vec<WorkspaceSelectItem>>>,
    pending_file_path: Entity<Option<String>>,
    editing_connection: Option<StoredConnection>,
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

                    if field.field_type == FormFieldType::Password {
                        input_state = input_state.masked(true);
                    }

                    if field.field_type == FormFieldType::TextArea {
                        input_state = input_state.auto_grow(5, 15);
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

        let pending_file_path = cx.new(|_| None);

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
            pending_file_path,
            editing_connection: None,
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
        self.editing_connection = Some(connection.clone());
        self.set_field_value("name", &connection.name, window, cx);

        if let Ok(params) = connection.to_db_connection() {
            self.set_field_value("host", &params.host, window, cx);
            self.set_field_value("port", &params.port.to_string(), window, cx);
            self.set_field_value("username", &params.username, window, cx);
            self.set_field_value("password", &params.password, window, cx);
            if let Some(db) = &params.database {
                self.set_field_value("database", db, window, cx);
            }
        }

        if let Some(remark) = &connection.remark {
            self.set_field_value("remark", remark, window, cx);
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
            database_type: *self.current_db_type.read(cx),
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

        let global_state = cx.global::<GlobalDbState>().clone();
        let test_result_handle = self.test_result.clone();
        let is_testing_handle = self.is_testing.clone();

        cx.spawn(async move |_, cx: &mut AsyncApp| {
            let manager = global_state.db_manager;

            let test_result = Tokio::spawn_result(cx, async move {
                let db_plugin = manager.get_plugin(&db_type)?;
                let conn = db_plugin.create_connection(connection).await?;
                conn.ping().await?;
                Ok::<bool, Error>(true)
            });

            let result_msg = match test_result {
                Ok(_) => Ok(true),
                Err(_) => Err("测试连接失败".to_string()),
            };

            let _ = cx.update(|cx| {
                is_testing_handle.update(cx, |testing, cx| {
                    *testing = false;
                    cx.notify();
                });
                test_result_handle.update(cx, |result, cx| {
                    *result = Some(result_msg);
                    cx.notify();
                });
            });
        }).detach();
    }

    pub fn build_stored_connection(&self, cx: &App) -> Result<(StoredConnection, bool), String> {
        self.validate(cx)?;

        let connection = self.build_connection(cx);
        let remark = self.get_field_value("remark", cx);
        let remark_opt = if remark.is_empty() { None } else { Some(remark) };
        let is_update = self.editing_connection.is_some();

        let mut stored = match &self.editing_connection {
            Some(conn) => {
                let mut c = conn.clone();
                c.name = connection.name.clone();
                c.workspace_id = connection.workspace_id;
                c.params = serde_json::to_string(&connection)
                    .map_err(|e| format!("序列化连接参数失败: {}", e))?;
                c
            }
            None => StoredConnection::from_db_connection(connection),
        };

        stored.remark = remark_opt;
        Ok((stored, is_update))
    }

    pub fn set_save_error(&mut self, error: String, cx: &mut Context<Self>) {
        self.test_result.update(cx, |result, cx| {
            *result = Some(Err(error));
            cx.notify();
        });
    }

    pub fn trigger_cancel(&mut self, _cx: &mut Context<Self>) {
        self.editing_connection = None;
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

    fn browse_file_path(&mut self, _window: &mut Window, cx: &mut App) {
        let pending = self.pending_file_path.clone();

        let future = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            multiple: false,
            directories: false,
            prompt: Some("选择SQLite数据库文件".into()),
        });

        cx.spawn(async move |cx| {
            if let Ok(Ok(Some(paths))) = future.await {
                if let Some(path) = paths.first() {
                    let path_str = path.to_string_lossy().to_string();
                    let _ = cx.update(|cx| {
                        pending.update(cx, |p, cx| {
                            *p = Some(path_str);
                            cx.notify();
                        });
                    });
                }
            }
        })
        .detach();
    }

    fn get_input_by_name(&self, field_name: &str) -> Option<&Entity<InputState>> {
        let mut idx = 0;
        for tab_group in &self.config.tab_groups {
            for field in &tab_group.fields {
                if field.name == field_name {
                    return self.field_inputs.get(idx);
                }
                idx += 1;
            }
        }
        None
    }
}

impl Focusable for DbConnectionForm {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DbConnectionForm {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Check if there's a pending file path to apply
        if let Some(path) = self.pending_file_path.read(cx).clone() {
            if let Some(host_input) = self.get_input_by_name("host").cloned() {
                host_input.update(cx, |state, cx| {
                    state.set_value(path, window, cx);
                });
            }
            self.pending_file_path.update(cx, |p, _| *p = None);
        }

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
                        let db_type = self.config.db_type;

                        this.child(
                            v_form()
                                .layout(Axis::Horizontal)
                                .with_size(Size::Medium)
                                .columns(1)
                                .label_width(px(100.))
                                .children(
                                    current_tab_fields
                                        .iter()
                                        .enumerate()
                                        .map(|(i, field_info)| {
                                            let input_idx = field_input_offset + i;
                                            let is_sqlite_path = db_type == DatabaseType::SQLite && field_info.name == "host";
                                            let is_textarea = field_info.field_type == FormFieldType::TextArea;

                                            field()
                                                .label(field_info.label.clone())
                                                .required(field_info.required)
                                                .when(!is_textarea, |f| f.items_center())
                                                .when(is_textarea, |f| f.items_start())
                                                .label_justify_end()
                                                .child(
                                                    h_flex()
                                                        .w_full()
                                                        .gap_2()
                                                        .child(Input::new(&self.field_inputs[input_idx]).w_full())
                                                        .when(is_sqlite_path, |el| {
                                                            el.child(
                                                                Button::new("browse-file")
                                                                    .icon(IconName::FolderOpen)
                                                                    .ghost()
                                                                    .on_click(cx.listener(|this, _, window, cx| {
                                                                        this.browse_file_path(window, cx);
                                                                    }))
                                                            )
                                                        })
                                                )
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
