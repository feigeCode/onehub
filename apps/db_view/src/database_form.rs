use std::collections::HashMap;

use gpui::{div, px, App, AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, ParentElement, Render, SharedString, Styled, Window};
use gpui_component::{
    form::{field, v_form},
    input::{Input, InputState},
    select::{Select, SelectItem, SelectState},
    v_flex, IndexPath, Sizable, Size,
};
use db::plugin::{DatabaseFormConfig, DatabaseFormFieldType, DatabaseOperationRequest};

/// Select item for database form dropdown fields
#[derive(Clone, Debug)]
pub struct DatabaseFormSelectItem {
    pub value: String,
}

impl DatabaseFormSelectItem {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl SelectItem for DatabaseFormSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        self.value.clone().into()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

pub enum DatabaseFormEvent {
    Save(DatabaseOperationRequest),
    Cancel,
}

/// Database form for create/edit operations
pub struct DatabaseForm {
    config: DatabaseFormConfig,
    database_name: String,
    focus_handle: FocusHandle,
    field_inputs: Vec<Entity<InputState>>,
    field_selects: Vec<Entity<SelectState<Vec<DatabaseFormSelectItem>>>>,
}

impl DatabaseForm {
    pub fn new(config: DatabaseFormConfig, database_name: String, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        // Initialize inputs and selects
        let mut field_inputs = Vec::new();
        let mut field_selects = Vec::new();

        for field in &config.fields {
            let default_value = field.default_value.clone().unwrap_or_default();

            match &field.field_type {
                DatabaseFormFieldType::Text => {
                    let input = cx.new(|cx| {
                        let mut input_state = InputState::new(window, cx);
                        if let Some(placeholder) = &field.placeholder {
                            input_state = input_state.placeholder(placeholder);
                        }
                        input_state.set_value(default_value, window, cx);
                        input_state
                    });

                    // Input changes are handled automatically by InputState

                    field_inputs.push(input);
                    field_selects.push(cx.new(|cx| SelectState::new(Vec::<DatabaseFormSelectItem>::new(), None, window, cx))); // Placeholder
                }
                DatabaseFormFieldType::Select(options) => {
                    let items: Vec<DatabaseFormSelectItem> = options
                        .iter()
                        .map(|opt| DatabaseFormSelectItem::new(opt.clone()))
                        .collect();
                    
                    let default_index = if !default_value.is_empty() {
                        items.iter().position(|item| item.value == default_value)
                            .map(|i| IndexPath::new(i))
                    } else {
                        Some(IndexPath::new(0))
                    };

                    let select = cx.new(|cx| {
                        SelectState::new(items, default_index, window, cx)
                    });

                    // Note: Select change handling will be done in render method

                    field_selects.push(select);
                    field_inputs.push(cx.new(|cx| InputState::new(window, cx))); // Placeholder
                }
            }
        }

        Self {
            config,
            database_name,
            focus_handle,
            field_inputs,
            field_selects,
        }
    }

    fn get_field_value(&self, field_name: &str, cx: &App) -> String {
        // Find the field index
        if let Some((field_index, field)) = self.config.fields.iter().enumerate().find(|(_, f)| f.name == field_name) {
            match &field.field_type {
                DatabaseFormFieldType::Text => {
                    self.field_inputs[field_index].read(cx).text().to_string()
                }
                DatabaseFormFieldType::Select(_) => {
                    self.field_selects[field_index].read(cx).selected_value().cloned().unwrap_or_default()
                }
            }
        } else {
            String::new()
        }
    }

    fn build_request(&self, cx: &App) -> DatabaseOperationRequest {
        let mut field_values = HashMap::new();
        
        for field in &self.config.fields {
            let value = self.get_field_value(&field.name, cx);
            if !value.is_empty() {
                field_values.insert(field.name.clone(), value);
            }
        }

        DatabaseOperationRequest {
            database_name: self.database_name.clone(),
            field_values,
        }
    }

    fn validate(&self, cx: &App) -> Result<(), String> {
        for field in &self.config.fields {
            if field.required {
                let value = self.get_field_value(&field.name, cx);
                if value.trim().is_empty() {
                    return Err(format!("{} 是必填项", field.label));
                }
            }
        }
        Ok(())
    }

    pub fn trigger_save(&mut self, cx: &mut Context<Self>) {
        if let Err(e) = self.validate(cx) {
            // TODO: Show error message
            eprintln!("Validation error: {}", e);
            return;
        }

        let request = self.build_request(cx);
        cx.emit(DatabaseFormEvent::Save(request));
    }

    pub fn trigger_cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DatabaseFormEvent::Cancel);
    }
}

impl EventEmitter<DatabaseFormEvent> for DatabaseForm {}

impl Focusable for DatabaseForm {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DatabaseForm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_4()
            .size_full()
            .child(
                // Form fields
                div()
                    .flex_1()
                    .min_h(px(200.))
                    .child(
                        v_form()
                            .with_size(Size::Small)
                            .columns(1)
                            .label_width(px(100.))
                            .children(
                                self.config.fields
                                    .iter()
                                    .enumerate()
                                    .map(|(i, field_info)| {
                                        let field_element = match &field_info.field_type {
                                            DatabaseFormFieldType::Text => {
                                                div().child(Input::new(&self.field_inputs[i]).w_full())
                                            }
                                            DatabaseFormFieldType::Select(_) => {
                                                div().child(Select::new(&self.field_selects[i]).w_full())
                                            }
                                        };

                                        field()
                                            .label(field_info.label.clone())
                                            .required(field_info.required)
                                            .items_center()
                                            .label_justify_end()
                                            .child(field_element)
                                    }),
                            ),
                    ),
            )
    }
}