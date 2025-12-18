mod database_editor_view;

pub use database_editor_view::DatabaseEditorView;

use db::plugin::DatabaseOperationRequest;

/// 数据库表单通用事件
/// 所有数据库类型的表单都应该发出这些事件
pub enum DatabaseFormEvent {
    FormChanged(DatabaseOperationRequest),
}
