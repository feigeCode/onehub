use gpui::{div, prelude::*, px, Context, IntoElement, ParentElement, Render, Styled, Window};
use gpui_component::{h_flex, v_flex, ActiveTheme, StyledExt};
use one_core::storage::StoredConnection;

/// Connection list panel showing all connections in a workspace
pub struct ConnectionListPanel {
    connections: Vec<StoredConnection>,
    workspace_name: Option<String>,
}

impl ConnectionListPanel {
    pub fn new(connections: Vec<StoredConnection>, workspace_name: Option<String>) -> Self {
        Self {
            connections,
            workspace_name,
        }
    }

    pub fn set_connections(&mut self, connections: Vec<StoredConnection>, workspace_name: Option<String>, cx: &mut Context<Self>) {
        self.connections = connections;
        self.workspace_name = workspace_name;
        cx.notify();
    }
}

impl Render for ConnectionListPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let workspace_title = self.workspace_name.as_ref()
            .map(|name| format!("工作区: {}", name))
            .unwrap_or_else(|| "所有连接".to_string());

        v_flex()
            .size_full()
            .gap_4()
            .p_4()
            .child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .text_xl()
                            .font_semibold()
                            .text_color(cx.theme().foreground)
                            .child(workspace_title)
                    )
            )
            .when(self.connections.is_empty(), |this| {
                this.child(
                    div()
                        .flex()
                        .flex_1()
                        .items_center()
                        .justify_center()
                        .text_color(cx.theme().muted_foreground)
                        .child("此工作区暂无连接")
                )
            })
            .when(!self.connections.is_empty(), |this| {
                this.child(
                    v_flex()
                        .flex_1()
                        .gap_2()
                        .children(self.connections.iter().map(|conn| {
                            let host_port = if let Ok(params) = conn.to_database_params() {
                                format!("{}:{}", params.host, params.port)
                            } else {
                                "-".to_string()
                            };
                            
                            let username = if let Ok(params) = conn.to_database_params() {
                                params.username
                            } else {
                                "-".to_string()
                            };
                            
                            let database = if let Ok(params) = conn.to_database_params() {
                                params.database.unwrap_or_else(|| "-".to_string())
                            } else {
                                "-".to_string()
                            };

                            div()
                                .p_3()
                                .bg(cx.theme().muted)
                                .rounded(px(8.0))
                                .child(
                                    v_flex()
                                        .gap_2()
                                        .child(
                                            h_flex()
                                                .justify_between()
                                                .child(
                                                    div()
                                                        .font_semibold()
                                                        .text_color(cx.theme().foreground)
                                                        .child(conn.name.clone())
                                                )
                                                .child(
                                                    div()
                                                        .text_sm()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child(format!("{:?}", conn.connection_type))
                                                )
                                        )
                                        .child(
                                            h_flex()
                                                .gap_4()
                                                .text_sm()
                                                .text_color(cx.theme().muted_foreground)
                                                .child(format!("主机: {}", host_port))
                                                .child(format!("用户: {}", username))
                                                .child(format!("数据库: {}", database))
                                        )
                                )
                        }))
                )
            })
    }
}

impl Clone for ConnectionListPanel {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            workspace_name: self.workspace_name.clone(),
        }
    }
}
