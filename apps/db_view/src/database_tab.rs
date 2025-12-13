use std::any::Any;

use db::GlobalDbState;
use gpui::{div, prelude::FluentBuilder, px, AnyElement, App, AppContext, Entity, FontWeight, Hsla, IntoElement, ParentElement, SharedString, Styled, Window};
use gpui_component::{h_flex, resizable::{h_resizable, resizable_panel}, v_flex, ActiveTheme, IconName};

use crate::database_objects_tab::DatabaseObjectsPanel;
use crate::db_tree_event::DatabaseEventHandler;
use crate::db_tree_view::DbTreeView;
use one_core::{storage::StoredConnection, tab_container::{TabContainer, TabContent, TabContentType, TabItem}};

// Database connection tab content - using TabContainer architecture
pub struct DatabaseTabContent {
    connections: Vec<StoredConnection>,
    tab_container: Entity<TabContainer>,
    db_tree_view: Entity<DbTreeView>,
    objects_panel: Entity<DatabaseObjectsPanel>,
    status_msg: Entity<String>,
    is_connected: Entity<bool>,
    event_handler: Option<Entity<DatabaseEventHandler>>,
    tab_name: Option< String>
}

impl DatabaseTabContent {

    pub fn new( connections: Vec<StoredConnection>, window: &mut Window, cx: &mut App) -> Self {
        Self::new_with_name(None, connections, window, cx)
    }
    pub fn new_with_name(tab_name: Option<String>, connections: Vec<StoredConnection>, window: &mut Window, cx: &mut App) -> Self {
        // Create database tree view
        let db_tree_view = cx.new(|cx| {
            DbTreeView::new(&connections, window, cx)
        });

        // Create tab container - use default theme colors for automatic theme switching
        let tab_container = cx.new(|cx| {
            TabContainer::new(window, cx)
        });

        // Create objects panel
        let objects_panel = cx.new(|cx| {
            DatabaseObjectsPanel::new(window, cx)
        });
        

        // Add objects panel to tab container
        tab_container.update(cx, |container, cx| {
            let panel_content = objects_panel.read(cx).clone();
            let tab = TabItem::new("objects-panel", panel_content);
            container.add_and_activate_tab(tab, cx);
        });

        let status_msg = cx.new(|_| "Ready".to_string());
        let is_connected = cx.new(|_| true);

        // Create event handler to handle tree view events
        let event_handler = cx.new(|cx| {
            DatabaseEventHandler::new(&db_tree_view, tab_container.clone(), objects_panel.clone(), window, cx)
        });

        // 注册连接配置到 GlobalDbState，然后自动连接
        let global_state = cx.global::<GlobalDbState>().clone();
        let connections_clone = connections.clone();

        cx.spawn(async move |_cx| {
            // 先注册所有连接
            for conn in &connections_clone {
                if let Ok(db_config) = conn.to_db_connection() {
                    let _ = global_state.register_connection(db_config).await;
                }
            }
        }).detach();

        Self {
            connections: connections.clone(),
            tab_container,
            db_tree_view,
            objects_panel,
            status_msg,
            is_connected,
            event_handler: Some(event_handler),
            tab_name
        }
    }

    fn render_connection_status(&self, cx: &mut App) -> AnyElement {
        let status_text = self.status_msg.read(cx).clone();
        let is_error = status_text.contains("Failed") || status_text.contains("failed");

        // 获取第一个连接信息用于显示
        let first_conn = self.connections.first();
        let conn_name = first_conn.map(|c| c.name.clone()).unwrap_or_else(|| "Unknown".to_string());
        let (conn_host, conn_port, conn_username, conn_database) = first_conn
            .and_then(|c| c.to_database_params().ok())
            .map(|p| (p.host, p.port, p.username, p.database))
            .unwrap_or_default();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_6()
            .child(
                // Loading animation or error icon
                div()
                    .w(px(64.0))
                    .h(px(64.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .w(px(48.0))
                            .h(px(48.0))
                            .rounded(px(24.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .when(!is_error, |this| {
                                // Loading animation - simple circle
                                this.border_4()
                                    .border_color(cx.theme().accent)
                                    .text_2xl()
                                    .text_color(cx.theme().accent)
                                    .child("⟳")
                            })
                            .when(is_error, |this| {
                                // Error state - red circle
                                this.bg(Hsla::red())
                                    .text_color(gpui::white())
                                    .text_2xl()
                                    .child("✕")
                            })
                    )
            )
            .child(
                div()
                    .text_xl()
                    .font_weight(FontWeight::BOLD)
                    .child(format!("Database Connection: {}", conn_name))
            )
            .child(
                v_flex()
                    .gap_2()
                    .p_4()
                    .bg(cx.theme().muted)
                    .rounded(px(8.0))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Host:")
                            )
                            .child(conn_host)
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Port:")
                            )
                            .child(format!("{}", conn_port))
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child("Username:")
                            )
                            .child(conn_username)
                    )
                    .when_some(conn_database, |this, db| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .child(
                                    div()
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .child("Database:")
                                )
                                .child(db)
                        )
                    })
            )
            .child(
                div()
                    .text_lg()
                    .when(!is_error, |this| {
                        this.text_color(cx.theme().accent)
                    })
                    .when(is_error, |this| {
                        this.text_color(Hsla::red())
                    })
                    .child(status_text)
            )
            .into_any_element()
    }
}

impl TabContent for DatabaseTabContent {
    fn title(&self) -> SharedString {
        if let Some(name) = self.tab_name.clone() {
            name.into()
        }else {
            self.connections.first()
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "Database".to_string())
                .into()
        }

    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::File)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        let is_connected_flag = *self.is_connected.read(cx);

        if !is_connected_flag {
            // Show loading/connection status
            self.render_connection_status(cx)
        } else {
            // Show layout with toolbar on top, resizable panels below
            v_flex()
                .size_full()
                .child(
                    h_resizable("db-panels")
                        .child(
                            resizable_panel()
                                .size(px(280.0))
                                .size_range(px(200.0)..px(500.0))
                                .child(self.db_tree_view.clone())
                        )
                        .child(
                            resizable_panel()
                                .child(self.tab_container.clone())
                        )
                )
                .into_any_element()
        }
    }

    fn content_type(&self) -> TabContentType {
        let name = self.connections.first()
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        TabContentType::Custom(format!("database-{}", name))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for DatabaseTabContent {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            tab_container: self.tab_container.clone(),
            db_tree_view: self.db_tree_view.clone(),
            objects_panel: self.objects_panel.clone(),
            status_msg: self.status_msg.clone(),
            is_connected: self.is_connected.clone(),
            event_handler: self.event_handler.clone(),
            tab_name: self.tab_name.clone(),
        }
    }
}
