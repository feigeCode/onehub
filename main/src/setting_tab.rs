use std::any::Any;
use std::sync::Arc;

use gpui::{AnyElement, App, AppContext, Entity, IntoElement, SharedString, Window};
use gpui_component::IconName;
use one_core::tab_container::{TabContent, TabContentType};

use crate::settings::llm_providers_view::LlmProvidersView;

pub struct SettingsTabContent {
    llm_providers_view: Entity<LlmProvidersView>,
}

impl SettingsTabContent {
    pub fn new(_window: &mut Window, cx: &mut App) -> Self {
        let llm_providers_view = cx.new(|cx| LlmProvidersView::new(cx));
        Self {
            llm_providers_view,
        }
    }
}

impl TabContent for SettingsTabContent {
    fn title(&self) -> SharedString {
        "设置".into()
    }

    fn icon(&self) -> Option<IconName> {
        Some(IconName::Settings)
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, _window: &mut Window, _cx: &mut App) -> AnyElement {
        self.llm_providers_view.clone().into_any_element()
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::Custom("settings".to_string())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}