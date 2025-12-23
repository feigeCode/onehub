use std::any::Any;

use gpui::{AnyElement, App, AppContext, Entity, FocusHandle, Focusable, Global, IntoElement, SharedString, Window};
use gpui_component::{ActiveTheme, IconName, Sizable, Size, Theme, ThemeMode, group_box::GroupBoxVariant, setting::{
    NumberFieldOptions, SettingField, SettingGroup, SettingItem, SettingPage, Settings,
}, Icon};
use one_core::tab_container::{TabContent, TabContentType};

use crate::settings::llm_providers_view::LlmProvidersView;

struct AppSettings {
    auto_switch_theme: bool,
    font_family: SharedString,
    font_size: f64,
    line_height: f64,
    notifications_enabled: bool,
    auto_update: bool,
    resettable: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            auto_switch_theme: false,
            font_family: "Arial".into(),
            font_size: 14.0,
            line_height: 12.0,
            notifications_enabled: true,
            auto_update: true,
            resettable: true,
        }
    }
}

impl Global for AppSettings {}

impl AppSettings {
    fn global(cx: &App) -> &AppSettings {
        cx.global::<AppSettings>()
    }

    pub fn global_mut(cx: &mut App) -> &mut AppSettings {
        cx.global_mut::<AppSettings>()
    }
}


pub struct SettingsTabContent {
    focus_handle: FocusHandle,
    llm_providers_view: Entity<LlmProvidersView>,
    size: Size,
    group_variant: GroupBoxVariant,
}

impl SettingsTabContent {
    pub fn new(_window: &mut Window, cx: &mut App) -> Self {
        let llm_providers_view = cx.new(|cx| LlmProvidersView::new(cx));
        Self {
            focus_handle: cx.focus_handle(),
            llm_providers_view,
            size: Size::default(),
            group_variant: GroupBoxVariant::Outline,
        }
    }

    fn setting_pages(&self, _window: &mut Window, _cx: &App) -> Vec<SettingPage> {
        let llm_view = self.llm_providers_view.clone();
        let default_settings = AppSettings::default();
        let resettable = AppSettings::global(_cx).resettable;
        
        vec![
            SettingPage::new("通用")
                .resettable(resettable)
                .default_open(true)
                .groups(vec![
                    SettingGroup::new().title("外观").items(vec![
                        SettingItem::new(
                            "深色模式",
                            SettingField::switch(
                                |cx: &App| cx.theme().mode.is_dark(),
                                |val: bool, cx: &mut App| {
                                    let mode = if val {
                                        ThemeMode::Dark
                                    } else {
                                        ThemeMode::Light
                                    };
                                    Theme::global_mut(cx).mode = mode;
                                    Theme::change(mode, None, cx);
                                },
                            )
                            .default_value(false),
                        )
                        .description("在浅色和深色主题之间切换"),
                        SettingItem::new(
                            "自动切换主题",
                            SettingField::checkbox(
                                |cx: &App| AppSettings::global(cx).auto_switch_theme,
                                |val: bool, cx: &mut App| {
                                    AppSettings::global_mut(cx).auto_switch_theme = val;
                                },
                            )
                            .default_value(default_settings.auto_switch_theme),
                        )
                        .description("根据系统设置自动切换主题"),
                    ]),
                    SettingGroup::new()
                        .title("字体")
                        .item(
                            SettingItem::new(
                                "字体系列",
                                SettingField::dropdown(
                                    vec![
                                        ("Arial".into(), "Arial".into()),
                                        ("Helvetica".into(), "Helvetica".into()),
                                        ("Times New Roman".into(), "Times New Roman".into()),
                                        ("Courier New".into(), "Courier New".into()),
                                    ],
                                    |cx: &App| AppSettings::global(cx).font_family.clone(),
                                    |val: SharedString, cx: &mut App| {
                                        AppSettings::global_mut(cx).font_family = val;
                                    },
                                )
                                .default_value(default_settings.font_family),
                            )
                            .description("选择应用的字体系列"),
                        )
                        .item(
                            SettingItem::new(
                                "字体大小",
                                SettingField::number_input(
                                    NumberFieldOptions {
                                        min: 8.0,
                                        max: 72.0,
                                        ..Default::default()
                                    },
                                    |cx: &App| AppSettings::global(cx).font_size,
                                    |val: f64, cx: &mut App| {
                                        AppSettings::global_mut(cx).font_size = val;
                                    },
                                )
                                .default_value(default_settings.font_size),
                            )
                            .description("调整字体大小以获得更好的可读性（8-72）"),
                        ),
                ]),
            SettingPage::new("LLM 提供商")
                .group(
                    SettingGroup::new()
                        .item(SettingItem::render(move |_options, _window, _cx| {
                            llm_view.clone().into_any_element()
                        }))
                ),
        ]
    }
}

impl Focusable for SettingsTabContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl TabContent for SettingsTabContent {
    fn title(&self) -> SharedString {
        "设置".into()
    }

    fn icon(&self) -> Option<Icon> {
        Some(IconName::Settings.color())
    }

    fn closeable(&self) -> bool {
        true
    }

    fn render_content(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        // 确保全局设置已初始化
        if !cx.has_global::<AppSettings>() {
            cx.set_global::<AppSettings>(AppSettings::default());
        }
        
        // 使用稳定的ID确保Settings组件状态持久化
        Settings::new("main-app-settings")
            .with_size(self.size)
            .with_group_variant(self.group_variant)
            .pages(self.setting_pages(window, cx))
            .into_any_element()
    }

    fn on_activate(&self, _window: &mut Window, cx: &mut App) {
        // 确保全局设置在tab激活时已初始化
        if !cx.has_global::<AppSettings>() {
            cx.set_global::<AppSettings>(AppSettings::default());
        }
    }

    fn content_type(&self) -> TabContentType {
        TabContentType::Custom("settings".to_string())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}