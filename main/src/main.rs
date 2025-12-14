mod onehup_app;
mod home;
mod setting_tab;
mod settings;

use gpui::*;
use gpui_component::Root;
use db::GlobalDbState;
use gpui_component_assets::Assets;
use one_core::gpui_tokio::Tokio;
use crate::onehup_app::OneHupApp;

fn main() {
    let app = Application::new().with_assets(Assets);



    app.run(move |cx| {
        onehup_app::init(cx);
        // Initialize global database state
        let db_state = GlobalDbState::new();
        // Start cleanup task
        db_state.start_cleanup_task(cx);
        cx.set_global(db_state);
        let mut window_size = size(px(1600.0), px(1200.0));
        if let Some(display) = cx.primary_display() {
            let display_size = display.bounds().size;
            window_size.width = window_size.width.min(display_size.width * 0.85);
            window_size.height = window_size.height.min(display_size.height * 0.85);
        }

        let window_bounds = Bounds::centered(None, window_size, cx);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(window_bounds)),
            #[cfg(not(target_os = "linux"))]
            titlebar: Some(gpui_component::TitleBar::title_bar_options()),
            window_min_size: Some(Size {
                width: px(640.),
                height: px(480.),
            }),
            #[cfg(target_os = "linux")]
            window_background: gpui::WindowBackgroundAppearance::Transparent,
            #[cfg(target_os = "linux")]
            window_decorations: Some(gpui::WindowDecorations::Client),
            kind: WindowKind::Normal,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            cx.open_window(options, |window, cx| {
                let view = cx.new(|cx| {
                    OneHupApp::new(window, cx)
                });
                cx.new(|cx| Root::new(view, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    });
}
