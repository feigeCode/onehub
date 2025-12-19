pub mod types;
pub mod provider;
pub mod manager;
pub mod storage;
pub mod openai_compat;
pub mod openai_client;
pub mod claude_client;
pub mod chat_history;

pub use manager::register_provider;


use gpui::App;

use self::manager::GlobalProviderState;


/// Initialize global LLM state (call this from one_core::init)
pub fn init(cx: &mut App) {
    storage::init(cx);
    let client = cx.http_client();
    let state = GlobalProviderState::new(client);
    cx.set_global(state);

}


