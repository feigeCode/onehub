pub mod types;
pub mod provider;
pub mod manager;
pub mod storage;
pub mod openai_compat;
pub mod openai_client;
pub mod claude_client;
pub mod chat_history;

pub use manager::register_provider;

use std::sync::Arc;

use gpui::{App, Global};

use self::manager::GlobalProviderState;
use self::storage::ProviderRepository;


/// Initialize global LLM state (call this from one_core::init)
pub fn init(cx: &mut App) {
    storage::init(cx);
    let client = cx.http_client();
    let state = GlobalProviderState::new(client);
    cx.set_global(state);

}


