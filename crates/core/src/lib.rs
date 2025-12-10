use gpui::App;

pub mod tab_container;
pub mod themes;
pub mod storage;
pub mod gpui_tokio;
pub mod llm;


pub fn init(cx: &mut App){
    gpui_tokio::init(cx);
    themes::init(cx);
    storage::init(cx);
    llm::init(cx);
}