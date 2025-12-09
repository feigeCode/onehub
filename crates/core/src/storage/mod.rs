pub mod manager;
pub mod models;
pub mod repository;
pub mod traits;
pub mod query_model;
pub mod query_repository;

use gpui::App;
pub use manager::*;
pub use models::*;
pub use repository::*;


pub fn init(cx: &mut App){
    manager::init(cx);
    repository::init(cx);
}