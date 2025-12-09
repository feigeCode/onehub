use once_cell::sync::Lazy;
use tokio::runtime::{Runtime, Handle};

pub static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("one-hub-tokio")
        .build()
        .unwrap()
});
pub static TOKIO_HANDLE: Lazy<Handle> = Lazy::new(|| TOKIO_RUNTIME.handle().clone());

pub async fn spawn_result<F, T>(f: F) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match TOKIO_HANDLE.spawn(f).await {
        Ok(res) => res,
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}
