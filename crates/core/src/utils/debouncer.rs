use tokio::time::{sleep, Duration};
use tokio::sync::Mutex;
use std::sync::Arc;

pub struct Debouncer {
    delay: Duration,
    task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl Debouncer {
    /// Create a new debouncer with a delay.
    pub fn new(delay: Duration) -> Self {
        Debouncer {
            delay,
            task: Arc::new(Mutex::new(None)),
        }
    }

    /// Call the function after the delay.
    pub async fn call<F, Fut, R>(&self, f: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = R> + Send + 'static,
    {
        let mut task = self.task.lock().await;

        // Cancel existing timer if it exists
        if let Some(handle) = task.take() {
            handle.abort();
        }

        // Set a new timer
        let delay = self.delay;
        *task = Some(tokio::spawn(async move {
            sleep(delay).await;
            f().await;
        }));
    }
}