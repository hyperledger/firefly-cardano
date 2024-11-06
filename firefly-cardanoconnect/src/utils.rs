use std::{fmt::Debug, future::Future};

use futures::future::{BoxFuture, FutureExt as _};
use tokio::sync::Mutex;

#[macro_export]
macro_rules! strong_id {
    ($Outer:ident, $Inner:ty) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub struct $Outer($Inner);
        impl std::fmt::Display for $Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
        impl From<$Inner> for $Outer {
            fn from(value: $Inner) -> Self {
                Self(value)
            }
        }
        impl From<$Outer> for $Inner {
            fn from(value: $Outer) -> Self {
                value.0
            }
        }
    };
}

pub struct LazyInit<T> {
    factory: Mutex<Option<BoxFuture<'static, T>>>,
    value: Option<T>,
}

impl<T: Debug> Debug for LazyInit<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyInit")
            .field("value", &self.value)
            .finish()
    }
}

impl<T> LazyInit<T> {
    pub fn new<F: Future<Output = T> + Send + 'static>(fut: F) -> Self {
        Self {
            factory: Mutex::new(Some(fut.boxed())),
            value: None,
        }
    }
    pub async fn get(&mut self) -> &mut T {
        if self.value.is_none() {
            let mut lock = self.factory.lock().await;
            let Some(fut) = lock.as_mut() else {
                panic!("already resolved");
            };
            let value = fut.await;
            self.value = Some(value);
            lock.take();
        }
        self.value.as_mut().unwrap()
    }
}
