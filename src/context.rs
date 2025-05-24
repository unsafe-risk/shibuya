//! A Rust implementation of Golang's context package.
//!
//! This module provides functionality similar to Go's context package,
//! which carries deadlines, cancellation signals, and request-scoped values
//! across API boundaries and between processes.

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use std::error::Error;
use tokio::sync::Notify;
use tokio::time;
use std::collections::HashSet;

/// Error returned when a Context is canceled.
#[derive(Debug, Clone)]
pub struct CanceledError;

impl fmt::Display for CanceledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "context canceled")
    }
}

impl Error for CanceledError {}

/// Error returned when a Context's deadline passes.
#[derive(Debug, Clone)]
pub struct DeadlineExceededError;

impl fmt::Display for DeadlineExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "context deadline exceeded")
    }
}

impl Error for DeadlineExceededError {}

/// A Context carries a deadline, cancellation signal, and request-scoped values
/// across API boundaries. Its methods are safe for simultaneous use by multiple
/// goroutines.
pub trait Context: Send + Sync {
    /// Returns the time when this Context will be canceled, if any.
    fn deadline(&self) -> Option<Instant>;

    /// Returns a Notify that is notified when this Context is canceled.
    /// If the context can never be canceled, done may return None.
    /// This is analogous to Go's Done() channel that's closed when the context is canceled.
    fn done(&self) -> Option<Arc<Notify>>;

    /// If Done is not yet notified, Err returns None.
    /// If Done is notified, Err returns a non-nil error explaining why:
    /// Canceled if the context was canceled
    /// or DeadlineExceeded if the context's deadline passed.
    fn err(&self) -> Option<Box<dyn Error + Send + Sync>>;

    /// Returns the raw value associated with this Context for a key, or None if no
    /// value is associated with key.
    fn value_raw(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>>;
}

/// Extension trait to provide type-safe value retrieval
pub trait ContextExt: Context {
    /// Returns the value associated with this Context for a key, or None if no
    /// value is associated with key. Successive calls to Value with the same key
    /// returns the same result.
    fn value<T: 'static + Send + Sync + Clone>(&self, key: &str) -> Option<Arc<T>> {
        self.value_raw(key).and_then(|v| {
            v.downcast_ref::<T>().map(|val| {
                // Clones the value and wraps it in a new Arc
                Arc::new(val.clone())
            })
        })
    }
}

// Implement ContextExt for any type that implements Context
impl<T: Context> ContextExt for T {}

/// An emptyCtx is never canceled, has no values, and has no deadline.
#[derive(Debug, Clone, Copy)]
struct EmptyCtx;

impl Context for EmptyCtx {
    fn deadline(&self) -> Option<Instant> {
        None
    }

    fn done(&self) -> Option<Arc<Notify>> {
        None
    }

    fn err(&self) -> Option<Box<dyn Error + Send + Sync>> {
        None
    }

    fn value_raw(&self, _key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}

/// Background returns a non-nil, empty Context. It is never canceled, has no
/// values, and has no deadline. It is typically used by the main function,
/// initialization, and tests, and as the top-level Context for incoming
/// requests.
pub fn background() -> impl Context {
    EmptyCtx
}

/// TODO returns a non-nil, empty Context. Code should use context.TODO when
/// it's unclear which Context to use or it is not yet available (because the
/// surrounding function has not yet been extended to accept a Context
/// parameter).
pub fn todo() -> impl Context {
    EmptyCtx
}

/// A ValueContext carries a key-value pair. It implements Value for that key and
/// delegates all other calls to the embedded Context.
pub struct ValueContext<C: Context> {
    parent: C,
    key: String,
    value: Arc<dyn Any + Send + Sync>,
}

impl<C: Context> Context for ValueContext<C> {
    fn deadline(&self) -> Option<Instant> {
        self.parent.deadline()
    }

    fn done(&self) -> Option<Arc<Notify>> {
        self.parent.done()
    }

    fn err(&self) -> Option<Box<dyn Error + Send + Sync>> {
        self.parent.err()
    }

    fn value_raw(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        if key == self.key {
            return Some(self.value.clone());
        }
        self.parent.value_raw(key)
    }
}

/// WithValue returns a copy of parent in which the value associated with key is val.
pub fn with_value<C: Context, T: 'static + Clone + Send + Sync>(
    parent: C,
    key: &str,
    val: T,
) -> impl Context {
    ValueContext {
        parent,
        key: key.to_string(),
        value: Arc::new(val),
    }
}

/// A CancelContext can be canceled. When canceled, it also cancels any children
/// that implement cancellation.
pub struct CancelContext<C: Context> {
    parent: C,
    notify: Arc<Notify>,
    err: Arc<RwLock<Option<Box<dyn Error + Send + Sync>>>>,
    deadline: Option<Instant>,
    // Track children for propagation
    children: Arc<Mutex<HashSet<Weak<Notify>>>>,
    // Task handles for resource cleanup
    timer_task_handle: Option<tokio::task::JoinHandle<()>>,
    parent_watcher_handle: Option<tokio::task::JoinHandle<()>>,
}

// Note: We intentionally don't implement Clone for CancelContext
// to avoid confusion with Go's context model where contexts are not typically cloned.
// Users should use Arc<CancelContext> if shared ownership is needed.

impl<C: Context> Context for CancelContext<C> {
    fn deadline(&self) -> Option<Instant> {
        self.deadline.or_else(|| self.parent.deadline())
    }

    fn done(&self) -> Option<Arc<Notify>> {
        Some(self.notify.clone())
    }

    fn err(&self) -> Option<Box<dyn Error + Send + Sync>> {
        // First check our own error
        if let Ok(err) = self.err.read() {
            if let Some(e) = &*err {
                // Create a new Box with the same error type
                if let Some(_) = e.downcast_ref::<CanceledError>() {
                    return Some(Box::new(CanceledError));
                } else if let Some(_) = e.downcast_ref::<DeadlineExceededError>() {
                    return Some(Box::new(DeadlineExceededError));
                }
                // Add more error types as needed
            }
        }
        // Then check parent's error
        self.parent.err()
    }

    fn value_raw(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        self.parent.value_raw(key)
    }
}

impl<C: Context> Drop for CancelContext<C> {
    fn drop(&mut self) {
        // Abort any spawned tasks when the context is dropped
        if let Some(handle) = self.timer_task_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.parent_watcher_handle.take() {
            handle.abort();
        }
    }
}

/// A CancelFunc tells an operation to abandon its work.
/// A CancelFunc does not wait for the work to stop.
/// A CancelFunc may be called by multiple goroutines simultaneously.
/// After the first call, subsequent calls to a CancelFunc do nothing.
pub type CancelFunc = Box<dyn Fn() + Send + Sync>;

// Helper function to propagate cancellation to children
fn propagate_cancel(children: &Arc<Mutex<HashSet<Weak<Notify>>>>) {
    let mut children_lock = children.lock().unwrap();
    // Notify all children and remove any that have been dropped
    children_lock.retain(|child| {
        if let Some(notify) = child.upgrade() {
            notify.notify_waiters();
            true
        } else {
            false
        }
    });
}

// Helper function to check if parent is already canceled
fn check_parent_canceled<C: Context>(parent: &C) -> bool {
    // Check if parent is already canceled by polling its error
    parent.err().is_some()
}

/// WithCancel returns a copy of parent with a new Done channel. The returned
/// context's Done channel is closed when the returned cancel function is called
/// or when the parent context's Done channel is closed, whichever happens first.
pub fn with_cancel<C: Context + 'static>(parent: C) -> (CancelContext<C>, CancelFunc) {
    let notify = Arc::new(Notify::new());
    let err = Arc::new(RwLock::new(None));
    let children = Arc::new(Mutex::new(HashSet::new()));

    let mut ctx = CancelContext {
        parent,
        notify: notify.clone(),
        err: err.clone(),
        deadline: None,
        children,
        timer_task_handle: None,
        parent_watcher_handle: None,
    };

    // Check if parent is already canceled
    let parent_already_canceled = check_parent_canceled(&ctx.parent);
    if parent_already_canceled {
        // If parent is already canceled, cancel this context immediately
        if let Ok(mut err_write) = err.write() {
            *err_write = Some(Box::new(CanceledError));
        }
        notify.notify_waiters();
    } else if let Some(parent_done) = ctx.parent.done() {
        // Set up a watcher for parent cancellation
        let notify_clone = notify.clone();
        let err_clone = err.clone();

        // Spawn a task to watch for parent cancellation
        let handle = tokio::spawn(async move {
            parent_done.notified().await;

            // Parent was canceled, propagate to this context
            if let Ok(mut err_write) = err_clone.write() {
                if err_write.is_none() {
                    // Per Go spec, child context is Canceled if parent is Canceled
                    // unless child has an earlier deadline that fired.
                    *err_write = Some(Box::new(CanceledError));
                }
            }

            // Notify all waiters
            notify_clone.notify_waiters();
        });

        // Store the handle for cleanup
        ctx.parent_watcher_handle = Some(handle);
    }

    let notify_clone = notify.clone();
    let err_clone = err.clone();
    let children_clone = ctx.children.clone();

    let cancel = Box::new(move || {
        // Set the error
        let mut should_notify = false;
        if let Ok(mut err_write) = err_clone.write() {
            if err_write.is_none() {
                *err_write = Some(Box::new(CanceledError));
                should_notify = true;
            }
        }

        if should_notify {
            // Notify all waiters
            notify_clone.notify_waiters();

            // Propagate cancellation to children
            propagate_cancel(&children_clone);
        }
    });

    (ctx, cancel)
}

/// WithDeadline returns a copy of the parent context with the deadline adjusted
/// to be no later than d. If the parent's deadline is already earlier than d,
/// WithDeadline(parent, d) is semantically equivalent to parent. The returned
/// context's Done channel is closed when the deadline expires, when the returned
/// cancel function is called, or when the parent context's Done channel is
/// closed, whichever happens first.
pub fn with_deadline<C: Context + 'static>(
    parent: C,
    deadline: Instant,
) -> (CancelContext<C>, CancelFunc) {
    // Check if deadline is already expired
    if Instant::now() >= deadline {
        let (ctx, cancel) = with_cancel(parent);
        // Cancel immediately with DeadlineExceededError
        if let Ok(mut err_write) = ctx.err.write() {
            *err_write = Some(Box::new(DeadlineExceededError));
        }
        ctx.notify.notify_waiters();
        return (ctx, cancel);
    }

    // If parent's deadline is earlier, use that
    if let Some(parent_deadline) = parent.deadline() {
        if parent_deadline <= deadline {
            return with_cancel(parent);
        }
    }

    let (ctx, cancel) = with_cancel(parent);

    // Set the deadline
    let mut ctx = ctx;
    ctx.deadline = Some(deadline);

    // Set up a timer to cancel when the deadline is reached
    let notify_clone = ctx.notify.clone();
    let err_clone = ctx.err.clone();
    let children_clone = ctx.children.clone();

    // Spawn a task to handle the deadline timer
    let handle = tokio::spawn(async move {
        // Sleep until the deadline
        time::sleep_until(deadline.into()).await;

        // Check if we need to cancel
        let mut should_notify = false;
        if let Ok(mut err_write) = err_clone.write() {
            if err_write.is_none() {
                *err_write = Some(Box::new(DeadlineExceededError));
                should_notify = true;
            }
        }

        if should_notify {
            // Notify all waiters
            notify_clone.notify_waiters();

            // Propagate cancellation to children
            propagate_cancel(&children_clone);
        }
    });

    // Store the handle for cleanup
    ctx.timer_task_handle = Some(handle);

    (ctx, cancel)
}

/// WithTimeout returns WithDeadline(parent, Instant::now() + timeout).
pub fn with_timeout<C: Context + 'static>(
    parent: C,
    timeout: Duration,
) -> (CancelContext<C>, CancelFunc) {
    with_deadline(parent, Instant::now() + timeout)
}

/// AfterFunc returns a copy of the parent context that will be canceled when the
/// provided function completes. The function is executed in a new tokio task.
/// The returned context's Done channel is closed when the function returns,
/// when the returned cancel function is called, or when the parent context's Done
/// channel is closed, whichever happens first.
pub fn after_func<C: Context + 'static, F, Fut>(
    parent: C,
    f: F,
) -> (impl Context, CancelFunc)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (ctx, cancel) = with_cancel(parent);

    // Clone necessary values for the task
    let notify_clone = ctx.notify.clone();
    let err_clone = ctx.err.clone();
    let children_clone = ctx.children.clone();

    // Spawn a task to execute the function
    let handle = tokio::spawn(async move {
        // Execute the function and await its completion
        f().await;

        // After function completes, cancel the context
        let mut should_notify = false;
        if let Ok(mut err_write) = err_clone.write() {
            if err_write.is_none() {
                *err_write = Some(Box::new(CanceledError));
                should_notify = true;
            }
        }

        if should_notify {
            // Notify all waiters
            notify_clone.notify_waiters();

            // Propagate cancellation to children
            propagate_cancel(&children_clone);
        }
    });

    // Store the handle for cleanup
    let mut ctx = ctx;
    ctx.timer_task_handle = Some(handle);

    (ctx, cancel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_background() {
        let ctx = background();
        assert!(ctx.deadline().is_none());
        assert!(ctx.done().is_none());
        assert!(ctx.err().is_none());
        assert!(ctx.value::<String>("key").is_none());
    }

    #[test]
    fn test_with_value() {
        let ctx = background();
        let ctx = with_value(ctx, "key", "value".to_string());

        // Value should be retrievable
        let value = ctx.value::<String>("key");
        assert!(value.is_some());

        // Value should match what we put in
        if let Some(v) = value {
            assert_eq!(*v, "value".to_string());
        }

        // Non-existent key should return None
        assert!(ctx.value::<String>("nonexistent").is_none());
    }

    #[test]
    fn test_with_cancel() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = background();
            let (ctx, cancel) = with_cancel(ctx);

            // Before cancellation
            assert!(ctx.deadline().is_none());
            assert!(ctx.done().is_some());
            assert!(ctx.err().is_none());

            // After cancellation
            cancel();
            assert!(ctx.err().is_some());
            if let Some(err) = ctx.err() {
                assert!(err.is::<CanceledError>());
            }
        });
    }

    #[test]
    fn test_parent_cancellation_propagation() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (parent_ctx, parent_cancel) = with_cancel(background());
            let (child_ctx, _) = with_cancel(parent_ctx);

            // Cancel the parent
            parent_cancel();

            // Child should be notified
            if let Some(_done) = child_ctx.done() {
                // In a real async context, we would await this
                // For the test, we'll just check that the error is propagated
                assert!(child_ctx.err().is_some());
                if let Some(err) = child_ctx.err() {
                    assert!(err.is::<CanceledError>());
                }
            } else {
                panic!("Child's done() returned None");
            }
        });
    }

    #[test]
    fn test_with_deadline() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = background();
            let deadline = Instant::now() + Duration::from_millis(50);
            let (ctx, _) = with_deadline(ctx, deadline);

            assert_eq!(ctx.deadline(), Some(deadline));

            // Wait for the deadline to pass
            tokio::time::sleep(Duration::from_millis(60)).await;

            // Context should be canceled with DeadlineExceededError
            assert!(ctx.err().is_some());
            if let Some(err) = ctx.err() {
                assert!(err.is::<DeadlineExceededError>());
            }
        });
    }

    #[test]
    fn test_with_timeout() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = background();
            let timeout = Duration::from_millis(50);
            let (ctx, _) = with_timeout(ctx, timeout);

            // Deadline should be approximately now + timeout
            let now = Instant::now();
            if let Some(deadline) = ctx.deadline() {
                let diff = deadline.duration_since(now);
                assert!(diff <= timeout);
                assert!(diff > Duration::from_millis(0));
            } else {
                panic!("Expected deadline to be set");
            }

            // Wait for the timeout to expire
            tokio::time::sleep(Duration::from_millis(60)).await;

            // Context should be canceled with DeadlineExceededError
            assert!(ctx.err().is_some());
            if let Some(err) = ctx.err() {
                assert!(err.is::<DeadlineExceededError>());
            }
        });
    }

    #[test]
    fn test_expired_deadline() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = background();
            // Create a deadline that's already passed
            let deadline = Instant::now() - Duration::from_millis(10);
            let (ctx, _) = with_deadline(ctx, deadline);

            // Context should be immediately canceled with DeadlineExceededError
            assert!(ctx.err().is_some());
            if let Some(err) = ctx.err() {
                assert!(err.is::<DeadlineExceededError>());
            }
        });
    }

    #[test]
    fn test_already_canceled_parent() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (parent_ctx, parent_cancel) = with_cancel(background());

            // Cancel the parent
            parent_cancel();

            // Create a child of the already-canceled parent
            let (child_ctx, _) = with_cancel(parent_ctx);

            // Child should be immediately canceled
            assert!(child_ctx.err().is_some());
            if let Some(err) = child_ctx.err() {
                assert!(err.is::<CanceledError>());
            }
        });
    }

    #[test]
    fn test_after_func() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = background();

            // Use a flag to track if the function was called
            let flag = Arc::new(Mutex::new(false));
            let flag_clone = flag.clone();

            // Create a context that will be canceled after the function completes
            let (ctx, _) = after_func(ctx, move || async move {
                // Set the flag to true to indicate the function was called
                let mut flag = flag_clone.lock().unwrap();
                *flag = true;
            });

            // Initially, the context should not be canceled
            assert!(ctx.err().is_none());

            // Wait a bit for the function to execute
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Verify the function was called
            let flag_value = *flag.lock().unwrap();
            assert!(flag_value, "Function should have been called");

            // Context should be canceled with CanceledError
            assert!(ctx.err().is_some());
            if let Some(err) = ctx.err() {
                assert!(err.is::<CanceledError>());
            }
        });
    }
}
