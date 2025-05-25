//! A Rust implementation of Golang's singleflight package.
//!
//! This module provides functionality similar to Go's singleflight package,
//! which provides a duplicate function call suppression mechanism.
//! It is useful in scenarios where you want to ensure that only one execution
//! of a function is in-flight for a given key at a time.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Error returned when a call was suppressed because another call with the same key is already in flight.
#[derive(Debug, Clone)]
pub struct DuplicateCallError;

impl fmt::Display for DuplicateCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "duplicate call suppressed")
    }
}

impl std::error::Error for DuplicateCallError {}

/// Error returned when a singleflight operation fails.
#[derive(Debug)]
pub enum SingleFlightError<E> {
    /// Error from the user-provided function.
    FunctionError(E),
    /// Error when the leader task failed and closed the channel unexpectedly.
    LeaderTaskFailed,
}

impl<E: fmt::Debug> fmt::Display for SingleFlightError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SingleFlightError::FunctionError(e) => write!(f, "function error: {:?}", e),
            SingleFlightError::LeaderTaskFailed => write!(f, "leader task failed unexpectedly"),
        }
    }
}

impl<E: fmt::Debug> std::error::Error for SingleFlightError<E> {}

/// Result of a singleflight call.
#[derive(Debug, Clone)]
pub struct Result<T> {
    /// The value returned by the function call.
    pub val: T,
    /// Whether the function call was actually executed or if the result was shared.
    pub shared: bool,
}

/// A call record for tracking in-flight function calls.
struct Call<T> {
    /// Channel for sending the result to waiters.
    wg: Vec<oneshot::Sender<Arc<T>>>,
    /// The result of the function call, if available.
    val: Option<Arc<T>>,
}

/// Group represents a class of work and creates a space in which units of work
/// can be merged for deduplication purposes.
pub struct Group<K, T> {
    /// Map of in-flight function calls, shared with Arc.
    m: Arc<Mutex<HashMap<K, Call<T>>>>,
}

impl<K, T> Default for Group<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T> Group<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone,
{
    /// Creates a new Group.
    pub fn new() -> Self {
        Group {
            m: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Executes and returns the results of the given function, making sure that
    /// only one execution is in-flight for a given key at a time.
    ///
    /// If a duplicate call comes in while the first call is still in flight,
    /// the duplicate caller waits for the original call to complete and receives
    /// the same results.
    /// Errors from the function `f` are propagated.
    ///
    /// The function `f` can be asynchronous and will be executed in a separate task.
    pub async fn do_<F, Fut, E>(&self, key: K, f: F) -> std::result::Result<Result<T>, SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + Sync + Clone + 'static,
        E: fmt::Debug + Send + 'static,
    {
        // Fast path: check if a call is already in flight
        let (rx, shared) = {
            let mut m = self.m.lock().unwrap();

            if let Some(call) = m.get_mut(&key) {
                // A call is already in flight, create a channel to receive the result
                let (tx, rx) = oneshot::channel();
                call.wg.push(tx);
                (Some(rx), true)
            } else {
                // First call, create a new call record
                m.insert(key.clone(), Call {
                    wg: Vec::new(),
                    val: None,
                });
                (None, false)
            }
        };

        // If we're a duplicate call, wait for the result
        if let Some(rx) = rx {
            return match rx.await {
                Ok(val) => {
                    Ok(Result {
                        val: (*val).clone(),
                        shared: true,
                    })
                }
                Err(_) => {
                    // The channel was closed without sending a value
                    // This shouldn't happen in normal operation
                    Err(SingleFlightError::LeaderTaskFailed)
                }
            }
        }

        // We're the first call, spawn a task to execute the function
        let (tx, rx) = oneshot::channel();
        let key_clone = key.clone();
        let group = self.clone();

        tokio::spawn(async move {
            // Execute the function asynchronously
            match f().await {
                Ok(val) => {
                    let val_arc = Arc::new(val);

                    // Get the waiters, update the result, and remove the call from the map in a single lock
                    let waiters = {
                        let mut m = group.m.lock().unwrap();
                        let waiters = if let Some(call) = m.get_mut(&key_clone) {
                            call.val = Some(val_arc.clone());
                            std::mem::take(&mut call.wg)
                        } else {
                            Vec::new() // This shouldn't happen
                        };

                        // Remove the call from the map while we have the lock
                        m.remove(&key_clone);

                        waiters
                    };

                    // Notify all waiters after releasing the lock
                    for waiter_tx in waiters {
                        let _ = waiter_tx.send(val_arc.clone());
                    };

                    // Send the result to the original caller
                    let _ = tx.send(Ok(val_arc));
                }
                Err(err) => {
                    // Remove the call from the map on error
                    {
                        let mut m = group.m.lock().unwrap();
                        m.remove(&key_clone);
                    }

                    // Send the error to the original caller
                    let _ = tx.send(Err(err));
                }
            }
        });

        // Wait for the result from the spawned task
        match rx.await {
            Ok(result) => {
                match result {
                    Ok(val_arc) => {
                        Ok(Result {
                            val: (*val_arc).clone(),
                            shared,
                        })
                    }
                    Err(err) => {
                        Err(SingleFlightError::FunctionError(err))
                    }
                }
            }
            Err(_) => {
                // The spawned task panicked or was cancelled
                // Remove the call from the map
                {
                    let mut m = self.m.lock().unwrap();
                    m.remove(&key);
                }
                Err(SingleFlightError::LeaderTaskFailed)
            }
        }
    }

    /// Similar to do_ but returns a channel that receives the results when they are ready.
    ///
    /// The function `f` can be asynchronous and will be executed in a separate task.
    pub fn do_chan<F, Fut, E>(&self, key: K, f: F) -> (oneshot::Receiver<Arc<T>>, bool)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + Sync + 'static,
        E: fmt::Debug + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        // Check if a call is already in flight
        let shared = {
            let mut m = self.m.lock().unwrap();

            if let Some(call) = m.get_mut(&key) {
                // A call is already in flight
                if let Some(val) = &call.val {
                    // Result is already available, send it immediately
                    let _ = tx.send(val.clone());
                } else {
                    // Wait for the result
                    call.wg.push(tx);
                }
                true
            } else {
                // First call, create a new call record
                m.insert(key.clone(), Call {
                    wg: vec![tx],
                    val: None,
                });

                // Spawn a task to execute the function
                let key_clone = key.clone();
                let group = self.clone();
                tokio::spawn(async move {
                    // Execute the function asynchronously
                    match f().await {
                        Ok(val) => {
                            let val_arc = Arc::new(val);

                            // Get the waiters, update the result, and remove the call from the map in a single lock
                            let waiters = {
                                let mut m = group.m.lock().unwrap();
                                let waiters = if let Some(call) = m.get_mut(&key_clone) {
                                    call.val = Some(val_arc.clone());
                                    std::mem::take(&mut call.wg)
                                } else {
                                    Vec::new() // This shouldn't happen
                                };

                                // Remove the call from the map while we have the lock
                                m.remove(&key_clone);

                                waiters
                            };

                            // Notify all waiters after releasing the lock
                            for tx in waiters {
                                let _ = tx.send(val_arc.clone());
                            };
                        }
                        Err(err) => {
                            // Remove the call from the map on error
                            {
                                let mut m = group.m.lock().unwrap();
                                m.remove(&key_clone);
                            }

                            // Log the error
                            eprintln!("singleflight: function error: {:?}", err);
                        }
                    }
                });

                false
            }
        };

        (rx, shared)
    }

    /// Forgets a key, allowing subsequent calls to do_ for that key to execute new functions.
    /// This is useful when you want to invalidate cached results.
    pub fn forget(&self, key: K) {
        let mut m = self.m.lock().unwrap();
        m.remove(&key);
    }
}

impl<K, T> Clone for Group<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone,
{
    fn clone(&self) -> Self {
        Group {
            m: Arc::clone(&self.m),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::runtime::Runtime;

    #[test]
    fn test_do_basic() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Group::<&str, usize>::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // First call should execute the function
            let counter_clone = counter.clone();
            let result = g.do_("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(42)
            }).await;

            let result_val = result.as_ref().unwrap();
            assert_eq!(result_val.val, 42);
            assert_eq!(result_val.shared, false);
            assert_eq!(counter.load(Ordering::SeqCst), 1);

            // Second call with the same key will execute the function again
            // because the first call completed and was removed from the map
            let counter_clone = counter.clone();
            let result = g.do_("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(43) // Different value for the second execution
            }).await;

            let result_val = result.as_ref().unwrap();
            assert_eq!(result_val.val, 43); // Should get the new value (43)
            assert_eq!(result_val.shared, false); // Not shared, it's a new execution
            assert_eq!(counter.load(Ordering::SeqCst), 2); // Counter should increment
        });
    }

    #[test]
    fn test_do_concurrent() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Arc::new(Group::<&str, usize>::new());
            let counter = Arc::new(AtomicUsize::new(0));

            // Spawn multiple tasks that call do_ with the same key
            let mut handles = Vec::new();
            for _ in 0..10 {
                let g = g.clone();
                let counter = counter.clone();
                let handle = tokio::spawn(async move {
                    // Simulate work with a synchronous function
                    let result = g.do_("key", move || async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        // Simulate work with a synchronous operation
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, &str>(42)
                    }).await;
                    result
                });
                handles.push(handle);
            }

            // Wait for all tasks to complete
            let mut results = Vec::new();
            for handle in handles {
                results.push(handle.await.unwrap());
            }

            // Verify that the function was only executed once
            assert_eq!(counter.load(Ordering::SeqCst), 1);

            // Verify that all tasks got the same result
            for result in results {
                let result_val = result.as_ref().unwrap();
                assert_eq!(result_val.val, 42);
                // Some will have shared=true, but we can't guarantee which ones
                // due to the race condition between the first call completing
                // and subsequent calls checking the map
            }
        });
    }

    #[test]
    fn test_forget() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Group::<&str, usize>::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // First call
            let counter_clone = counter.clone();
            let result = g.do_("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(42)
            }).await;

            let result_val = result.as_ref().unwrap();
            assert_eq!(result_val.val, 42);
            assert_eq!(counter.load(Ordering::SeqCst), 1);

            // Forget the key
            g.forget("key");

            // Second call should execute the function again
            let counter_clone = counter.clone();
            let result = g.do_("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(43)
            }).await;

            let result_val = result.as_ref().unwrap();
            assert_eq!(result_val.val, 43);
            assert_eq!(counter.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn test_do_chan() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Group::<&str, usize>::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // First call
            let counter_clone = counter.clone();
            let (rx, shared) = g.do_chan("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                // Use synchronous sleep instead of async
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<_, &str>(42)
            });

            assert_eq!(shared, false);

            // Second call should get the same result
            let counter_clone = counter.clone();
            let (rx2, shared2) = g.do_chan("key", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(43)
            });

            assert_eq!(shared2, true);

            // Wait for results
            let result = rx.await.unwrap();
            let result2 = rx2.await.unwrap();

            assert_eq!(*result, 42);
            assert_eq!(*result2, 42);
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn test_different_keys() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Group::<&str, usize>::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // Call with key1
            let counter_clone = counter.clone();
            let result1 = g.do_("key1", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(42)
            }).await;

            // Call with key2
            let counter_clone = counter.clone();
            let result2 = g.do_("key2", move || async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(43)
            }).await;

            let result1_val = result1.as_ref().unwrap();
            let result2_val = result2.as_ref().unwrap();
            assert_eq!(result1_val.val, 42);
            assert_eq!(result2_val.val, 43);
            assert_eq!(counter.load(Ordering::SeqCst), 2); // Both functions should execute
        });
    }
}