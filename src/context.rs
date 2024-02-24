
use std::{collections::HashMap, sync::{Arc, Mutex}};
use std::any::Any;

use tokio::sync::broadcast;

pub enum Context<'a> {
    Base,
    WithData(Arc<Mutex<Context<'a>>>, HashMap<&'a str, Box<dyn Any>>),
    WithCancel(Arc<Mutex<Context<'a>>>, broadcast::Sender<()>, bool),
}

impl Context<'static> {
    pub fn with_value<'x>(self, value: Vec<(&'static str, Box<dyn Any>)>) -> Arc<Mutex<Context<'static>>> {
        let mut data = HashMap::new();
        for (k, v) in value {
            data.insert(k, v);
        }
        Arc::new(Mutex::new(Context::WithData(Arc::new(Mutex::new(self)), data)))
    }

    pub fn value<V: Any + Copy>(&'_ self, key: &'static str) -> Option<V> {
        match self {
            Context::Base => None,
            Context::WithData(ctx, data) => {
                match data.get(key) {
                    None => {
                        let ctx = ctx.lock().unwrap();
                        ctx.value::<V>(key)
                    },
                    Some(x) => {
                        let x = x.downcast_ref::<V>();
                        match x {
                            None => None,
                            Some(x) => Some(*x),
                        }
                    },
                }
            },
            Context::WithCancel(ctx, _, _) => {
                match ctx.lock() {
                    Ok(ctx) => ctx.value::<V>(key),
                    Err(_) => None,
                }
            },
        }
    }

    pub fn with_cancel(self) -> (Arc<Mutex<Context<'static>>>, broadcast::Receiver<()>) {
        let (tx, rx) = broadcast::channel(1);
        (Arc::new(Mutex::new(Context::WithCancel(Arc::new(Mutex::new(self)), tx, false))), rx)
    }

    pub fn cancel(&self) {
        match self {
            Context::Base => {},
            Context::WithData(ctx, _) => {
                ctx.lock().unwrap().cancel();
            },
            Context::WithCancel(_, tx, _) => {
                tx.send(()).unwrap();
            },
        }
    }

    pub fn done(&self) -> Option<broadcast::Receiver<()>> {
        match self {
            Context::Base => {
                None
            },
            Context::WithData(ctx, _) => {
                ctx.lock().unwrap().done()
            },
            Context::WithCancel(_, tx, _) => {
                Some(tx.subscribe())
            },
        }
    }
}
