
use std::{collections::HashMap, sync::{Arc, Mutex}};
use std::any::Any;

use tokio::sync::broadcast;

pub enum Context {
    Base,
    ValueContext(Arc<Mutex<Context>>, HashMap<&'static str, &'static Box<dyn Any + 'static>>),
    CancelContext(Arc<Mutex<Context>>, broadcast::Sender<()>, bool),
}

pub fn background() -> Arc<Mutex<Context>> {
    Arc::new(Mutex::new(Context::Base))
}

pub fn with_value(ctx: Arc<Mutex<Context>>, values: &'static [(&str, Box<dyn Any>)]) -> Arc<Mutex<Context>> {
    let mut map = HashMap::new();
    for (k, v) in values {
        map.insert(*k, v);
    }

    Arc::new(Mutex::new(Context::ValueContext(ctx, map)))
}

pub fn with_cancel(ctx: Arc<Mutex<Context>>) -> Arc<Mutex<Context>> {
    let (tx, _) = broadcast::channel(1);
    Arc::new(Mutex::new(Context::CancelContext(ctx, tx, false)))
}

pub fn value(ctx: Arc<Mutex<Context>>, key: &'static str) -> Option<&'static Box<dyn Any + 'static>> {
    match &*ctx.lock().unwrap() {
        Context::Base => None,
        Context::ValueContext(ctx, map) => {
            match map.get(key) {
                Some(v) => Some(v),
                None => value(ctx.clone(), key),
            }
        },
        Context::CancelContext(ctx, _, _) => {
            value(ctx.clone(), key)
        },
    }
}

pub fn cancel_ctx(ctx: Arc<Mutex<Context>>) {
    match &*ctx.lock().unwrap() {
        Context::Base => {},
        Context::ValueContext(ctx, _) => {
            cancel_ctx(ctx.clone());
        },
        Context::CancelContext(_, tx, _) => {
            tx.send(()).unwrap();
        },
    }
}

pub fn done_of(ctx: Arc<Mutex<Context>>) -> Option<broadcast::Receiver<()>> {
    match &*ctx.lock().unwrap() {
        Context::Base => None,
        Context::ValueContext(ctx, _) => {
            done_of(ctx.clone())
        },
        Context::CancelContext(_, tx, _) => {
            Some(tx.subscribe())
        },
    }
}

pub fn is_cancelled(ctx: Arc<Mutex<Context>>) -> bool {
    match &*ctx.lock().unwrap() {
        Context::Base => false,
        Context::ValueContext(ctx, _) => {
            is_cancelled(ctx.clone())
        },
        Context::CancelContext(_, _, done) => {
            *done
        },
    }
}

pub fn after(ctx: Arc<Mutex<Context>>, fun: Arc<Mutex<dyn Fn() + Send + Sync>>) -> Result<(), &'static str> {
    let rx = done_of(ctx.clone());

    match rx {
        Some(mut rx) => {
            tokio::spawn(async move {
                rx.recv().await.unwrap();
                fun.as_ref().lock().unwrap()();
            });
            Ok(())
        },
        None => Err("Context has no done channel"),
    }
}