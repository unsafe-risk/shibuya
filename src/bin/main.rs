use std::vec;

use shibuya::context::{after_ctx, background, cancel_ctx, with_cancel};

#[tokio::main]
async fn main() {
    let ctx = background();
    let ctx = with_cancel(ctx);
    match after_ctx(ctx.clone(), vec![Box::new(|| {
        println!("Hello, world!");
    })]) {
        Ok(_) => {},
        Err(e) => {
            println!("{}", e);
            return;
        },
    }

    cancel_ctx(ctx.clone());

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}