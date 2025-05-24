use shibuya::context::{after_func, background, with_cancel, with_value, ContextExt};

#[tokio::main]
async fn main() {
    let ctx = background();
    let ctx = with_value(ctx, "id", 123i32);
    let ctx = with_value(ctx, "name", "Alice".to_string());
    let (ctx, cancel) = with_cancel(ctx);
    let (ctx, _) = after_func(ctx, || {
        println!("This function will be executed after a delay.");
    });

    // Cancel the context
    cancel();

    let id = match ctx.value::<i32>("id") {
        None => 0,
        Some(a) => *a
    };
    let name = match ctx.value::<String>("name") {
        None => "".to_string(),
        Some(a) => a.to_string()
    };

    println!("id: {} name: {}", id, name)

    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
