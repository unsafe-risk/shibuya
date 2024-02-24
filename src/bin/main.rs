use shibuya::context::Context;


#[tokio::main]
async fn main() {
    let ctx = Context::Base;
    let ctx = ctx.with_value(vec![("aaaa", Box::new(123)), ("bbbb", Box::new("hello")), ("cccc", Box::new(3.14))]);
    println!("{:?}", ctx.lock().unwrap().value::<i32>("aaaa"));
    println!("{:?}", ctx.lock().unwrap().value::<&str>("bbbb"));
    println!("{:?}", ctx.lock().unwrap().value::<f64>("cccc"));
}