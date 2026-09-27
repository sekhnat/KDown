// The scripted HTTP adapter is crate-internal: importing it from a
// downstream crate must fail (consumer-api spec, task 2.3).
use kdown_engine::http::scripted::ScriptedHttp;

fn main() {
    let _ = ScriptedHttp::new();
}
