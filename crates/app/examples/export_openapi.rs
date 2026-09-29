fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os().nth(1).ok_or("missing output path")?;
    let bytes = serde_json::to_vec_pretty(&kdown_app::api::openapi::document())?;
    std::fs::write(path, bytes)?;
    Ok(())
}
