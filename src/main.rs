// Owns desktop bootstrap and the single Tauri window.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod cfg;

fn main() {
    run().expect("failed to run pippo");
}

fn run() -> anyhow::Result<()> {
    let cfg = cfg::load()?;
    tauri::Builder::default()
        .manage(cfg)
        .run(tauri::generate_context!())
        .map_err(Into::into)
}
