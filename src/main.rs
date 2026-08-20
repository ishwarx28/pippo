// Owns desktop bootstrap and the single Tauri window.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod cfg;
mod store;

fn main() {
    run().expect("failed to run pippo");
}

fn run() -> anyhow::Result<()> {
    let root = cfg::root()?;
    let cfg = cfg::load_at(root.clone())?;
    let store = store::Store::open(root)?;
    tauri::Builder::default()
        .manage(cfg)
        .manage(store)
        .run(tauri::generate_context!())
        .map_err(Into::into)
}
