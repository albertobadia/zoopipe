use std::sync::OnceLock;

pub fn init_drivers() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        sqlx::any::install_default_drivers();
    });
}

pub fn ensure_parent_dir(uri: &str) {
    let path_part = if let Some(path) = uri.strip_prefix("sqlite:///") {
        path
    } else if let Some(path) = uri.strip_prefix("sqlite://") {
        path
    } else if let Some(path) = uri.strip_prefix("sqlite:") {
        path
    } else {
        return;
    };

    let path_only = path_part.split('?').next().unwrap_or(path_part);
    if !path_only.is_empty()
        && path_only != ":memory:"
        && let Some(parent) = std::path::Path::new(path_only).parent()
        && !parent.as_os_str().is_empty()
    {
        let _ = std::fs::create_dir_all(parent);
    }
}

pub fn is_postgres(uri: &str) -> bool {
    uri.starts_with("postgres://") || uri.starts_with("postgresql://")
}
