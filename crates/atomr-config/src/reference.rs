//! Built-in reference configuration — defaults for the `atomr.*` keys.

pub fn reference_config() -> &'static str {
    include_str!("reference.conf.toml")
}
