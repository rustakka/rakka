//! Built-in reference configuration — mirrors `akka.net/src/core/Akka/Configuration/Pigeon.conf`
//! defaults for the keys rakka currently supports.

pub fn reference_config() -> &'static str {
    include_str!("reference.conf.toml")
}
