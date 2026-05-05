//! `Config` and `ConfigValue` — the HOCON-equivalent value tree.
//! akka.net: `Configuration/Config.cs` + `ConfigValue`.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::error::ConfigError;
use crate::path::ConfigPath;
use crate::reference::reference_config;

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<ConfigValue>),
    Object(BTreeMap<String, ConfigValue>),
}

impl ConfigValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
        }
    }

    fn from_toml(v: toml::Value) -> Self {
        match v {
            toml::Value::String(s) => Self::String(s),
            toml::Value::Integer(i) => Self::Int(i),
            toml::Value::Float(f) => Self::Float(f),
            toml::Value::Boolean(b) => Self::Bool(b),
            toml::Value::Datetime(d) => Self::String(d.to_string()),
            toml::Value::Array(a) => Self::Array(a.into_iter().map(Self::from_toml).collect()),
            toml::Value::Table(t) => {
                Self::Object(t.into_iter().map(|(k, v)| (k, Self::from_toml(v))).collect())
            }
        }
    }
}

/// Akka `Config` root — a merged, layered value tree.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Config {
    root: BTreeMap<String, ConfigValue>,
}

impl Config {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load the atomr reference configuration (akka.net `reference.conf` equivalent).
    pub fn reference() -> Self {
        Self::from_toml_str(reference_config()).expect("built-in reference.conf.toml is valid")
    }

    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let v: toml::Value = toml::from_str(s)?;
        let table = match v {
            toml::Value::Table(t) => t,
            _ => return Err(ConfigError::WrongType { path: "".into(), expected: "object" }),
        };
        Ok(Self { root: table.into_iter().map(|(k, v)| (k, ConfigValue::from_toml(v))).collect() })
    }

    /// Parse a HOCON document (Akka.NET / Pekko `reference.conf`
    /// syntax). See [`crate::hocon`] for the supported subset.
    pub fn from_hocon_str(s: &str) -> Result<Self, ConfigError> {
        let v = crate::hocon::parse(s, std::path::Path::new("."))?;
        match v {
            ConfigValue::Object(o) => Ok(Self { root: o }),
            _ => Err(ConfigError::WrongType { path: "".into(), expected: "object" }),
        }
    }

    /// Parse a HOCON file from disk; `include` directives resolve
    /// relative to the file's parent directory.
    pub fn from_hocon_file(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let v = crate::hocon::parse_file(path.as_ref())?;
        match v {
            ConfigValue::Object(o) => Ok(Self { root: o }),
            _ => Err(ConfigError::WrongType { path: "".into(), expected: "object" }),
        }
    }

    /// Merge `other` on top of `self`; keys from `other` win for scalars,
    /// objects merge recursively — matches HOCON fallback/merge semantics.
    pub fn with_fallback(mut self, fallback: Self) -> Self {
        merge_object(&mut self.root, fallback.root, /*override_rhs=*/ false);
        self
    }

    /// Merge `other` on top of `self`, where `other` wins.
    pub fn merged_with(mut self, other: Self) -> Self {
        merge_object(&mut self.root, other.root, true);
        self
    }

    pub fn get(&self, path: &str) -> Option<&ConfigValue> {
        let p = ConfigPath::parse(path);
        lookup(&self.root, p.segments())
    }

    pub fn get_string(&self, path: &str) -> Result<String, ConfigError> {
        match self.get(path) {
            Some(ConfigValue::String(s)) => Ok(s.clone()),
            Some(v) => Err(ConfigError::WrongType { path: path.into(), expected: v.type_name() }),
            None => Err(ConfigError::NotFound(path.into())),
        }
    }

    pub fn get_int(&self, path: &str) -> Result<i64, ConfigError> {
        match self.get(path) {
            Some(ConfigValue::Int(i)) => Ok(*i),
            Some(ConfigValue::Float(f)) => Ok(*f as i64),
            Some(v) => Err(ConfigError::WrongType { path: path.into(), expected: v.type_name() }),
            None => Err(ConfigError::NotFound(path.into())),
        }
    }

    pub fn get_bool(&self, path: &str) -> Result<bool, ConfigError> {
        match self.get(path) {
            Some(ConfigValue::Bool(b)) => Ok(*b),
            Some(v) => Err(ConfigError::WrongType { path: path.into(), expected: v.type_name() }),
            None => Err(ConfigError::NotFound(path.into())),
        }
    }

    /// Accepts "10ms", "5s", "2m", "1h", or integer milliseconds.
    pub fn get_duration(&self, path: &str) -> Result<Duration, ConfigError> {
        match self.get(path) {
            Some(ConfigValue::String(s)) => parse_duration(s)
                .ok_or_else(|| ConfigError::WrongType { path: path.into(), expected: "duration" }),
            Some(ConfigValue::Int(i)) => Ok(Duration::from_millis(*i as u64)),
            Some(v) => Err(ConfigError::WrongType { path: path.into(), expected: v.type_name() }),
            None => Err(ConfigError::NotFound(path.into())),
        }
    }

    pub fn get_sub(&self, path: &str) -> Option<Config> {
        match self.get(path)? {
            ConfigValue::Object(o) => Some(Self { root: o.clone() }),
            _ => None,
        }
    }

    /// Deserialize a sub-tree at `path` into a strongly-typed value `T`.
    /// Bridge through `serde_json::Value` so any `serde::Deserialize`
    /// type composes. Akka.NET-equivalent of typed `Config.As<T>()`
    /// extension.
    ///
    /// Returns [`ConfigError::NotFound`] if `path` is absent.
    pub fn extract<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ConfigError> {
        let v = self.get(path).ok_or_else(|| ConfigError::NotFound(path.into()))?;
        let json = config_value_to_json(v);
        serde_json::from_value(json)
            .map_err(|e| ConfigError::WrongType { path: path.into(), expected: leak(e.to_string()) })
    }

    /// Deserialize the entire root config into `T`.
    pub fn extract_root<T: serde::de::DeserializeOwned>(&self) -> Result<T, ConfigError> {
        let json =
            config_value_to_json(&ConfigValue::Object(self.root.clone()));
        serde_json::from_value(json)
            .map_err(|e| ConfigError::WrongType { path: "".into(), expected: leak(e.to_string()) })
    }
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn config_value_to_json(v: &ConfigValue) -> serde_json::Value {
    match v {
        ConfigValue::Null => serde_json::Value::Null,
        ConfigValue::Bool(b) => serde_json::Value::Bool(*b),
        ConfigValue::Int(i) => serde_json::Value::Number((*i).into()),
        ConfigValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ConfigValue::String(s) => serde_json::Value::String(s.clone()),
        ConfigValue::Array(items) => {
            serde_json::Value::Array(items.iter().map(config_value_to_json).collect())
        }
        ConfigValue::Object(o) => {
            let map: serde_json::Map<String, serde_json::Value> =
                o.iter().map(|(k, v)| (k.clone(), config_value_to_json(v))).collect();
            serde_json::Value::Object(map)
        }
    }
}

fn lookup<'a>(root: &'a BTreeMap<String, ConfigValue>, segs: &[String]) -> Option<&'a ConfigValue> {
    let (head, tail) = segs.split_first()?;
    let v = root.get(head)?;
    if tail.is_empty() {
        return Some(v);
    }
    match v {
        ConfigValue::Object(o) => lookup(o, tail),
        _ => None,
    }
}

fn merge_object(
    dst: &mut BTreeMap<String, ConfigValue>,
    src: BTreeMap<String, ConfigValue>,
    override_rhs: bool,
) {
    for (k, v) in src {
        match dst.get_mut(&k) {
            Some(ConfigValue::Object(inner)) => {
                if let ConfigValue::Object(src_inner) = v {
                    merge_object(inner, src_inner, override_rhs);
                } else if override_rhs {
                    dst.insert(k, v);
                }
            }
            Some(_) if override_rhs => {
                dst.insert(k, v);
            }
            Some(_) => {} // keep existing
            None => {
                dst.insert(k, v);
            }
        }
    }
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    let (num, unit) = split_number_unit(s)?;
    let n: f64 = num.parse().ok()?;
    let ms = match unit {
        "ms" | "millis" | "milliseconds" => n,
        "s" | "sec" | "seconds" | "" => n * 1000.0,
        "m" | "min" | "minutes" => n * 60_000.0,
        "h" | "hr" | "hours" => n * 3_600_000.0,
        "d" | "days" => n * 86_400_000.0,
        _ => return None,
    };
    Some(Duration::from_micros((ms * 1000.0) as u64))
}

fn split_number_unit(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-')).unwrap_or(s.len());
    let (n, u) = s.split_at(idx);
    Some((n.trim(), u.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_loads() {
        let c = Config::reference();
        assert!(c.get_string("akka.actor.provider").is_ok());
    }

    #[test]
    fn fallback_keeps_existing() {
        let a = Config::from_toml_str("[akka]\nfoo = \"a\"\n").unwrap();
        let b = Config::from_toml_str("[akka]\nfoo = \"b\"\nbar = \"B\"\n").unwrap();
        let c = a.with_fallback(b);
        assert_eq!(c.get_string("akka.foo").unwrap(), "a");
        assert_eq!(c.get_string("akka.bar").unwrap(), "B");
    }

    #[test]
    fn override_merge() {
        let a = Config::from_toml_str("[akka]\nfoo = \"a\"\n").unwrap();
        let b = Config::from_toml_str("[akka]\nfoo = \"b\"\n").unwrap();
        let c = a.merged_with(b);
        assert_eq!(c.get_string("akka.foo").unwrap(), "b");
    }

    #[test]
    fn duration_parses_units() {
        let c = Config::from_toml_str("[x]\nt = \"500ms\"\n").unwrap();
        assert_eq!(c.get_duration("x.t").unwrap(), Duration::from_millis(500));
        let c = Config::from_toml_str("[x]\nt = \"2s\"\n").unwrap();
        assert_eq!(c.get_duration("x.t").unwrap(), Duration::from_secs(2));
    }

    #[test]
    fn get_sub_returns_sub_tree() {
        let c = Config::reference();
        let actor = c.get_sub("akka.actor").unwrap();
        assert!(actor.get_string("provider").is_ok());
    }

    #[test]
    fn extract_typed_value() {
        #[derive(serde::Deserialize, PartialEq, Debug)]
        struct Cluster {
            seed_nodes: Vec<String>,
            min_members: u32,
        }
        let toml = "[akka.cluster]\nseed_nodes = [\"a\", \"b\"]\nmin_members = 3\n";
        let c = Config::from_toml_str(toml).unwrap();
        let cl: Cluster = c.extract("akka.cluster").unwrap();
        assert_eq!(cl, Cluster { seed_nodes: vec!["a".into(), "b".into()], min_members: 3 });
    }

    #[test]
    fn extract_returns_not_found_for_missing_path() {
        let c = Config::empty();
        let r: Result<u32, _> = c.extract("missing.key");
        assert!(matches!(r, Err(ConfigError::NotFound(_))));
    }

    #[test]
    fn extract_returns_wrong_type_for_mismatch() {
        let c = Config::from_toml_str("[x]\ny = \"not a number\"\n").unwrap();
        let r: Result<u32, _> = c.extract("x.y");
        assert!(matches!(r, Err(ConfigError::WrongType { .. })));
    }
}
