//! Dotted config path (`akka.actor.default-dispatcher.throughput`).
//! akka.net: `Configuration/HoconPath.cs`.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigPath {
    segments: Vec<String>,
}

impl ConfigPath {
    pub fn parse(path: &str) -> Self {
        Self { segments: path.split('.').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect() }
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn push(&mut self, segment: impl Into<String>) {
        self.segments.push(segment.into());
    }
}

impl fmt::Display for ConfigPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.segments.join("."))
    }
}

impl From<&str> for ConfigPath {
    fn from(s: &str) -> Self {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dotted_path() {
        let p = ConfigPath::parse("akka.actor.default-dispatcher");
        assert_eq!(p.segments(), &["akka", "actor", "default-dispatcher"]);
        assert_eq!(p.to_string(), "akka.actor.default-dispatcher");
    }

    #[test]
    fn parses_empty() {
        assert!(ConfigPath::parse("").is_empty());
    }
}
