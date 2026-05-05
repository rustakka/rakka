//! HOCON-subset parser for migrating Akka.NET / Pekko `reference.conf`
//! files (Phase 2 of `docs/full-port-plan.md`).
//!
//! Supports the slice of HOCON that the upstream `reference.conf`
//! files actually use:
//!
//! * `key = value` and `key : value` assignments;
//! * dotted keys (`akka.actor.provider = "local"`);
//! * nested objects (`akka { actor { provider = "local" } }`);
//! * arrays (`["a", "b"]`);
//! * single-line `#` and `//` comments, multi-line `/* … */` comments;
//! * triple-quoted strings (`"""…"""`);
//! * `include "file"` of a file relative to the current path;
//! * `${path.to.value}` strict substitutions (errors if missing);
//! * `${?ENV_VAR}` optional env-var substitutions
//!   (silently skipped if absent).
//!
//! What it does **not** support yet (folded into a follow-on if a
//! real `reference.conf` needs them): unquoted multi-token strings,
//! self-referential substitutions, list concatenation across lines,
//! `${path}` references that mix scalar + object types, durations
//! parsed at parse time (we keep those as strings; `Config::
//! get_duration` already parses on read).
//!
//! The parser is intentionally hand-written (no `nom` / `pest`
//! dependency) — it's <500 LOC and the syntax surface is small.

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::value::ConfigValue;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HoconError {
    #[error("unexpected character `{ch}` at line {line}, col {col}")]
    Unexpected { ch: char, line: usize, col: usize },
    #[error("unterminated {kind} at line {line}")]
    Unterminated { kind: &'static str, line: usize },
    #[error("unknown substitution `${{{key}}}` (no such config key)")]
    MissingSubstitution { key: String },
    #[error("include error: {0}")]
    Include(String),
    #[error("io error reading `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("expected {expected}, found `{found}` at line {line}")]
    Expected { expected: &'static str, found: String, line: usize },
}

/// Parse a HOCON document and return the merged root object.
///
/// `base_dir` controls how `include "rel/path"` resolves. Pass
/// `Path::new(".")` when parsing in-memory documents without a
/// physical anchor.
pub fn parse(text: &str, base_dir: &Path) -> Result<ConfigValue, HoconError> {
    let mut p = Parser::new(text, base_dir.to_path_buf());
    let root = p.parse_root()?;
    let resolved = resolve_substitutions(root)?;
    Ok(resolved)
}

/// Parse a HOCON file from disk, resolving `include` relative to its
/// parent directory.
pub fn parse_file(path: &Path) -> Result<ConfigValue, HoconError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| HoconError::Io { path: path.display().to_string(), source: e })?;
    let base = path.parent().unwrap_or(Path::new(".")).to_path_buf();
    parse(&text, &base)
}

// -- Parser -----------------------------------------------------------

struct Parser<'a> {
    src: &'a [u8],
    pos: usize,
    line: usize,
    col: usize,
    base_dir: PathBuf,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str, base_dir: PathBuf) -> Self {
        Self { src: src.as_bytes(), pos: 0, line: 1, col: 1, base_dir }
    }

    fn parse_root(&mut self) -> Result<ConfigValue, HoconError> {
        self.skip_ws_and_comments();
        // A HOCON root may omit the surrounding braces.
        let mut obj = if self.peek() == Some(b'{') {
            self.advance(1);
            let o = self.parse_object_body(b'}')?;
            self.skip_ws_and_comments();
            o
        } else {
            self.parse_object_body(0)?
        };
        // Allow trailing whitespace.
        self.skip_ws_and_comments();
        if self.pos < self.src.len() {
            return Err(HoconError::Unexpected {
                ch: self.src[self.pos] as char,
                line: self.line,
                col: self.col,
            });
        }
        merge_in_place(&mut obj, BTreeMap::new());
        Ok(ConfigValue::Object(obj))
    }

    /// Parse an object body — either until `terminator` (e.g. `b'}'`)
    /// or end-of-input if `terminator == 0`.
    fn parse_object_body(&mut self, terminator: u8) -> Result<BTreeMap<String, ConfigValue>, HoconError> {
        let mut obj: BTreeMap<String, ConfigValue> = BTreeMap::new();
        loop {
            self.skip_ws_and_comments();
            match self.peek() {
                None => {
                    if terminator != 0 {
                        return Err(HoconError::Unterminated { kind: "object", line: self.line });
                    }
                    return Ok(obj);
                }
                Some(c) if c == terminator => {
                    self.advance(1);
                    return Ok(obj);
                }
                Some(b',') | Some(b'\n') | Some(b';') => {
                    self.advance(1);
                    continue;
                }
                _ => {}
            }

            // include "..."
            if self.starts_with_keyword("include") {
                self.advance(7);
                self.skip_ws_inline();
                let path = self.parse_string()?;
                let included = self.do_include(&path)?;
                if let ConfigValue::Object(child) = included {
                    deep_merge(&mut obj, child);
                } else {
                    return Err(HoconError::Include(format!(
                        "included file `{path}` did not resolve to an object"
                    )));
                }
                continue;
            }

            // key (.key)*  ('=' | ':' | '{' | '+=')  value
            let key = self.parse_key()?;
            self.skip_ws_inline();
            let next = self.peek();
            let (value, append) = match next {
                Some(b'{') => {
                    self.advance(1);
                    let inner = self.parse_object_body(b'}')?;
                    (ConfigValue::Object(inner), false)
                }
                Some(b'+') if self.peek_at(1) == Some(b'=') => {
                    // akka HOCON `key += value` — append to existing array.
                    self.advance(2);
                    self.skip_ws_inline();
                    (self.parse_value()?, true)
                }
                Some(b'=') | Some(b':') => {
                    self.advance(1);
                    self.skip_ws_inline();
                    (self.parse_value()?, false)
                }
                Some(c) => {
                    return Err(HoconError::Expected {
                        expected: "= or : or { or +=",
                        found: format!("{}", c as char),
                        line: self.line,
                    })
                }
                None => return Err(HoconError::Unterminated { kind: "assignment", line: self.line }),
            };
            if append {
                append_dotted(&mut obj, &key, value);
            } else {
                insert_dotted(&mut obj, &key, value);
            }
        }
    }

    fn parse_key(&mut self) -> Result<Vec<String>, HoconError> {
        let mut parts = Vec::new();
        loop {
            self.skip_ws_inline();
            let part = if self.peek() == Some(b'"') {
                self.parse_string()?
            } else {
                let start = self.pos;
                while let Some(c) = self.peek() {
                    if c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-') {
                        self.advance(1);
                    } else {
                        break;
                    }
                }
                if start == self.pos {
                    return Err(HoconError::Expected {
                        expected: "key",
                        found: self.peek().map(|c| (c as char).to_string()).unwrap_or_default(),
                        line: self.line,
                    });
                }
                std::str::from_utf8(&self.src[start..self.pos])
                    .map_err(|_| HoconError::Unexpected { ch: '\0', line: self.line, col: self.col })?
                    .to_string()
            };
            parts.push(part);
            if self.peek() == Some(b'.') {
                self.advance(1);
                continue;
            }
            return Ok(parts);
        }
    }

    fn parse_value(&mut self) -> Result<ConfigValue, HoconError> {
        self.skip_ws_inline();
        match self.peek() {
            Some(b'"') => Ok(ConfigValue::String(self.parse_string()?)),
            Some(b'[') => self.parse_array(),
            Some(b'{') => {
                self.advance(1);
                let inner = self.parse_object_body(b'}')?;
                Ok(ConfigValue::Object(inner))
            }
            Some(b'$') if self.peek_at(1) == Some(b'{') => {
                let sub = self.parse_substitution()?;
                Ok(sub)
            }
            Some(_) => self.parse_unquoted_scalar(),
            None => Err(HoconError::Unterminated { kind: "value", line: self.line }),
        }
    }

    fn parse_string(&mut self) -> Result<String, HoconError> {
        // Triple-quoted?
        if self.starts_with(b"\"\"\"") {
            self.advance(3);
            let start = self.pos;
            while self.pos + 2 < self.src.len() && &self.src[self.pos..self.pos + 3] != b"\"\"\"" {
                if self.src[self.pos] == b'\n' {
                    self.line += 1;
                    self.col = 1;
                } else {
                    self.col += 1;
                }
                self.pos += 1;
            }
            if self.pos + 2 >= self.src.len() {
                return Err(HoconError::Unterminated { kind: "string", line: self.line });
            }
            let s = std::str::from_utf8(&self.src[start..self.pos])
                .map_err(|_| HoconError::Unterminated { kind: "string", line: self.line })?
                .to_string();
            self.advance(3);
            return Ok(s);
        }
        if self.peek() != Some(b'"') {
            return Err(HoconError::Expected {
                expected: "\"",
                found: self.peek().map(|c| (c as char).to_string()).unwrap_or_default(),
                line: self.line,
            });
        }
        self.advance(1);
        let mut out = String::new();
        loop {
            match self.peek() {
                None | Some(b'\n') => {
                    return Err(HoconError::Unterminated { kind: "string", line: self.line })
                }
                Some(b'"') => {
                    self.advance(1);
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.advance(1);
                    match self.peek() {
                        Some(b'n') => {
                            out.push('\n');
                            self.advance(1);
                        }
                        Some(b't') => {
                            out.push('\t');
                            self.advance(1);
                        }
                        Some(b'r') => {
                            out.push('\r');
                            self.advance(1);
                        }
                        Some(b'"') => {
                            out.push('"');
                            self.advance(1);
                        }
                        Some(b'\\') => {
                            out.push('\\');
                            self.advance(1);
                        }
                        Some(b'/') => {
                            out.push('/');
                            self.advance(1);
                        }
                        Some(c) => {
                            out.push(c as char);
                            self.advance(1);
                        }
                        None => {
                            return Err(HoconError::Unterminated { kind: "string-escape", line: self.line })
                        }
                    }
                }
                Some(c) => {
                    out.push(c as char);
                    self.advance(1);
                }
            }
        }
    }

    fn parse_array(&mut self) -> Result<ConfigValue, HoconError> {
        debug_assert_eq!(self.peek(), Some(b'['));
        self.advance(1);
        let mut items = Vec::new();
        loop {
            self.skip_ws_and_comments();
            match self.peek() {
                Some(b']') => {
                    self.advance(1);
                    return Ok(ConfigValue::Array(items));
                }
                Some(b',') | Some(b'\n') => {
                    self.advance(1);
                }
                Some(_) => {
                    let v = self.parse_value()?;
                    items.push(v);
                }
                None => return Err(HoconError::Unterminated { kind: "array", line: self.line }),
            }
        }
    }

    fn parse_substitution(&mut self) -> Result<ConfigValue, HoconError> {
        // `${path}` or `${?path}`
        debug_assert_eq!(self.peek(), Some(b'$'));
        self.advance(1);
        debug_assert_eq!(self.peek(), Some(b'{'));
        self.advance(1);
        let optional = self.peek() == Some(b'?');
        if optional {
            self.advance(1);
        }
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'}' {
                break;
            }
            self.advance(1);
        }
        if self.peek() != Some(b'}') {
            return Err(HoconError::Unterminated { kind: "substitution", line: self.line });
        }
        let key = std::str::from_utf8(&self.src[start..self.pos])
            .map_err(|_| HoconError::Unterminated { kind: "substitution", line: self.line })?
            .trim()
            .to_string();
        self.advance(1);
        // Stash as a placeholder; resolver replaces in pass 2.
        let marker = if optional { format!("__atomr_sub_opt::{key}") } else { format!("__atomr_sub::{key}") };
        Ok(ConfigValue::String(marker))
    }

    fn parse_unquoted_scalar(&mut self) -> Result<ConfigValue, HoconError> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if matches!(c, b',' | b'\n' | b'}' | b']' | b';' | b'#') {
                break;
            }
            if c == b'/' && self.peek_at(1) == Some(b'/') {
                break;
            }
            self.advance(1);
        }
        let raw = std::str::from_utf8(&self.src[start..self.pos])
            .map_err(|_| HoconError::Unexpected { ch: '\0', line: self.line, col: self.col })?
            .trim();
        if raw.is_empty() {
            return Err(HoconError::Expected { expected: "value", found: String::new(), line: self.line });
        }
        Ok(scalar_from_str(raw))
    }

    fn do_include(&self, rel: &str) -> Result<ConfigValue, HoconError> {
        let p = self.base_dir.join(rel);
        parse_file(&p)
    }

    // -- low-level cursor helpers --

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }
    fn peek_at(&self, off: usize) -> Option<u8> {
        self.src.get(self.pos + off).copied()
    }
    fn starts_with(&self, needle: &[u8]) -> bool {
        self.src.len() >= self.pos + needle.len() && &self.src[self.pos..self.pos + needle.len()] == needle
    }
    fn starts_with_keyword(&self, kw: &str) -> bool {
        if !self.starts_with(kw.as_bytes()) {
            return false;
        }
        match self.src.get(self.pos + kw.len()) {
            None => true,
            Some(c) => !c.is_ascii_alphanumeric() && *c != b'_',
        }
    }
    fn advance(&mut self, n: usize) {
        for _ in 0..n {
            if self.pos >= self.src.len() {
                break;
            }
            if self.src[self.pos] == b'\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
            self.pos += 1;
        }
    }
    fn skip_ws_inline(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' {
                self.advance(1);
            } else {
                break;
            }
        }
    }
    fn skip_ws_and_comments(&mut self) {
        loop {
            match self.peek() {
                Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r') => {
                    self.advance(1);
                }
                Some(b'#') => {
                    while let Some(c) = self.peek() {
                        if c == b'\n' {
                            break;
                        }
                        self.advance(1);
                    }
                }
                Some(b'/') if self.peek_at(1) == Some(b'/') => {
                    while let Some(c) = self.peek() {
                        if c == b'\n' {
                            break;
                        }
                        self.advance(1);
                    }
                }
                Some(b'/') if self.peek_at(1) == Some(b'*') => {
                    self.advance(2);
                    while !(self.peek() == Some(b'*') && self.peek_at(1) == Some(b'/')) {
                        if self.peek().is_none() {
                            return;
                        }
                        self.advance(1);
                    }
                    self.advance(2);
                }
                _ => return,
            }
        }
    }
}

fn scalar_from_str(s: &str) -> ConfigValue {
    if s == "null" {
        return ConfigValue::Null;
    }
    if s == "true" {
        return ConfigValue::Bool(true);
    }
    if s == "false" {
        return ConfigValue::Bool(false);
    }
    if let Ok(i) = s.parse::<i64>() {
        return ConfigValue::Int(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return ConfigValue::Float(f);
    }
    ConfigValue::String(s.to_string())
}

/// Walk `obj` along `key`, ensuring nested objects exist, then append
/// `value` to the array at the leaf. If the leaf does not exist yet,
/// create a fresh array; if it exists but is not an array, replace it
/// with a single-element array. Akka HOCON `key += value`.
fn append_dotted(obj: &mut BTreeMap<String, ConfigValue>, key: &[String], value: ConfigValue) {
    if key.is_empty() {
        return;
    }
    if key.len() == 1 {
        let leaf = obj.entry(key[0].clone()).or_insert_with(|| ConfigValue::Array(Vec::new()));
        match leaf {
            ConfigValue::Array(items) => items.push(value),
            other => {
                *other = ConfigValue::Array(vec![value]);
            }
        }
        return;
    }
    let entry = obj.entry(key[0].clone()).or_insert_with(|| ConfigValue::Object(BTreeMap::new()));
    if let ConfigValue::Object(child) = entry {
        append_dotted(child, &key[1..], value);
    } else {
        let mut new_child: BTreeMap<String, ConfigValue> = BTreeMap::new();
        append_dotted(&mut new_child, &key[1..], value);
        *entry = ConfigValue::Object(new_child);
    }
}

fn insert_dotted(obj: &mut BTreeMap<String, ConfigValue>, key: &[String], value: ConfigValue) {
    if key.is_empty() {
        return;
    }
    if key.len() == 1 {
        if let Some(ConfigValue::Object(existing)) = obj.get_mut(&key[0]) {
            if let ConfigValue::Object(new_obj) = value {
                deep_merge(existing, new_obj);
                return;
            }
        }
        obj.insert(key[0].clone(), value);
        return;
    }
    let entry = obj.entry(key[0].clone()).or_insert_with(|| ConfigValue::Object(BTreeMap::new()));
    if let ConfigValue::Object(child) = entry {
        insert_dotted(child, &key[1..], value);
    } else {
        let mut new_child: BTreeMap<String, ConfigValue> = BTreeMap::new();
        insert_dotted(&mut new_child, &key[1..], value);
        *entry = ConfigValue::Object(new_child);
    }
}

fn deep_merge(into: &mut BTreeMap<String, ConfigValue>, from: BTreeMap<String, ConfigValue>) {
    for (k, v) in from {
        match (into.get_mut(&k), v) {
            (Some(ConfigValue::Object(a)), ConfigValue::Object(b)) => {
                deep_merge(a, b);
            }
            (_, v) => {
                into.insert(k, v);
            }
        }
    }
}

fn merge_in_place(_into: &mut BTreeMap<String, ConfigValue>, _from: BTreeMap<String, ConfigValue>) {}

// -- Substitution resolution -----------------------------------------

fn resolve_substitutions(v: ConfigValue) -> Result<ConfigValue, HoconError> {
    let snapshot = v.clone();
    resolve_in(v, &snapshot)
}

fn resolve_in(v: ConfigValue, root: &ConfigValue) -> Result<ConfigValue, HoconError> {
    match v {
        ConfigValue::String(s) => {
            if let Some(rest) = s.strip_prefix("__atomr_sub::") {
                let lookup = lookup_path(root, rest);
                lookup.ok_or_else(|| HoconError::MissingSubstitution { key: rest.to_string() })
            } else if let Some(rest) = s.strip_prefix("__atomr_sub_opt::") {
                Ok(env::var(rest).map(ConfigValue::String).unwrap_or(ConfigValue::Null))
            } else {
                Ok(ConfigValue::String(s))
            }
        }
        ConfigValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(resolve_in(it, root)?);
            }
            Ok(ConfigValue::Array(out))
        }
        ConfigValue::Object(o) => {
            let mut out = BTreeMap::new();
            for (k, v) in o {
                out.insert(k, resolve_in(v, root)?);
            }
            Ok(ConfigValue::Object(out))
        }
        other => Ok(other),
    }
}

fn lookup_path(root: &ConfigValue, path: &str) -> Option<ConfigValue> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = match cur {
            ConfigValue::Object(o) => o.get(seg)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn parse_str(s: &str) -> ConfigValue {
        parse(s, Path::new(".")).unwrap()
    }

    #[test]
    fn flat_assignments() {
        let v = parse_str("a = 1\nb = \"hi\"\nc = true");
        if let ConfigValue::Object(o) = v {
            assert_eq!(o.get("a"), Some(&ConfigValue::Int(1)));
            assert_eq!(o.get("b"), Some(&ConfigValue::String("hi".into())));
            assert_eq!(o.get("c"), Some(&ConfigValue::Bool(true)));
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn dotted_keys_create_nested_objects() {
        let v = parse_str("akka.actor.provider = \"local\"");
        if let ConfigValue::Object(o) = v {
            let actor = o.get("akka").unwrap();
            if let ConfigValue::Object(a) = actor {
                let inner = a.get("actor").unwrap();
                if let ConfigValue::Object(b) = inner {
                    assert_eq!(b.get("provider"), Some(&ConfigValue::String("local".into())));
                    return;
                }
            }
        }
        panic!("nested structure missing");
    }

    #[test]
    fn nested_object_syntax() {
        let v = parse_str("akka { actor { provider = \"local\" } }");
        if let ConfigValue::Object(o) = v {
            let s = lookup_path(&ConfigValue::Object(o.clone()), "akka.actor.provider");
            assert_eq!(s, Some(ConfigValue::String("local".into())));
        }
    }

    #[test]
    fn comments_ignored() {
        let v = parse_str("# comment\na = 1 // inline\n/* block */\nb = 2");
        if let ConfigValue::Object(o) = v {
            assert_eq!(o.get("a"), Some(&ConfigValue::Int(1)));
            assert_eq!(o.get("b"), Some(&ConfigValue::Int(2)));
        }
    }

    #[test]
    fn substitution_resolves() {
        let src = "host = \"example.com\"\nurl = ${host}";
        let v = parse_str(src);
        let s = lookup_path(&v, "url");
        assert_eq!(s, Some(ConfigValue::String("example.com".into())));
    }

    #[test]
    fn missing_substitution_errors() {
        let r = parse("missing = ${nope}", Path::new("."));
        assert!(matches!(r, Err(HoconError::MissingSubstitution { .. })));
    }

    #[test]
    fn optional_env_substitution_returns_null_when_unset() {
        // Choose an env var name unlikely to exist.
        std::env::remove_var("ATOMR_TEST_HOCON_UNSET_X9Z");
        let v = parse_str("x = ${?ATOMR_TEST_HOCON_UNSET_X9Z}");
        assert_eq!(lookup_path(&v, "x"), Some(ConfigValue::Null));
    }

    #[test]
    fn optional_env_substitution_returns_value_when_set() {
        std::env::set_var("ATOMR_TEST_HOCON_SET_K1", "from-env");
        let v = parse_str("x = ${?ATOMR_TEST_HOCON_SET_K1}");
        assert_eq!(lookup_path(&v, "x"), Some(ConfigValue::String("from-env".into())));
        std::env::remove_var("ATOMR_TEST_HOCON_SET_K1");
    }

    #[test]
    fn arrays_parse() {
        let v = parse_str("xs = [1, 2, 3]");
        let xs = lookup_path(&v, "xs").unwrap();
        if let ConfigValue::Array(items) = xs {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], ConfigValue::Int(1));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn dotted_assignment_does_not_clobber_sibling() {
        let v = parse_str("akka.actor.provider = \"local\"\nakka.actor.dispatcher = \"default\"");
        assert_eq!(lookup_path(&v, "akka.actor.provider"), Some(ConfigValue::String("local".into())));
        assert_eq!(lookup_path(&v, "akka.actor.dispatcher"), Some(ConfigValue::String("default".into())));
    }

    #[test]
    fn triple_quoted_string() {
        let v = parse_str("x = \"\"\"line1\nline2\"\"\"");
        assert_eq!(lookup_path(&v, "x"), Some(ConfigValue::String("line1\nline2".into())));
    }

    #[test]
    fn append_creates_array_when_absent() {
        let v = parse_str("xs += 1\nxs += 2");
        if let Some(ConfigValue::Array(items)) = lookup_path(&v, "xs") {
            assert_eq!(items, vec![ConfigValue::Int(1), ConfigValue::Int(2)]);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn append_extends_existing_array() {
        let v = parse_str("xs = [1, 2]\nxs += 3");
        if let Some(ConfigValue::Array(items)) = lookup_path(&v, "xs") {
            assert_eq!(items.len(), 3);
            assert_eq!(items[2], ConfigValue::Int(3));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn append_with_dotted_key() {
        let v = parse_str("akka.actor.deployers += \"local\"\nakka.actor.deployers += \"remote\"");
        if let Some(ConfigValue::Array(items)) = lookup_path(&v, "akka.actor.deployers") {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected nested array");
        }
    }

    #[test]
    fn substitution_inside_array_resolves() {
        let v = parse_str("base = \"x\"\nxs = [${base}, ${base}]");
        if let Some(ConfigValue::Array(items)) = lookup_path(&v, "xs") {
            assert_eq!(items, vec![ConfigValue::String("x".into()), ConfigValue::String("x".into())]);
        } else {
            panic!("expected array");
        }
    }
}
