//! `ActorPath` — immutable, hierarchical, string-backed path.
//! akka.net: `Actor/ActorPath.cs`.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::address::Address;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathElement(String);

impl PathElement {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PathElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPath {
    pub address: Address,
    pub elements: Vec<PathElement>,
    pub uid: u64,
}

impl ActorPath {
    pub fn root(address: Address) -> Self {
        Self { address, elements: vec![], uid: 0 }
    }

    pub fn child(&self, name: impl Into<String>) -> Self {
        let mut e = self.elements.clone();
        e.push(PathElement::new(name));
        Self { address: self.address.clone(), elements: e, uid: 0 }
    }

    pub fn with_uid(mut self, uid: u64) -> Self {
        self.uid = uid;
        self
    }

    pub fn name(&self) -> &str {
        self.elements.last().map(|e| e.as_str()).unwrap_or("/")
    }

    pub fn parent(&self) -> Option<Self> {
        if self.elements.is_empty() {
            return None;
        }
        let mut e = self.elements.clone();
        e.pop();
        Some(Self { address: self.address.clone(), elements: e, uid: 0 })
    }

    pub fn depth(&self) -> usize {
        self.elements.len()
    }

    pub fn to_string_without_address(&self) -> String {
        let mut s = String::from("/");
        for (i, el) in self.elements.iter().enumerate() {
            if i > 0 {
                s.push('/');
            }
            s.push_str(el.as_str());
        }
        s
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.address, self.to_string_without_address())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_child_path() {
        let root = ActorPath::root(Address::local("S"));
        let user = root.child("user");
        let foo = user.child("foo");
        assert_eq!(foo.name(), "foo");
        assert_eq!(foo.depth(), 2);
        assert_eq!(foo.to_string(), "akka://S/user/foo");
    }

    #[test]
    fn parent_pops_element() {
        let root = ActorPath::root(Address::local("S")).child("user").child("a");
        assert_eq!(root.parent().unwrap().name(), "user");
    }
}
