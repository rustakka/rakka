//! ddata-maps spec parity. akka.net:
//! `ORDictionarySpec`, `LWWDictionarySpec`, `PNCounterDictionarySpec`,
//! `ORMultiDictionarySpec`.

use atomr_distributed_data::{CrdtMerge, GCounter, LWWMap, ORMap, ORMultiMap, PNCounterMap};

fn merged<T: CrdtMerge>(a: &T, b: &T) -> T {
    let mut c = a.clone();
    c.merge(b);
    c
}

// --- ORMap ---------------------------------------------------------

#[test]
fn ormap_put_then_get() {
    let mut m: ORMap<String, GCounter> = ORMap::new();
    let mut g = GCounter::new();
    g.increment("n1", 5);
    m.put("k".into(), g);
    let got = m.get(&"k".to_string()).unwrap();
    assert_eq!(got.value(), 5);
}

#[test]
fn ormap_remove_clears_key() {
    let mut m: ORMap<String, GCounter> = ORMap::new();
    let mut g = GCounter::new();
    g.increment("n1", 1);
    m.put("k".into(), g);
    m.remove(&"k".to_string());
    assert!(m.get(&"k".to_string()).is_none());
}

#[test]
fn ormap_merge_unions_keys() {
    let mut a: ORMap<String, GCounter> = ORMap::new();
    let mut g = GCounter::new();
    g.increment("n1", 2);
    a.put("k1".into(), g);
    let mut b: ORMap<String, GCounter> = ORMap::new();
    let mut g2 = GCounter::new();
    g2.increment("n2", 3);
    b.put("k2".into(), g2);
    let m = merged(&a, &b);
    assert!(m.get(&"k1".to_string()).is_some());
    assert!(m.get(&"k2".to_string()).is_some());
}

#[test]
fn ormap_merge_combines_values_for_shared_key() {
    let mut a: ORMap<String, GCounter> = ORMap::new();
    let mut ga = GCounter::new();
    ga.increment("n1", 5);
    a.put("k".into(), ga);
    let mut b: ORMap<String, GCounter> = ORMap::new();
    let mut gb = GCounter::new();
    gb.increment("n2", 7);
    b.put("k".into(), gb);
    let m = merged(&a, &b);
    let v = m.get(&"k".to_string()).unwrap();
    assert_eq!(v.value(), 12, "shared key merges its inner CRDT");
}

// --- LWWMap --------------------------------------------------------

#[test]
fn lww_map_take_latest_timestamp() {
    let mut m: LWWMap<String, &'static str> = LWWMap::new();
    m.put("k".into(), "first", 1);
    m.put("k".into(), "second", 5);
    m.put("k".into(), "lost", 2);
    assert_eq!(m.get(&"k".to_string()).copied(), Some("second"));
}

#[test]
fn lww_map_merge_picks_max_timestamp() {
    let mut a: LWWMap<String, u32> = LWWMap::new();
    a.put("k".into(), 10, 1);
    let mut b: LWWMap<String, u32> = LWWMap::new();
    b.put("k".into(), 20, 2);
    let m = merged(&a, &b);
    assert_eq!(m.get(&"k".to_string()).copied(), Some(20));
}

// --- PNCounterMap --------------------------------------------------

#[test]
fn pn_counter_map_per_key_inc_dec() {
    let mut m: PNCounterMap<&'static str> = PNCounterMap::new();
    m.increment("a", "n1", 10);
    m.decrement("a", "n1", 3);
    m.increment("b", "n2", 4);
    assert_eq!(m.value(&"a"), 7);
    assert_eq!(m.value(&"b"), 4);
}

#[test]
fn pn_counter_map_unknown_key_is_zero() {
    let m: PNCounterMap<&'static str> = PNCounterMap::new();
    assert_eq!(m.value(&"missing"), 0);
}

#[test]
fn pn_counter_map_merge_combines_per_key() {
    let mut a: PNCounterMap<&'static str> = PNCounterMap::new();
    a.increment("k", "n1", 5);
    let mut b: PNCounterMap<&'static str> = PNCounterMap::new();
    b.increment("k", "n2", 3);
    let m = merged(&a, &b);
    assert_eq!(m.value(&"k"), 8);
}

// --- ORMultiMap ----------------------------------------------------

#[test]
fn or_multi_map_add_and_contains() {
    let mut m: ORMultiMap<&'static str, &'static str> = ORMultiMap::new();
    m.add("k", "v1");
    m.add("k", "v2");
    assert!(m.contains(&"k", &"v1"));
    assert!(m.contains(&"k", &"v2"));
    assert!(!m.contains(&"k", &"v3"));
}

#[test]
fn or_multi_map_remove_drops_value() {
    let mut m: ORMultiMap<&'static str, &'static str> = ORMultiMap::new();
    m.add("k", "v1");
    m.add("k", "v2");
    m.remove(&"k", &"v1");
    assert!(!m.contains(&"k", &"v1"));
    assert!(m.contains(&"k", &"v2"));
}

#[test]
fn or_multi_map_key_count_reflects_distinct_keys() {
    let mut m: ORMultiMap<&'static str, &'static str> = ORMultiMap::new();
    m.add("a", "v");
    m.add("b", "v");
    m.add("a", "w");
    assert_eq!(m.key_count(), 2);
}

#[test]
fn or_multi_map_merge_unions_per_key() {
    let mut a: ORMultiMap<&'static str, &'static str> = ORMultiMap::new();
    a.add("k", "v1");
    let mut b: ORMultiMap<&'static str, &'static str> = ORMultiMap::new();
    b.add("k", "v2");
    let m = merged(&a, &b);
    assert!(m.contains(&"k", &"v1"));
    assert!(m.contains(&"k", &"v2"));
}
