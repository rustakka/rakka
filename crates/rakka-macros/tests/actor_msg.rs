use rakka_macros::actor_msg;

#[actor_msg]
#[allow(dead_code)]
enum MyMsg {
    A,
    B(u32),
}

#[test]
fn attribute_adds_debug() {
    let v = MyMsg::B(7);
    let s = format!("{:?}", v);
    assert!(s.contains("B(7)"));
}
