//! Specification combinators.

use atomr_patterns::specification::{FnSpec, Specification};

#[derive(Debug)]
struct Order {
    amount: i64,
    region: &'static str,
}

#[test]
fn and_or_not_compose_predicates() {
    let big = FnSpec(|o: &Order| o.amount > 100);
    let eu = FnSpec(|o: &Order| o.region == "EU");
    let big_and_eu = big.and(eu);

    assert!(big_and_eu.is_satisfied_by(&Order { amount: 150, region: "EU" }));
    assert!(!big_and_eu.is_satisfied_by(&Order { amount: 150, region: "US" }));
    assert!(!big_and_eu.is_satisfied_by(&Order { amount: 50, region: "EU" }));

    let small = FnSpec(|o: &Order| o.amount > 100).not();
    assert!(small.is_satisfied_by(&Order { amount: 50, region: "US" }));
    assert!(!small.is_satisfied_by(&Order { amount: 150, region: "US" }));

    let big_or_eu = FnSpec(|o: &Order| o.amount > 100).or(FnSpec(|o: &Order| o.region == "EU"));
    assert!(big_or_eu.is_satisfied_by(&Order { amount: 1, region: "EU" }));
    assert!(big_or_eu.is_satisfied_by(&Order { amount: 200, region: "US" }));
    assert!(!big_or_eu.is_satisfied_by(&Order { amount: 1, region: "US" }));
}
