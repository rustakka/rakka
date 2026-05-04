"""Interpreter pool + quota + compat registry tests."""

import atomr
from atomr import Actor, ActorSystem, InterpreterQuota, props


class Counter(Actor):
    def __init__(self):
        self.n = 0

    async def handle(self, ctx, msg):
        self.n += 1
        return self.n


def test_configure_interpreter_with_quota():
    sys = ActorSystem.create_blocking("interp-test")
    try:
        quota = InterpreterQuota(max_actors=100, max_handler_ms=1000)
        sys.configure_interpreter("workers", "python-subinterpreter-pool", 2, quota)
        ref = sys.actor_of(
            props(Counter, interpreter_role="workers", dispatcher="python-subinterpreter-pool"),
            "w",
        )
        for i in range(5):
            assert ref.ask_blocking(i, 5.0) == i + 1

        metrics = atomr._native.interpreter_metrics()
        labels = {m["label"] for m in metrics}
        assert "workers" in labels
        workers = [m for m in metrics if m["label"] == "workers"][0]
        assert workers["messages_handled"] >= 5
        assert workers["kind"] == "python-subinterpreter-pool"
    finally:
        sys.terminate_blocking()


def test_compat_declare_and_lookup():
    atomr.declare_compat("mylib", subinterpreter_safe=True, nogil_safe=False, notes="custom")
    flags = atomr.compat_flags("mylib")
    assert flags["subinterpreter_safe"] is True
    assert flags["nogil_safe"] is False
    assert flags["notes"] == "custom"


def test_interpreter_quota_rejects_overload():
    sys = ActorSystem.create_blocking("quota-test")
    try:
        quota = InterpreterQuota(max_actors=2)
        sys.configure_interpreter("tiny", "python-pinned", 1, quota)
        ref1 = sys.actor_of(props(Counter, interpreter_role="tiny"), "a1")
        ref2 = sys.actor_of(props(Counter, interpreter_role="tiny"), "a2")
        try:
            sys.actor_of(props(Counter, interpreter_role="tiny"), "a3")
            raised = False
        except atomr.InterpreterOverloaded:
            raised = True
        assert raised, "third actor should be rejected"
    finally:
        sys.terminate_blocking()
