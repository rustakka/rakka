"""Smoke tests — module imports, version, capabilities."""

import rustakka


def test_version():
    assert isinstance(rustakka.__version__, str)
    assert rustakka.__version__


def test_submodules_present():
    for mod in [
        "testkit",
        "cluster",
        "cluster_tools",
        "cluster_sharding",
        "ddata",
        "persistence",
        "streams",
        "coordination",
        "discovery",
        "di",
        "hosting",
    ]:
        assert hasattr(rustakka, mod), f"missing facade: {mod}"


def test_compat_defaults():
    flags = rustakka.compat_flags("json")
    assert flags is not None and flags["subinterpreter_safe"]
