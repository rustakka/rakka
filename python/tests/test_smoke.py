"""Smoke tests — module imports, version, capabilities."""

import rakka


def test_version():
    assert isinstance(rakka.__version__, str)
    assert rakka.__version__


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
        assert hasattr(rakka, mod), f"missing facade: {mod}"


def test_compat_defaults():
    flags = rakka.compat_flags("json")
    assert flags is not None and flags["subinterpreter_safe"]
