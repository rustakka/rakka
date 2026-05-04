"""Smoke tests — module imports, version, capabilities."""

import atomr


def test_version():
    assert isinstance(atomr.__version__, str)
    assert atomr.__version__


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
        assert hasattr(atomr, mod), f"missing facade: {mod}"


def test_compat_defaults():
    flags = atomr.compat_flags("json")
    assert flags is not None and flags["subinterpreter_safe"]
