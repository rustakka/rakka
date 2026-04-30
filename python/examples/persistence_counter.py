"""Persistent counter — writes events through the Rust journal, replays on start."""

from __future__ import annotations

import json

from rakka import persistence


def main() -> None:
    j = persistence.InMemoryJournal()
    pid = "counter-1"
    for i in range(1, 6):
        event = {"delta": 1, "seq": i}
        j.write(pid, i, json.dumps(event).encode())
    print("highest seq:", j.highest_sequence_nr(pid))
    total = sum(json.loads(bytes(e))["delta"] for e in j.replay(pid))
    print("replayed total:", total)


if __name__ == "__main__":
    main()
