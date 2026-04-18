"""Ping-pong actor via the Python facade."""

import rustakka
from rustakka import Actor, ActorSystem, props


class Pong(Actor):
    def __init__(self):
        self.count = 0

    async def handle(self, ctx, message):
        self.count += 1
        return {"echo": message, "count": self.count}


def test_ping_pong_ask():
    sys = ActorSystem.create_blocking("ping-pong-test")
    try:
        ref = sys.actor_of(props(Pong), "pong")
        reply = ref.ask_blocking({"n": 42}, 5.0)
        assert reply["echo"] == {"n": 42}
        assert reply["count"] == 1
        reply2 = ref.ask_blocking({"n": 43}, 5.0)
        assert reply2["count"] == 2
    finally:
        sys.terminate_blocking()


def test_tell_is_fire_and_forget():
    sys = ActorSystem.create_blocking("tell-test")
    try:
        ref = sys.actor_of(props(Pong), "p")
        ref.tell("hello")
    finally:
        sys.terminate_blocking()
