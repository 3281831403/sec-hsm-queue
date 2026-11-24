"""Five behaviour and edge tests for the HSM command queue.

These tests complement the ten defect contracts: two ordered commands keep
their own results, repeated execution stays idempotent, an expired command is
rejected, an evicted result cannot be replayed, and isolated batches match the
same queues run alone.
"""

import pytest

from hsmq.queue import (
    execute,
    queue_batch,
    replay,
    submit,
    validate_state,
)


def make_state(**overrides):
    """Build a complete queue state for a behaviour test."""
    state = {
        "now": 0.0,
        "next_seq": 1,
        "seq_limit": 64,
        "default_timeout": 10.0,
        "cache_limit": 3,
        "commands": [],
        "cache": {},
        "journal": [],
    }
    state.update(overrides)
    return state


def make_command(payload="digest", ttl=50.0, op="sign"):
    """Build a command request with a long lifetime by default."""
    return {"op": op, "payload": payload, "ttl": ttl}


def test_two_commands_execute_to_their_own_results():
    state = make_state()
    first_record = submit(state, make_command("first"), now=0.0)
    second_record = submit(state, make_command("second"), now=1.0)

    first = execute(state, first_record["seq"], now=2.0)
    second = execute(state, second_record["seq"], now=3.0)

    assert first == {
        "seq": 1,
        "op": "sign",
        "payload": "first",
        "executed_at": 2.0,
    }
    assert second == {
        "seq": 2,
        "op": "sign",
        "payload": "second",
        "executed_at": 3.0,
    }
    assert state["journal"] == [
        {"seq": 1, "at": 2.0},
        {"seq": 2, "at": 3.0},
    ]
    assert [record["status"] for record in state["commands"]] == [
        "completed",
        "completed",
    ]


def test_repeated_execute_returns_the_same_result():
    state = make_state()
    first_record = submit(state, make_command("alpha"), now=0.0)
    second_record = submit(state, make_command("beta"), now=0.5)

    first = execute(state, first_record["seq"], now=1.0)
    second = execute(state, second_record["seq"], now=2.0)
    first_again = execute(state, first_record["seq"], now=3.0)
    second_again = execute(state, second_record["seq"], now=4.0)

    assert first_again == first
    assert second_again == second
    assert replay(state, first_record["seq"], now=5.0) == first
    assert replay(state, second_record["seq"], now=6.0) == second
    assert state["journal"] == [
        {"seq": 1, "at": 1.0},
        {"seq": 2, "at": 2.0},
    ]
    assert state["commands"][0]["completed_at"] == 1.0
    assert state["commands"][1]["completed_at"] == 2.0


def test_expired_command_cannot_execute():
    state = make_state()
    record = submit(state, make_command("too-late", ttl=1.0), now=10.0)

    with pytest.raises(TimeoutError):
        execute(state, record["seq"], now=11.5)
    with pytest.raises(TimeoutError):
        execute(state, record["seq"], now=12.0)

    assert state["journal"] == []
    assert state["cache"] == {}
    assert state["commands"][0]["status"] == "expired"
    assert validate_state(state) == []


def test_evicted_oldest_result_cannot_be_replayed():
    state = make_state(cache_limit=2)
    records = [
        submit(state, make_command(f"cache-{index}", ttl=100.0), now=0.0)
        for index in range(1, 4)
    ]
    originals = [
        execute(state, record["seq"], now=float(index))
        for index, record in enumerate(records, start=1)
    ]

    assert set(state["cache"]) == {2, 3}
    with pytest.raises(KeyError):
        replay(state, records[0]["seq"], now=4.0)
    with pytest.raises(KeyError):
        execute(state, records[0]["seq"], now=4.0)

    assert len(state["journal"]) == 3
    assert state["commands"][0]["status"] == "completed"
    assert replay(state, records[1]["seq"], now=5.0) == originals[1]
    assert replay(state, records[2]["seq"], now=6.0) == originals[2]


def test_independent_batches_match_solo_runs():
    config = {
        "seq_limit": 20,
        "default_timeout": 5.0,
        "cache_limit": 3,
    }
    requests = [
        {"queue": "alpha", "command": make_command("a1")},
        {"queue": "beta", "command": make_command("b1")},
        {"queue": "alpha", "command": make_command("a2")},
        {"queue": "beta", "command": make_command("b2")},
        {"queue": "alpha", "command": make_command("a3")},
        {"queue": "beta", "command": make_command("b3")},
    ]

    mixed = queue_batch(requests, config, now=7.0)

    for queue in ("alpha", "beta"):
        solo_requests = [
            request for request in requests if request["queue"] == queue
        ]
        solo = queue_batch(solo_requests, config, now=7.0)
        assert mixed["queues"][queue] == solo["queues"][queue]
        assert [
            result["seq"] for result in mixed["queues"][queue]["results"]
        ] == [1, 2, 3]

    assert mixed["queues"]["alpha"] is not mixed["queues"]["beta"]
    assert validate_state(mixed["queues"]["alpha"]["state"]) == []
    assert validate_state(mixed["queues"]["beta"]["state"]) == []