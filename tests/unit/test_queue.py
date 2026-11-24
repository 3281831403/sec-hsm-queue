"""Ten contract tests for the hardware security module command queue.

Each test exercises one of the ten defects in the task statement.  A correct
implementation must keep cached replay exact, enforce the deadline boundary,
bound the result cache and isolate queues after a reconnect.
"""

import copy
import json

import pytest

from hsmq.queue import (
    cache_result,
    execute,
    next_seq,
    queue_batch,
    replay,
    submit,
    timeout_check,
    validate_state,
)


def make_state(**overrides):
    """Build a small but complete queue state for a test."""
    state = {
        "now": 0.0,
        "next_seq": 1,
        "seq_limit": 64,
        "default_timeout": 10.0,
        "cache_limit": 2,
        "commands": [],
        "cache": {},
        "journal": [],
    }
    state.update(overrides)
    return state


def make_command(payload="digest", ttl=5.0, op="sign"):
    """Build a command request; ttl is the per-command lifetime."""
    return {"op": op, "payload": payload, "ttl": ttl}


def make_record(seq, deadline, status="pending"):
    """Build a raw command record for validation and cache tests."""
    return {
        "seq": seq,
        "command": make_command("raw", ttl=100.0),
        "submitted_at": 0.0,
        "deadline": deadline,
        "status": status,
        "completed_at": None,
    }


def test_next_seq_is_persistent_and_rejects_wraparound():
    # Defect 1: the counter lives in the state, survives a reconnect and the
    # allocation before an overflow is refused instead of wrapping to zero.
    state = make_state(next_seq=41, seq_limit=43)
    assert next_seq(state) == 41

    restored = json.loads(json.dumps(state))
    assert restored["next_seq"] == 42
    assert next_seq(restored) == 42
    assert next_seq(restored) == 43
    assert restored["next_seq"] == 44

    with pytest.raises(OverflowError):
        next_seq(restored)
    assert restored["next_seq"] == 44

    assert next_seq(make_state()) == 1


def test_submit_assigns_unique_increasing_sequences():
    # Defect 2: two submissions must receive different sequence numbers, and a
    # poisoned state must expose the duplicate instead of silently aliasing it.
    state = make_state()
    source = make_command("first")
    first = submit(state, source, now=1.0)
    second = submit(state, make_command("second"), now=2.0)

    assert first["seq"] == 1
    assert second["seq"] == 2
    assert [record["seq"] for record in state["commands"]] == [1, 2]
    assert state["next_seq"] == 3
    assert state["now"] == 2.0

    assert first["status"] == "pending"
    assert first["completed_at"] is None
    assert first["submitted_at"] == 1.0
    assert first["deadline"] == 6.0
    assert second["deadline"] == 7.0

    source["payload"] = "mutated"
    assert first["command"] == make_command("first")

    state["commands"].append(copy.deepcopy(first))
    assert validate_state(state) == ["duplicate_seq"]


def test_execute_does_not_run_same_sequence_twice():
    # Defect 3: the cache is checked before the deadline, so even a late retry
    # returns the original result and the journal gains exactly one entry.
    state = make_state()
    record = submit(state, make_command("once", ttl=10.0), now=0.0)

    first = execute(state, record["seq"], now=1.0)
    second = execute(state, record["seq"], now=99.0)

    assert first == {
        "seq": 1,
        "op": "sign",
        "payload": "once",
        "executed_at": 1.0,
    }
    assert second == first
    assert state["journal"] == [{"seq": 1, "at": 1.0}]
    assert state["commands"][0]["status"] == "completed"
    assert state["commands"][0]["completed_at"] == 1.0
    assert state["cache"][1]["completed_at"] == 1.0


def test_replay_returns_original_cached_result():
    # Defect 4: replay must not execute again or mint a fresh executed_at; it
    # returns an independent copy of the first result.
    state = make_state()
    record = submit(state, make_command("signed", ttl=50.0), now=0.0)

    with pytest.raises(KeyError):
        replay(state, record["seq"], now=1.0)
    assert state["journal"] == []

    original = execute(state, record["seq"], now=2.0)

    replayed = replay(state, record["seq"], now=99.0)

    assert replayed == original
    assert replayed["executed_at"] == 2.0
    assert replayed is not original

    replayed["payload"] = "tampered"
    assert replay(state, record["seq"], now=100.0)["payload"] == "signed"
    assert state["journal"] == [{"seq": 1, "at": 2.0}]


def test_timeout_at_exact_deadline_is_not_expired():
    # Defect 5: expiration is strictly after the deadline, so equality is
    # still executable through the whole queue, not just in the predicate.
    command = make_command("boundary")
    assert timeout_check(command, now=4.999, deadline=5.0) is False
    assert timeout_check(command, now=5.0, deadline=5.0) is False
    assert timeout_check(command, now=5.000001, deadline=5.0) is True
    assert timeout_check(command, now=6.0, deadline=5.0) is True

    state = make_state()
    record = submit(state, command, now=0.0)
    result = execute(state, record["seq"], now=5.0)
    assert result == {
        "seq": 1,
        "op": "sign",
        "payload": "boundary",
        "executed_at": 5.0,
    }
    assert state["commands"][0]["status"] == "completed"
    assert state["commands"][0]["completed_at"] == 5.0
    assert state["journal"] == [{"seq": 1, "at": 5.0}]


def test_timed_out_command_is_rejected_without_side_effect():
    # Defect 6: a late command is marked expired before its side effect, and
    # every retry is rejected without touching the journal or result cache.
    state = make_state()
    record = submit(state, make_command("late", ttl=2.0), now=0.0)

    with pytest.raises(TimeoutError):
        execute(state, record["seq"], now=3.0)

    assert state["journal"] == []
    assert state["cache"] == {}
    assert state["commands"][0]["status"] == "expired"
    assert state["now"] == 3.0

    with pytest.raises(TimeoutError):
        execute(state, record["seq"], now=4.0)

    assert state["journal"] == []
    assert state["cache"] == {}


def test_cache_enforces_configured_limit():
    # Defect 7: the cache is bounded; after four completions with a limit of
    # two, only the two newest results remain replayable.
    state = make_state(cache_limit=2)
    records = [
        submit(state, make_command(f"payload-{index}", ttl=100.0), now=0.0)
        for index in range(1, 5)
    ]

    results = [
        execute(state, record["seq"], now=float(index))
        for index, record in enumerate(records, start=1)
    ]

    assert len(results) == 4
    assert len(state["cache"]) == 2
    assert set(state["cache"]) == {3, 4}
    assert state["cache"][3]["completed_at"] == 3.0
    assert state["cache"][4]["completed_at"] == 4.0
    assert [entry["seq"] for entry in state["commands"]] == [1, 2, 3, 4]
    assert all(entry["status"] == "completed" for entry in state["commands"])
    assert len(state["journal"]) == 4
    assert validate_state(state) == []


def test_cache_eviction_uses_completion_time_and_protects_pending():
    # Defect 8: insertion order is seq 1, 2, 99 while completion order makes 2
    # the oldest completed entry.  Incomplete entry 99 must survive.
    state = make_state(cache_limit=3)
    for payload in ("one", "two", "three"):
        submit(state, make_command(payload, ttl=100.0), now=0.0)

    state["cache"] = {
        1: {"seq": 1, "result": {"seq": 1}, "completed_at": 30.0},
        2: {"seq": 2, "result": {"seq": 2}, "completed_at": 20.0},
        99: {"seq": 99, "result": None, "completed_at": None},
    }
    state["now"] = 40.0

    entry = cache_result(state, 3, {"seq": 3, "result": "third"}, now=40.0)

    assert entry == state["cache"][3]
    assert set(state["cache"]) == {1, 3, 99}
    assert state["cache"][1]["result"] == {"seq": 1}
    assert state["cache"][3]["result"] == {"seq": 3, "result": "third"}
    assert state["cache"][99]["completed_at"] is None
    assert state["commands"][2]["status"] == "completed"
    assert state["commands"][2]["completed_at"] == 40.0

    kept = cache_result(state, 1, {"seq": 1, "result": "replacement"}, now=50.0)
    assert kept == {"seq": 1, "result": {"seq": 1}, "completed_at": 30.0}
    assert state["cache"][1] == kept
    assert validate_state(state) == []


def test_validate_state_reports_duplicate_cache_overflow_and_stale_pending():
    # Defect 9: validation must inspect the cache bound as well as duplicate
    # sequences and timed-out unfinished commands.
    state = make_state(now=6.0, cache_limit=1)
    state["commands"] = [
        make_record(1, deadline=5.0),
        make_record(1, deadline=30.0),
    ]
    state["cache"] = {
        1: {"seq": 1, "result": {"seq": 1}, "completed_at": 1.0},
        2: {"seq": 2, "result": {"seq": 2}, "completed_at": 2.0},
    }

    assert validate_state(state) == [
        "duplicate_seq",
        "cache_over_limit",
        "stale_pending",
    ]

    boundary = make_state(now=5.0)
    boundary["commands"] = [make_record(1, deadline=5.0)]
    boundary["cache"] = {
        1: {"seq": 1, "result": {"seq": 1}, "completed_at": 1.0},
    }
    assert validate_state(boundary) == []
    assert validate_state(make_state()) == []


def test_batch_keeps_queue_state_independent():
    # Defect 10: the second queue starts at sequence 1 and does not inherit the
    # first queue's counter, results or cache.
    config = {
        "seq_limit": 20,
        "default_timeout": 5.0,
        "cache_limit": 2,
    }
    requests = [
        {"queue": "alpha", "command": make_command("a1")},
        {"queue": "beta", "command": make_command("b1")},
        {"queue": "alpha", "command": make_command("a2")},
    ]

    batch = queue_batch(requests, config, now=10.0)

    assert set(batch["queues"]) == {"alpha", "beta"}
    alpha = batch["queues"]["alpha"]
    beta = batch["queues"]["beta"]

    assert [result["seq"] for result in alpha["results"]] == [1, 2]
    assert [result["payload"] for result in alpha["results"]] == ["a1", "a2"]
    assert [result["seq"] for result in beta["results"]] == [1]
    assert [result["payload"] for result in beta["results"]] == ["b1"]

    assert alpha["state"]["next_seq"] == 3
    assert beta["state"]["next_seq"] == 2
    assert len(alpha["state"]["cache"]) == 2
    assert len(beta["state"]["cache"]) == 1
    assert alpha["state"] is not beta["state"]
    assert [entry["seq"] for entry in beta["state"]["commands"]] == [1]
    assert validate_state(alpha["state"]) == []
    assert validate_state(beta["state"]) == []