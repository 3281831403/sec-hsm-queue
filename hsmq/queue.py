"""Hardware security module command queue contracts.

The queue state is a plain mutable dictionary so callers can inspect it after a
reconnect, persist it as JSON and restore it without losing sequence numbers:

    {
        "now": 0.0,
        "next_seq": 1,
        "seq_limit": 65535,
        "default_timeout": 10.0,
        "cache_limit": 32,
        "commands": [],
        "cache": {},
        "journal": [],
    }

Field meanings:

    now              latest explicit time supplied to submit, execute or
                     cache_result
    next_seq         next sequence number to allocate; starts at 1 and is
                     stored in the state so a reconnect continues the sequence
    seq_limit        last usable sequence number, inclusive; allocation beyond
                     this value raises OverflowError instead of wrapping to zero
    default_timeout  fallback lifetime in seconds when a command has no ttl
    cache_limit      maximum number of cached completed results
    commands         command records in submission order
    cache            sequence number to completed-result cache entry
    journal          append-only trace of successful executions

A command record has this shape:

    {
        "seq": 1,
        "command": {"op": "sign", "payload": "digest", "ttl": 5.0},
        "submitted_at": 0.0,
        "deadline": 5.0,
        "status": "pending",       # pending, completed or expired
        "completed_at": None,
    }

A cache entry has this shape:

    {
        "seq": 1,
        "result": {"seq": 1, "op": "sign", "payload": "digest",
                   "executed_at": 1.0},
        "completed_at": 1.0,
    }

An entry whose completed_at is None is not complete and must never be evicted.
A successful execution appends {"seq": seq, "at": now} to the journal and
caches the result; the journal is the side-effect counter used by the tests.

All eight public functions are stubs in this exercise.  They mutate the state
in place except for timeout_check and validate_state, which only inspect their
arguments.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping


def _records_for(state: dict, seq: int) -> list[dict]:
    """Return every command record carrying seq, in submission order."""
    return [record for record in state["commands"] if record.get("seq") == seq]


def _single_record(state: dict, seq: int) -> dict:
    """Return the one record for seq or raise KeyError.

    A duplicated sequence cannot be resolved to a single command, so it is
    treated as an unknown sequence rather than silently picking one.
    """
    records = _records_for(state, seq)
    if len(records) != 1:
        raise KeyError(seq)
    return records[0]


def _cache_entry(state: dict, seq: int) -> dict | None:
    """Return a completed cache entry for seq, or None."""
    entry = state["cache"].get(seq)
    if entry is not None and entry.get("completed_at") is not None:
        return entry
    return None


def next_seq(state: dict) -> int:
    """Allocate and return the next command sequence number.

    The allocated value is removed from the available range by incrementing
    state["next_seq"], so a JSON round trip of the state preserves the counter.
    Numbers are strictly increasing and never reused.  When the next value is
    greater than state["seq_limit"], raise OverflowError and leave the state
    unchanged; wrapping back to zero is forbidden.
    """
    # Defect 1: the counter is read from, and written back to, the state, so a
    # reconnect never restarts at 1; exhausting the range refuses instead of
    # wrapping onto an already issued sequence.
    seq = state["next_seq"]
    if seq > state["seq_limit"]:
        raise OverflowError(
            f"sequence {seq} exceeds limit {state['seq_limit']}"
        )
    state["next_seq"] = seq + 1
    return seq


def submit(state: dict, command: dict, now: float) -> dict:
    """Submit command at now and return its new command record.

    The record receives a fresh sequence from next_seq, a deep copy of command,
    submitted_at equal to now and deadline equal to now plus the command ttl
    (or state["default_timeout"] when ttl is absent).  The status is pending,
    completed_at is None, and the record is appended to state["commands"] in
    submission order.  state["now"] becomes now.  Sequences must not repeat and
    each sequence must identify exactly one command.

    Raises:
        ValueError: if command is not a mapping or ttl is not positive.
    """
    if not isinstance(command, Mapping):
        raise ValueError("command must be a mapping")
    if "ttl" in command:
        ttl = command["ttl"]
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            raise ValueError("ttl must be a number")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
    else:
        ttl = state["default_timeout"]

    # Defect 2: every record takes its own fresh sequence from the persistent
    # counter, so two submissions can never alias the same command.
    seq = next_seq(state)
    record = {
        "seq": seq,
        "command": copy.deepcopy(dict(command)),
        "submitted_at": now,
        "deadline": now + ttl,
        "status": "pending",
        "completed_at": None,
    }
    state["commands"].append(record)
    state["now"] = now
    return record


def execute(state: dict, seq: int, now: float) -> dict:
    """Execute the command for seq at most once and return its cached result.

    Lookup order is deliberate.  A completed cache entry wins even when now is
    past the deadline: the exact original result is returned as an independent
    deep copy, the journal is not appended and completed_at is unchanged.  A
    record marked completed whose cache entry was evicted raises KeyError
    rather than running the command again.  Only a pending, unique record may
    execute.  If now is strictly greater than its deadline, the record becomes
    expired and TimeoutError is raised before any journal entry is written.

    A successful execution writes {"seq": seq, "at": now} to state["journal"],
    returns

        {"seq": seq, "op": ..., "payload": ..., "executed_at": now}

    marks the record completed, caches that result and updates state["now"].
    """
    # Defect 3: the cache is consulted before any deadline logic, so a retry of
    # an already executed sequence replays the original result instead of
    # signing a second time.
    entry = _cache_entry(state, seq)
    if entry is not None:
        return copy.deepcopy(entry["result"])

    record = _single_record(state, seq)
    if record.get("status") == "completed":
        # Completed but the cached result was evicted: never run it again.
        raise KeyError(seq)
    if record.get("status") == "expired":
        # A late retry stays rejected and leaves no side effect behind.
        state["now"] = now
        raise TimeoutError(f"sequence {seq} expired before retry")

    # Defect 5/6: expiration is strict, and the record is marked expired before
    # any journal entry is written, so no late side effect can occur.
    if timeout_check(record["command"], now, record["deadline"]):
        record["status"] = "expired"
        state["now"] = now
        raise TimeoutError(f"sequence {seq} exceeded its deadline")

    command = record["command"]
    result = {
        "seq": seq,
        "op": command.get("op"),
        "payload": command.get("payload"),
        "executed_at": now,
    }
    state["journal"].append({"seq": seq, "at": now})
    record["status"] = "completed"
    record["completed_at"] = now
    state["now"] = now
    cache_result(state, seq, result, now)
    return copy.deepcopy(result)


def timeout_check(command: dict, now: float, deadline: float) -> bool:
    """Return True only when now is strictly greater than deadline.

    A command exactly at its deadline is still legal.  command is accepted so
    callers can keep operation metadata beside the deadline, but it must not
    change this boundary rule.
    """
    # Defect 5: strict inequality; now == deadline is still legal.
    return now > deadline


def cache_result(state: dict, seq: int, result: dict, now: float) -> dict:
    """Store the first result for seq and return the cache entry.

    The first result for a sequence is authoritative: if a completed entry
    already exists it is returned unchanged and result is ignored.  Otherwise
    result is deep-copied into state["cache"], the command record is marked
    completed with completed_at equal to now, and state["now"] becomes now.
    When the cache exceeds state["cache_limit"], evict the entry with the
    smallest (completed_at, seq); entries whose completed_at is None are
    incomplete and are never eviction candidates.  Stop only when the cache is
    back within its limit or every remaining entry is incomplete.

    Raises:
        KeyError: if seq has no command record.
        ValueError: if the command record is already expired.
    """
    record = _single_record(state, seq)
    if record.get("status") == "expired":
        raise ValueError(f"sequence {seq} is expired")

    # First result wins: an existing completed entry is authoritative, so the
    # replayed result stays byte-for-byte reproducible.
    existing = _cache_entry(state, seq)
    if existing is not None:
        return existing

    entry = {
        "seq": seq,
        "result": copy.deepcopy(result),
        "completed_at": now,
    }
    state["cache"][seq] = entry
    record["status"] = "completed"
    record["completed_at"] = now
    state["now"] = now

    # Defect 8: eviction order is completion time, then seq, never insertion
    # order; incomplete entries (completed_at is None) are protected.
    # Defect 7: the loop enforces the configured bound, one entry at a time.
    limit = state["cache_limit"]
    while len(state["cache"]) > limit:
        candidates = [
            (cached["completed_at"], cached["seq"], key)
            for key, cached in state["cache"].items()
            if cached.get("completed_at") is not None
        ]
        if not candidates:
            # Only incomplete entries remain; nothing is evictable.
            break
        victim = min(candidates)[2]
        del state["cache"][victim]

    return entry


def replay(state: dict, seq: int, now: float) -> dict:
    """Return an independent copy of the original cached result for seq.

    Replay never executes anything, never changes timestamps and never appends
    to the journal.  A pending, expired or evicted command has no cached
    result, so replay raises KeyError; now is accepted only for interface
    symmetry.
    """
    # Defect 4: replay is read-only; it never mints a new executed_at and it
    # hands back an independent copy so callers cannot poison the cache.
    entry = _cache_entry(state, seq)
    if entry is None:
        raise KeyError(seq)
    return copy.deepcopy(entry["result"])


def validate_state(state: dict) -> list[str]:
    """Return structural problems in fixed order, or [] when the state is valid.

    The only problem codes, emitted in this exact order, are:

        duplicate_seq      more than one command record carries the same seq
        cache_over_limit   len(state["cache"]) exceeds state["cache_limit"]
        stale_pending      a pending record has deadline below state["now"]

    A deadline equal to state["now"] is not stale.  Validation never mutates
    the state and never raises for a malformed record.
    """
    problems: list[str] = []

    records = [
        record
        for record in state.get("commands") or ()
        if isinstance(record, dict)
    ]

    seen: set = set()
    duplicated = False
    for record in records:
        try:
            seq = record.get("seq")
            if seq in seen:
                duplicated = True
            else:
                seen.add(seq)
        except TypeError:
            # An unhashable seq cannot be checked for duplication.
            continue
    if duplicated:
        problems.append("duplicate_seq")

    # Defect 9: validation covers the cache bound too, not just the commands.
    if len(state.get("cache") or {}) > state["cache_limit"]:
        problems.append("cache_over_limit")

    now = state["now"]
    for record in records:
        if record.get("status") != "pending":
            continue
        try:
            stale = record["deadline"] < now
        except (KeyError, TypeError):
            continue
        if stale:
            problems.append("stale_pending")
            break

    return problems


def queue_batch(requests: list[dict], config: dict, now: float) -> dict:
    """Submit and execute independent batches and return one state per queue.

    Each request is {"queue": name, "command": {...}}.  config supplies
    seq_limit, default_timeout and cache_limit; the optional config["queues"]
    mapping may override those three fields for one queue.  Queues are
    discovered from the requests and every queue starts with next_seq equal to
    1, so a request in one queue can never consume another queue's sequence.
    Requests are processed in input order, all with the supplied now.

    Returns:

        {
            "queues": {
                "queue-name": {
                    "state": {...},
                    "results": [execution result, ...],
                },
            },
        }
    """
    # Defect 10: one freshly initialised state per queue, built up front, so
    # sequences, caches and journals can never leak between queues.
    queues: dict[str, dict] = {}
    overrides = (config or {}).get("queues") or {}
    for request in requests:
        name = request["queue"]
        if name in queues:
            continue
        settings = {
            "seq_limit": config["seq_limit"],
            "default_timeout": config["default_timeout"],
            "cache_limit": config["cache_limit"],
        }
        settings.update(overrides.get(name) or {})
        queues[name] = {
            "now": now,
            "next_seq": 1,
            "seq_limit": settings["seq_limit"],
            "default_timeout": settings["default_timeout"],
            "cache_limit": settings["cache_limit"],
            "commands": [],
            "cache": {},
            "journal": [],
        }

    results: dict[str, list] = {name: [] for name in queues}
    for request in requests:
        name = request["queue"]
        state = queues[name]
        record = submit(state, request["command"], now)
        results[name].append(execute(state, record["seq"], now))

    return {
        "queues": {
            name: {"state": state, "results": results[name]}
            for name, state in queues.items()
        }
    }