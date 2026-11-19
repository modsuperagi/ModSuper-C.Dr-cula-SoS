"""Lost-response retry of a mutating MCP `tools/call` (modelcontextprotocol #3394).

Scenarios (in-memory transport, mcp Python SDK):
  A  no ledger: tool commits an effect on every execution
  B  ledger:    tool checks a unique logical key (Postgres) before acting
  C  B with 3 concurrent calls carrying the same key (no fault)
  D1 ledger written AFTER the effect; handler fails between effect and ledger write
  D2 key reserved BEFORE the effect; handler fails between effect and result write
  E  same key reused with different arguments
  F0 two-phase handler, downstream called WITHOUT a derived key; failure after the downstream call
  F  same, but a key derived from the caller's key is passed to the downstream system
  FC F with 3 concurrent calls on the same key, then one more call
  G  F with the downstream unreachable on the first retry: the server answers a distinct
     'unknown outcome' result instead of an error or a hang; a later retry reconciles
  R  recovery: ledger stores the result on first execution; after a lost response, a
     retry with the same caller-chosen key (fresh JSON-RPC id) returns that stored result
  N  nonce-only: ledger records that the effect happened and consumes the key, but does
     not store the result; after a lost response, the retry is rejected and the client
     has no path to the original outcome

Faults differ per scenario:
  A, B, R, N  the response to the FIRST tools/call is dropped on its way back to the client,
              after the server handler has returned (effect committed). The client times out
              and retries ONCE; the SDK assigns a fresh JSON-RPC id.
  C           no fault, only concurrency.
  D*, F*, G   the handler raises at a chosen point on its first execution (a tool error reaches
              the client, which retries). This simulates a failure point; it is not a lost response.
  E           no fault, a second call reuses the key with different arguments.

Metric: effects committed per logical operation (expected 1), plus the client-visible
outcome of the retry (R: stored original result recovered; N: retry rejected,
original outcome unresolved).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
import uuid

import anyio
import psycopg
from mcp.client import Client
from mcp.client._memory import InMemoryTransport
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import JSONRPCRequest, JSONRPCResponse, JSONRPCError

DSN = os.environ.get("LAB_DSN", "dbname=lab")

# The payload the tool returns on a successful charge. R asserts the retry
# returns exactly this stored result; N asserts the retry does not disclose it.
STORED_RESULT = {"status": "charged", "amount": 100}

SCHEMA = """
create table if not exists effects(
  id bigserial primary key, run text not null, logical_key text, at timestamptz default now());
create table if not exists ops(
  logical_key text primary key, args_hash text not null, result jsonb);
alter table ops add column if not exists phase text;
create table if not exists downstream(idem_key text primary key);
"""


def db() -> psycopg.Connection:
    return psycopg.connect(DSN, autocommit=True)


def reset() -> None:
    with db() as c:
        c.execute(SCHEMA)
        c.execute("truncate effects, ops, downstream restart identity")


def count_effects(run: str) -> int:
    with db() as c:
        return c.execute("select count(*) from effects where run=%s", (run,)).fetchone()[0]


def _payload_is_stored_result(text: str) -> bool:
    try:
        body = json.loads(text)
        if isinstance(body, str):
            body = json.loads(body)
        return body == STORED_RESULT
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


def _charge_sync(mode: str, run: str, amount: int, key: str, state: dict) -> str:
    first = not state.get("crashed")
    out = json.dumps({"status": "charged", "amount": amount})
    h = hashlib.sha256(json.dumps({"amount": amount}).encode()).hexdigest()
    if mode == "A":
        with db() as c:
            c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
        return out
    if mode == "N":
        # Consume the caller-chosen key and record the effect, but do not store the
        # result payload. A later retry with the same key is rejected and cannot
        # recover the original outcome.
        with db() as c:
            row = c.execute(
                "insert into ops(logical_key, args_hash, phase) values (%s, %s, 'consumed') "
                "on conflict do nothing returning logical_key", (key, h)).fetchone()
            if row is None:
                raise RuntimeError("nonce already used; retry rejected")
            c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
            return out
    if mode == "D1":  # check ledger, act, THEN record
        with db() as c:
            prev = c.execute("select result from ops where logical_key=%s", (key,)).fetchone()
            if prev:
                return prev[0]
            c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
            if first:
                state["crashed"] = True
                raise RuntimeError("simulated failure between effect and ledger write")
            c.execute("insert into ops(logical_key, args_hash, result) values (%s,%s,%s)",
                      (key, h, json.dumps(out)))
            return out
    if mode in ("F0", "F", "FC", "G"):  # two phases with a recovery point; downstream is a separate system
        with db() as c:
            row = c.execute(
                "insert into ops(logical_key, args_hash, phase) values (%s, %s, 'started') "
                "on conflict do nothing returning logical_key", (key, h)).fetchone()
            if row is None:
                prev = c.execute("select args_hash, phase, result from ops where logical_key=%s", (key,)).fetchone()
                if prev[0] != h:
                    raise ValueError("idempotency key reused with different arguments")
                if prev[1] == "done":
                    return prev[2]
                if mode == "G" and state.get("down"):  # recovery point reached, downstream unreachable
                    return json.dumps({"status": "unknown", "detail": "key reserved, result unknown; retry later or reconcile"})
            dk = f"{key}:charge" if mode != "F0" else uuid.uuid4().hex  # F0: no stable downstream key
            new = c.execute("insert into downstream(idem_key) values (%s) on conflict do nothing returning 1", (dk,)).fetchone()
            if new:
                c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
            if first:
                state["crashed"] = True
                if mode == "G":
                    state["down"] = True
                raise RuntimeError("simulated failure after the downstream call, before recording it")
            c.execute("update ops set phase='done', result=%s where logical_key=%s", (json.dumps(out), key))
            return out
    # B, C, D2, E, R: reserve key, act, record result (R is the lost-response
    # retry that then recovers this stored result).
    with db() as c:
        row = c.execute(
            "insert into ops(logical_key, args_hash) values (%s, %s) "
            "on conflict do nothing returning logical_key", (key, h)).fetchone()
        if row is None:  # key seen before
            prev = c.execute("select args_hash, result from ops where logical_key=%s", (key,)).fetchone()
            if prev[0] != h:
                raise ValueError("idempotency key reused with different arguments")
            if prev[1] is None:
                raise RuntimeError("operation in progress")
            return prev[1]
        c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
        if mode == "C":
            time.sleep(0.05)  # widen the race window
        if mode == "D2" and first:
            state["crashed"] = True
            raise RuntimeError("simulated failure between effect and result write")
        c.execute("update ops set result=%s where logical_key=%s", (json.dumps(out), key))
        return out


def make_server(mode: str, run: str) -> MCPServer:
    server = MCPServer("lab")
    state: dict = {}

    @server.tool()
    async def charge(amount: int, key: str = "") -> str:
        """Non-idempotent: records one effect per execution."""
        try:
            return await asyncio.to_thread(_charge_sync, mode, run, amount, key, state)
        except RuntimeError as e:
            # N: a consumed nonce is a tool-level rejection, not an unexpected crash.
            if str(e).startswith("nonce already used"):
                raise ToolError(str(e)) from e
            raise

    return server, state


class DropFirstResponse:
    """Transport wrapper: drops the first response to a tools/call.

    The drop happens after the server handler has returned (effect already
    committed). It is an after-commit lost response, not a cancellation of
    in-flight work. Request ids of each tools/call are recorded in order so
    a retry can be checked for a fresh JSON-RPC id.
    """

    def __init__(self, inner):
        self.inner, self.calls, self.dropped, self.call_ids = inner, set(), 0, []

    async def __aenter__(self):
        r, w = await self.inner.__aenter__()
        outer = self

        class R:
            async def receive(s):
                while True:
                    m = await r.receive()
                    msg = getattr(m, "message", None)
                    if (isinstance(msg, (JSONRPCResponse, JSONRPCError))
                            and msg.id in outer.calls and outer.dropped == 0):
                        outer.dropped += 1
                        continue  # response lost, after the handler returned
                    return m
            def __aiter__(s): return s
            async def __anext__(s):
                try:
                    return await s.receive()
                except (anyio.EndOfStream, anyio.ClosedResourceError):
                    raise StopAsyncIteration
            async def aclose(s): await r.aclose()
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return None

        class W:
            async def send(s, item, /):
                msg = getattr(item, "message", None)
                if isinstance(msg, JSONRPCRequest) and msg.method == "tools/call":
                    outer.calls.add(msg.id)
                    outer.call_ids.append(msg.id)
                await w.send(item)
            async def aclose(s): await w.aclose()
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return None

        return R(), W()

    async def __aexit__(self, *a):
        return await self.inner.__aexit__(*a)


async def call(client, args, timeout):
    """One attempt. Returns (ok, text). A lost response or a tool error is a failure."""
    try:
        r = await client.call_tool("charge", args, read_timeout_seconds=timeout)
    except Exception as e:
        return False, type(e).__name__
    text = "".join(getattr(c, "text", "") for c in r.content)
    return (not r.is_error), text


def _client_outcome(mode: str, outcomes: list, texts: list) -> str:
    """Client-visible outcome of the retry, independent of the effect count."""
    if mode == "R":
        if outcomes and outcomes[-1] and texts and _payload_is_stored_result(texts[-1]):
            return "stored original result recovered"
        return "recovery failed"
    if mode == "N":
        disclosed = any(_payload_is_stored_result(t) or '"status": "charged"' in (t or "")
                        for t in texts)
        retry_rejected = len(outcomes) >= 2 and not outcomes[-1]
        if retry_rejected and not disclosed and not any(outcomes):
            return "retry rejected; original outcome unresolved"
        return "unexpected outcome"
    return ""


async def one_run(mode: str, timeout: float, client_mode: str) -> dict:
    run = uuid.uuid4().hex
    key = f"op-{run}"  # logical operation key, chosen once by the caller
    srv, state = make_server("B" if mode == "E" else mode, run)
    seq = None
    fault = mode in ("A", "B", "R", "N")
    tr = DropFirstResponse(InMemoryTransport(srv))
    if not fault:
        tr.dropped = 1  # disable the drop
    attempts, outcomes, texts = 0, [], []
    async with Client(tr, mode=client_mode) as client:
        proto = client.protocol_version
        if mode == "C":
            res = await asyncio.gather(*[call(client, {"amount": 100, "key": key}, 10) for _ in range(3)])
            outcomes = [ok for ok, _ in res]; texts = [t for _, t in res]; attempts = 3
        elif mode == "FC":
            res = await asyncio.gather(*[call(client, {"amount": 100, "key": key}, 10) for _ in range(3)])
            last = await call(client, {"amount": 100, "key": key}, 10)
            outcomes = [ok for ok, _ in res] + [last[0]]
            texts = [t for _, t in res] + [last[1]]
            attempts = 4
        elif mode == "E":
            a1 = await call(client, {"amount": 100, "key": key}, 10)
            a2 = await call(client, {"amount": 200, "key": key}, 10)
            outcomes, texts, attempts = [a1[0], a2[0]], [a1[1], a2[1]], 2
        elif mode == "G":
            gtexts = []
            for i in range(3):
                ok, t = await call(client, {"amount": 100, "key": key}, 10)
                gtexts.append("error" if not ok else json.loads(t)["status"])
                texts.append(t)
                if i == 1:
                    state["down"] = False  # downstream recovers before the reconcile attempt
            outcomes = [t == "charged" for t in gtexts]; attempts = 3
            seq = gtexts
        else:  # A, B, R, N, D1, D2, F0, F: first try + one retry with the SAME key
            for _ in range(2):
                attempts += 1
                ok, t = await call(client, {"amount": 100, "key": key}, timeout)
                outcomes.append(ok)
                texts.append(t)
                if ok:
                    break
    return dict(proto=proto, seq=seq, effects=count_effects(run), attempts=attempts,
                dropped=tr.dropped if fault else 0, final_ok=outcomes[-1],
                n_ok=sum(outcomes), call_ids=list(tr.call_ids), texts=texts,
                client_outcome=_client_outcome(mode, outcomes, texts))


# scenario -> expected effects per logical operation. PASS means "matched this number", which for
# A, D1 and F0 is a demonstrated duplicate, not a success of the tool.
EXPECT = {"A": 2, "B": 1, "C": 1, "D1": 2, "D2": 1, "E": 1, "F0": 2, "F": 1, "FC": 1, "G": 1,
          "R": 1, "N": 1}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=list(EXPECT), required=True)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=1.0)
    ap.add_argument("--client-mode", default="auto",
                    help="'auto' (default), 'legacy' (initialize handshake, e.g. 2025-11-25) or a version pin")
    a = ap.parse_args()
    reset()
    rs = [await one_run(a.scenario, a.timeout, a.client_mode) for _ in range(a.runs)]
    print(f"client_mode={a.client_mode} negotiated_protocol={sorted({r['proto'] for r in rs})}")
    effects = [r["effects"] for r in rs]
    dist = {k: effects.count(k) for k in sorted(set(effects))}
    print(f"scenario={a.scenario} runs={a.runs} effects_per_logical_op={dist}")
    print(f"attempts_per_run={sorted({r['attempts'] for r in rs})} "
          f"responses_dropped_per_run={sorted({r['dropped'] for r in rs})} "
          f"final_call_ok={sorted({r['final_ok'] for r in rs})} "
          f"successful_calls_per_run={sorted({r['n_ok'] for r in rs})}")
    expected = EXPECT[a.scenario]
    ok = all(n == expected for n in effects)
    if a.scenario == "E":  # second call (different args) must be rejected
        ok = ok and all(r["n_ok"] == 1 and not r["final_ok"] for r in rs)
    if a.scenario == "G":
        print(f"call_sequence={sorted({tuple(r['seq']) for r in rs})}")
    if a.scenario in ("F", "FC"):
        ok = ok and all(r["final_ok"] for r in rs)
    if a.scenario == "G":
        ok = ok and all(r["seq"] == ["error", "unknown", "charged"] for r in rs)
    if a.scenario in ("R", "N"):
        # After-commit drop + fresh JSON-RPC id on the retry, plus the
        # client-visible outcome (not only the effect count).
        id_pairs = [tuple(r["call_ids"]) for r in rs]
        fresh_id = all(len(p) == 2 and p[0] != p[1] for p in id_pairs)
        after_commit_drop = all(r["dropped"] == 1 for r in rs)
        print(f"after_commit_response_drop={after_commit_drop} "
              f"fresh_request_id_on_retry={fresh_id} "
              f"request_ids_example={id_pairs[0] if id_pairs else ()}")
        print(f"client_visible_outcome={sorted({r['client_outcome'] for r in rs})}")
        if a.scenario == "R":
            ok = (ok and after_commit_drop and fresh_id
                  and all(r["client_outcome"] == "stored original result recovered"
                          and r["final_ok"] and r["n_ok"] == 1 for r in rs))
        if a.scenario == "N":
            # Do not treat "final call fails" as the result: a rejected retry
            # does not establish that the original operation failed.
            ok = (ok and after_commit_drop and fresh_id
                  and all(r["client_outcome"] == "retry rejected; original outcome unresolved"
                          and r["n_ok"] == 0 for r in rs))
    print(f"expected_effects={expected} -> {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
