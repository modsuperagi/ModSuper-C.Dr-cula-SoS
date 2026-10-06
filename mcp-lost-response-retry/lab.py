"""Lost-response retry of a mutating MCP `tools/call` (modelcontextprotocol #3394).

Scenarios (in-memory transport, mcp Python SDK):
  A  no ledger: tool commits an effect on every execution
  B  ledger:    tool checks a unique logical key (Postgres) before acting
  C  B with 3 concurrent calls carrying the same key (no fault)
  D1 ledger written AFTER the effect; handler fails between effect and ledger write
  D2 key reserved BEFORE the effect; handler fails between effect and result write
  E  same key reused with different arguments

Fault injected: the response to the FIRST tools/call is dropped on its way back
to the client, after the server handler has already returned (effect committed).
The client times out and retries ONCE; the SDK assigns a fresh JSON-RPC id.

Metric: effects committed per logical operation (expected 1).
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
from mcp_types import JSONRPCRequest, JSONRPCResponse, JSONRPCError

DSN = os.environ.get("LAB_DSN", "dbname=lab")

SCHEMA = """
create table if not exists effects(
  id bigserial primary key, run text not null, logical_key text, at timestamptz default now());
create table if not exists ops(
  logical_key text primary key, args_hash text not null, result jsonb);
"""


def db() -> psycopg.Connection:
    return psycopg.connect(DSN, autocommit=True)


def reset() -> None:
    with db() as c:
        c.execute(SCHEMA)
        c.execute("truncate effects, ops restart identity")


def count_effects(run: str) -> int:
    with db() as c:
        return c.execute("select count(*) from effects where run=%s", (run,)).fetchone()[0]


def _charge_sync(mode: str, run: str, amount: int, key: str, state: dict) -> str:
    first = not state.get("crashed")
    out = json.dumps({"status": "charged", "amount": amount})
    h = hashlib.sha256(json.dumps({"amount": amount}).encode()).hexdigest()
    if mode == "A":
        with db() as c:
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
    # B, C, D2, E: reserve key, act, record result
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
        return await asyncio.to_thread(_charge_sync, mode, run, amount, key, state)

    return server


class DropFirstResponse:
    """Transport wrapper: drops the first response to a tools/call."""

    def __init__(self, inner):
        self.inner, self.calls, self.dropped = inner, set(), 0

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
                        continue  # response lost
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


async def one_run(mode: str, timeout: float) -> dict:
    run = uuid.uuid4().hex
    key = f"op-{run}"  # logical operation key, chosen once by the caller
    srv = make_server("B" if mode == "E" else mode, run)
    fault = mode in ("A", "B")
    tr = DropFirstResponse(InMemoryTransport(srv))
    if not fault:
        tr.dropped = 1  # disable the drop
    attempts, outcomes = 0, []
    async with Client(tr) as client:
        if mode == "C":
            res = await asyncio.gather(*[call(client, {"amount": 100, "key": key}, 10) for _ in range(3)])
            outcomes = [ok for ok, _ in res]; attempts = 3
        elif mode == "E":
            outcomes.append((await call(client, {"amount": 100, "key": key}, 10))[0])
            outcomes.append((await call(client, {"amount": 200, "key": key}, 10))[0])
            attempts = 2
        else:  # A, B, D1, D2: first try + one retry with the SAME key
            for _ in range(2):
                attempts += 1
                ok, _t = await call(client, {"amount": 100, "key": key}, timeout)
                outcomes.append(ok)
                if ok:
                    break
    return dict(effects=count_effects(run), attempts=attempts,
                dropped=tr.dropped if fault else 0, final_ok=outcomes[-1],
                n_ok=sum(outcomes))


EXPECT = {  # scenario -> (expected effects, description of what PASS means)
    "A": 2, "B": 1, "C": 1, "D1": 2, "D2": 1, "E": 1,
}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=list(EXPECT), required=True)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=1.0)
    a = ap.parse_args()
    reset()
    rs = [await one_run(a.scenario, a.timeout) for _ in range(a.runs)]
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
    print(f"expected_effects={expected} -> {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
