"""Lost-response retry of a mutating MCP `tools/call` (modelcontextprotocol #3394).

Scenarios (in-memory transport, mcp Python SDK):
  A  no ledger: tool commits an effect on every execution
  B  ledger:    tool checks a unique logical key (Postgres) before acting

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


def make_server(mode: str, run: str, crash_before_ledger: bool = False) -> MCPServer:
    server = MCPServer("lab")

    @server.tool()
    def charge(amount: int, key: str = "") -> str:
        """Non-idempotent: records one effect per execution."""
        if mode == "A":
            with db() as c:
                c.execute("insert into effects(run, logical_key) values (%s, %s)", (run, key))
            return json.dumps({"status": "charged", "amount": amount})

        # mode B: ledger keyed by the logical operation key
        h = hashlib.sha256(json.dumps({"amount": amount}).encode()).hexdigest()
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
            out = json.dumps({"status": "charged", "amount": amount})
            c.execute("update ops set result=%s where logical_key=%s", (json.dumps(out), key))
            return out

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


async def one_run(mode: str, timeout: float) -> tuple[int, int, int]:
    run = uuid.uuid4().hex
    key = f"op-{run}"  # logical operation key, chosen once by the caller
    tr = DropFirstResponse(InMemoryTransport(make_server(mode, run)))
    attempts = 0
    async with Client(tr) as client:
        for _ in range(2):  # first try + one retry
            attempts += 1
            try:
                await client.call_tool("charge", {"amount": 100, "key": key},
                                       read_timeout_seconds=timeout)
                break
            except Exception as e:  # timeout of the lost response
                last = e
    return count_effects(run), attempts, tr.dropped


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=["A", "B"], required=True)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=1.0)
    a = ap.parse_args()
    reset()
    effects, att, drp = [], [], []
    for _ in range(a.runs):
        n, attempts, dropped = await one_run(a.scenario, a.timeout)
        effects.append(n); att.append(attempts); drp.append(dropped)
    dist = {k: effects.count(k) for k in sorted(set(effects))}
    print(f"scenario={a.scenario} runs={a.runs} effects_per_logical_op={dist}")
    print(f"attempts_per_run={sorted(set(att))} responses_dropped_per_run={sorted(set(drp))}")
    expected = 2 if a.scenario == "A" else 1
    ok = all(n == expected for n in effects)
    print(f"expected={expected} -> {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
