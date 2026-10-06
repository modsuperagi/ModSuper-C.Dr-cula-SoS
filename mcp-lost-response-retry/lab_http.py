"""HTTP variant of lab.py: streamable HTTP, protocol revision 2026-07-28 (no handshake, no session id).

The fault is injected at the HTTP layer, in an ASGI middleware in front of the MCP app: the first
`tools/call` for each logical key is executed by the real server (effect committed), its response is
discarded, and the connection is held open until the client gives up (read timeout). The client then
retries once; the SDK sends a new JSON-RPC id. Scenarios A (no ledger) and B (ledger) as in lab.py.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import socket
import sys
import uuid

import uvicorn
from mcp.client import Client
from mcp.server.mcpserver import MCPServer

from lab import _charge_sync, count_effects, reset


def build_app(mode: str):
    server = MCPServer("lab-http")

    @server.tool()
    async def charge(amount: int, key: str = "") -> str:
        """Non-idempotent: records one effect per execution (effects.run = key)."""
        return await asyncio.to_thread(_charge_sync, mode, key, amount, key, {})

    app = server.streamable_http_app(json_response=True, stateless_http=True)
    seen: set[str] = set()
    stats = {"dropped": 0}

    async def fault(scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            return await app(scope, receive, send)
        body = b""
        while True:
            m = await receive()
            body += m.get("body", b"")
            if not m.get("more_body"):
                break
        key = None
        try:
            j = json.loads(body)
            if j.get("method") == "tools/call":
                key = (j.get("params", {}).get("arguments") or {}).get("key")
        except Exception:
            pass
        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        if key and key not in seen:
            seen.add(key)

            async def discard(msg):  # response lost on the way back
                return None

            await app(scope, replay, discard)  # handler runs, effect committed
            stats["dropped"] += 1
            while (await receive())["type"] != "http.disconnect":  # hold until client gives up
                pass
            return
        await app(scope, _Replay(body, receive), send)

    return fault, app, stats


class _Replay:
    """Callable that re-serves a consumed request body, then defers to the real receive."""
    def __init__(self, body, receive):
        self.body, self.receive, self.sent = body, receive, False

    async def __call__(self):
        if not self.sent:
            self.sent = True
            return {"type": "http.request", "body": self.body, "more_body": False}
        return await self.receive()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=["A", "B"], required=True)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=1.0)
    a = ap.parse_args()
    reset()
    fault, app, stats = build_app(a.scenario)
    port = free_port()
    # the MCP app needs its lifespan: forward lifespan events to it, everything else through the fault layer
    async def lifespan_app(scope, receive, send):
        if scope["type"] == "lifespan":
            return await app(scope, receive, send)
        return await fault(scope, receive, send)
    cfg = uvicorn.Config(lifespan_app, host="127.0.0.1", port=port, log_level="error", lifespan="on")
    srv = uvicorn.Server(cfg)
    task = asyncio.create_task(srv.serve())
    while not srv.started:
        await asyncio.sleep(0.05)

    effects, attempts_l = [], []
    try:
        async with Client(f"http://127.0.0.1:{port}/mcp", mode="2026-07-28") as client:
            print("negotiated protocol:", client.protocol_version)
            for _ in range(a.runs):
                key = f"op-{uuid.uuid4().hex}"
                attempts = 0
                for _try in range(2):
                    attempts += 1
                    try:
                        r = await client.call_tool("charge", {"amount": 100, "key": key},
                                                   read_timeout_seconds=a.timeout)
                        if not r.is_error:
                            break
                    except Exception:
                        pass
                effects.append(count_effects(key)); attempts_l.append(attempts)
    finally:
        srv.should_exit = True
        await task
    dist = {k: effects.count(k) for k in sorted(set(effects))}
    print(f"http scenario={a.scenario} runs={a.runs} effects_per_logical_op={dist}")
    print(f"attempts_per_run={sorted(set(attempts_l))} responses_dropped_total={stats['dropped']}")
    expected = 2 if a.scenario == "A" else 1
    ok = all(n == expected for n in effects)
    print(f"expected_effects={expected} -> {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
