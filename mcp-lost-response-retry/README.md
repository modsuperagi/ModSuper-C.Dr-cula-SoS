# mcp-lost-response-retry

Reproduction for [modelcontextprotocol/modelcontextprotocol#3394](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394):
when the response to a mutating `tools/call` is lost and the client retries, the request gets a fresh JSON-RPC id and the effect can run twice.

## What this does
- Server tool `charge` is non-idempotent: it records one row per execution.
- Metric: effects committed per logical operation (want 1).
- Two different kinds of fault are used, and they are not the same thing:
  - **Lost response** (A, B): the response to the first `tools/call` is dropped after the handler returned (effect already committed). The client times out and retries once; the SDK assigns a new request id. This is the case the issue describes.
  - **Failure point inside the handler** (D1, D2, F0, F, FC, G): the handler raises at a chosen point on its first execution, so the client gets a tool error and retries. This is used to place a failure between two steps. It is not a lost response.

| Scenario | Description | Result (20 runs each) |
|---|---|---|
| A | no ledger; response to first call dropped, client retries once | 2 effects in 20/20 |
| B | Postgres ledger, unique logical key set by the caller and passed as a tool argument | 1 effect in 20/20 |
| C | B with 3 concurrent calls, same key, no fault | 1 effect in 20/20 (1 call succeeds, 2 get "operation in progress") |
| D1 | ledger written AFTER the effect; handler fails between effect and ledger write | 2 effects in 20/20 (the ledger does not help) |
| D2 | key reserved BEFORE the effect; handler fails between effect and result write | 1 effect in 20/20, but the retry never gets a result (stuck "in progress") |
| E | same key, different arguments | 1 effect in 20/20; the second call is rejected |
| F0 | two-phase handler (reserve key, call downstream, record), downstream called WITHOUT a derived key; failure after the downstream call | 2 effects in 20/20 |
| F | same, but a key derived from the caller's key (`<key>:charge`) is passed to the downstream system, which deduplicates | 1 effect in 20/20; the retry completes and gets the result |
| FC | F with 3 concurrent calls on the same key, then one more call | 1 effect in 20/20; the last call gets the result |
| G | F with the downstream unreachable on the first retry | 1 effect in 20/20; call sequence is always error, then a distinct `unknown` result (not an error, no hang), then `charged` after a reconcile retry |

F and G follow the pattern suggested in the upstream issue discussion (atomic phases with recovery points; derive a key for each external call; distinct outcome for "key reserved, result unknown"). They are my implementation of that idea on a toy "downstream" table, not a general proof. G's `unknown` result is a convention of this toy tool, not something the protocol defines. Simplifications to keep in mind: the "downstream" is a table in the same Postgres database, so its deduplication is a unique constraint and never fails or lags; "unreachable" in G is a flag the test flips, not a network failure; and in F a retry that finds the key in phase `started` simply calls the downstream again, which is safe only because the downstream deduplicates (FC checks that with concurrent calls).

D1 and D2 show the limit: when the failure falls between the effect and the record of it, a ledger inside the server cannot tell whether the effect happened. Resolving that needs the external system to take part (a lookup by key, or its own idempotency key).

### HTTP, spec revision 2026-07-28 (`lab_http.py`)
Streamable HTTP, stateless (no `initialize`, no `Mcp-Session-Id`); the client reports protocol `2026-07-28`. The loss is injected at the HTTP layer: an ASGI middleware lets the real server handle the first `tools/call` per logical key (effect committed), discards the response and holds the connection until the client times out. The client retries once and the SDK sends a new JSON-RPC id.

| Scenario | Result (20 runs each) |
|---|---|
| A (no ledger) | 2 effects in 20/20 |
| B (ledger, caller-chosen key) | 1 effect in 20/20 |

So the duplicate also happens on the stateless 2026-07-28 wire. The [2026-07-28 changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog) (major change 9) says: "A broken response stream loses the in-flight request; clients MUST re-issue it as a new request with a new request ID". I checked the requests on the wire for one run: two `tools/call` POSTs with ids 1 and 2, `MCP-Protocol-Version: 2026-07-28`, no `initialize`, no `Mcp-Session-Id` in either direction. Only A and B were run over HTTP.

One thing this test does not cover: on this transport, closing the response stream is the cancellation signal and the server "SHOULD stop work". Here the handler has already finished when the client gives up, so there is nothing left to cancel. A handler that is still running when the client disconnects is a different case and was not tested.

### Protocol versions
With the SDK's default client mode the in-memory runs negotiate `2026-07-28`. A and B were also run with `--client-mode legacy` (initialize handshake, negotiated `2025-11-25`, the version in the issue): same results, 2 effects and 1 effect in 20/20.

Raw output: `result_<scenario>.txt`, `result_legacy_<A|B>.txt`, `result_http_<A|B>.txt`.

## Run
```
pip install "mcp==2.2.0" "psycopg[binary]"
# needs a Postgres database; set LAB_DSN (default: "dbname=lab")
for s in A B C D1 D2 E F0 F FC G; do python lab.py --scenario $s --runs 20; done
for s in A B; do python lab.py --scenario $s --runs 20 --client-mode legacy; done   # 2025-11-25
for s in A B; do python lab_http.py --scenario $s --runs 20; done   # HTTP, 2026-07-28
```

## Limits (read before citing)
- `mcp` Python SDK 2.2.0 only. Scenarios A-G run on the in-memory transport; A and B also over HTTP on 2026-07-28. The message loss is injected by my own code (transport wrapper / ASGI middleware), not by a real network failure. Other SDKs and servers were not tested.
- B works because the caller chooses the key and the tool checks it. It needs the tool to expose a key. It does not make a tool idempotent when the key is missing.
- D1, D2, F0, F, FC and G place the failure by raising an exception in my own toy server. That is not a real process crash and not a lost response; a real crash can also leave a transaction half-open, which this does not exercise.
- A reserved key whose result is never written stays reserved forever here (D2). There is no expiry or lease, so this is not a complete design.
- Single machine, 20 runs per scenario. A 'PASS' means the result matched what I expected, including for A, D1 and F0 where the expected result is a duplicate.

## Disclosure
Written with AI assistance (Claude). Results come from the commands above; please re-run them rather than trust this text.
