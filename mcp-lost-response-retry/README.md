# mcp-lost-response-retry

Reproduction for [modelcontextprotocol/modelcontextprotocol#3394](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394):
when the response to a mutating `tools/call` is lost and the client retries, the request gets a fresh JSON-RPC id and the effect can run twice.

## What this does
- Server tool `charge` is non-idempotent: it records one row per execution.
- Fault: the response to the first `tools/call` is dropped after the handler returned (effect already committed).
- Client times out and retries once (SDK assigns a new request id).
- Metric: effects committed per logical operation (want 1).

| Scenario | Description | Result (20 runs each) |
|---|---|---|
| A | no ledger; response to first call dropped, client retries once | 2 effects in 20/20 |
| B | Postgres ledger, unique logical key set by the caller and passed as a tool argument | 1 effect in 20/20 |
| C | B with 3 concurrent calls, same key, no fault | 1 effect in 20/20 (1 call succeeds, 2 get "operation in progress") |
| D1 | ledger written AFTER the effect; handler fails between effect and ledger write | 2 effects in 20/20 (the ledger does not help) |
| D2 | key reserved BEFORE the effect; handler fails between effect and result write | 1 effect in 20/20, but the retry never gets a result (stuck "in progress") |
| E | same key, different arguments | 1 effect in 20/20; the second call is rejected |

D1 and D2 show the limit: when the failure falls between the effect and the record of it, a ledger inside the server cannot tell whether the effect happened. Resolving that needs the external system to take part (a lookup by key, or its own idempotency key).

Raw output: `result_<scenario>.txt`.

## Run
```
pip install "mcp==2.2.0" "psycopg[binary]"
# needs a Postgres database; set LAB_DSN (default: "dbname=lab")
for s in A B C D1 D2 E; do python lab.py --scenario $s --runs 20; done
```

## Limits (read before citing)
- In-memory transport only, `mcp` Python SDK 2.2.0. Not yet run over HTTP or against spec revision 2026-07-28.
- B works because the caller chooses the key and the tool checks it. It needs the tool to expose a key. It does not make a tool idempotent when the key is missing.
- C, D1, D2 and E are simulations of failure points inside my own toy server (the failure is raised in code, not a real process crash).
- Single machine, 20 runs per scenario. A 'PASS' means the result matched what I expected, including for D1 and D2 where the expected result is a failure to prevent duplicates.

## Disclosure
Written with AI assistance (Claude). Results come from the commands above; please re-run them rather than trust this text.
