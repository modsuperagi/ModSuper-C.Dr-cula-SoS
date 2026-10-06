# mcp-lost-response-retry

Reproduction for [modelcontextprotocol/modelcontextprotocol#3394](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394):
when the response to a mutating `tools/call` is lost and the client retries, the request gets a fresh JSON-RPC id and the effect can run twice.

## What this does
- Server tool `charge` is non-idempotent: it records one row per execution.
- Fault: the response to the first `tools/call` is dropped after the handler returned (effect already committed).
- Client times out and retries once (SDK assigns a new request id).
- Metric: effects committed per logical operation (want 1).

| Scenario | Description | Result (20 runs) |
|---|---|---|
| A | no ledger | 2 effects in 20/20 |
| B | Postgres ledger with unique logical key, set by the caller and passed as a tool argument | 1 effect in 20/20 |

Raw output: `result_A.txt`, `result_B.txt`.

## Run
```
pip install "mcp==2.2.0" "psycopg[binary]"
# needs a Postgres database; set LAB_DSN (default: "dbname=lab")
python lab.py --scenario A --runs 20
python lab.py --scenario B --runs 20
```

## Limits (read before citing)
- In-memory transport only, `mcp` Python SDK 2.2.0. Not yet run over HTTP or against spec revision 2026-07-28.
- B works because the caller chooses the key and the tool checks it. It needs the tool to expose a key. It does not make a tool idempotent when the key is missing.
- Not covered yet: concurrent retries, a crash between the effect and the ledger write (the ledger cannot fix that without the external system's cooperation), and the same key with different arguments (the code rejects it, but it is not tested yet).
- Single machine, single run of 20 per scenario.

## Disclosure
Written with AI assistance (Claude). Results come from the commands above; please re-run them rather than trust this text.
