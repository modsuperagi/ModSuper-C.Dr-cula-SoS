# State
- 2026-10-06: in-memory A, B, C, D1, D2, E, F0, F, FC, G (20 runs each, negotiated 2026-07-28), A and B in legacy mode (2025-11-25), HTTP 2026-07-28 A and B. All match expectations.
- 2026-10-06: reviewed. Fixed: README now separates "lost response" from "failure point inside the handler", states negotiated versions, quotes the changelog sentence, lists simplifications of F/G.
- Upstream issue #3394 got a reply: key propagation and an "unknown outcome" result will be covered in the SEP.
- 2026-11-18: added scenarios R (recovery) and N (nonce-only) on the same lost-response + fresh-id retry shape as A and B. Each run asserts effect count and client-visible outcome (R: stored original result recovered, 1 effect; N: retry rejected, original outcome unresolved, 1 effect). Also records request ids and the after-commit response drop. In-memory, mcp 2.2.0, negotiated 2026-07-28. Three independent batches of 20 runs each (60 per scenario); all PASS. Not run over HTTP. See result_R.txt, result_N.txt, and the rerun files.
- Open: handler still running when the client disconnects (cancellation); real process crash; other SDKs.
