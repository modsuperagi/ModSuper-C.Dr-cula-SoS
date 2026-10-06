# State
- 2026-10-06: in-memory A, B, C, D1, D2, E, F0, F, FC, G (20 runs each, negotiated 2026-07-28), A and B in legacy mode (2025-11-25), HTTP 2026-07-28 A and B. All match expectations.
- 2026-10-06: reviewed. Fixed: README now separates "lost response" from "failure point inside the handler", states negotiated versions, quotes the changelog sentence, lists simplifications of F/G.
- Upstream issue #3394 got a reply: key propagation and an "unknown outcome" result will be covered in the SEP.
- Open: handler still running when the client disconnects (cancellation); real process crash; other SDKs.
