# Changelog

## 0.1.0

The first release.

- One method per API operation, generated from the contract.
- Retries with jittered back-off that honour `Retry-After` and
  `RateLimit-Reset`; idempotency keys on the writes the API keys; per-attempt
  timeouts; context cancellation; an optional client-side rate limit.
- Typed errors: `APIError`, `Refusal`, `LocalTimeError`, `NetworkError`, with
  keys redacted.
- `Mask` for slot sets of any width; `LocalDay`/`LocalHour` for positions in a
  time zone.
- `Watch`: a World's live events, with reconnection and journal catch-up.
- `Assist`: the Director's event stream.
