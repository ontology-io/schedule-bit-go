# schedule-bit-go

The Go client for the [schedule-bit](https://schedule-bit.io) API: book time
(or any other hierarchy of slots) through a door that judges every claim.

```sh
go get github.com/ontology-io/schedule-bit-go
```

```go
c, err := schedulebit.New() // the key from SCHEDULE_BIT_API_KEY
if err != nil { log.Fatal(err) }

day, _ := time.Parse("2006-01-02", "2026-11-16")
receipt, refusal, err := c.TryClaim(ctx, "clinic", api.ClaimRequest{
	Who:   schedulebit.Who("dr-hana"),
	Slots: schedulebit.SlotRange(9, 10).SlotsInput(),
	Date:  &openapi_types.Date{Time: day},
})
switch {
case err != nil:
	log.Fatal(err)
case refusal != nil:
	fmt.Println("refused:", refusal.Reason.By, refusal.Detail)
default:
	fmt.Println("held", schedulebit.MustMask(receipt.Wrote).Slots())
}
```

Every operation of the API is a method on `Client`, generated from the
contract (`c.ListWorldsWithResponse`, `c.PlanesWithResponse`, …), so the
client is never behind the API. A failed answer comes back as `err`.

## What the SDK does for you

- **Retries.** 429 and 503 are retried; so are network failures and other
  5xx for reads, `explainClaim` and writes that carry an idempotency key.
  Back-off is exponential with full jitter (base 500 ms, cap 30 s, 4 retries),
  and `Retry-After` or `RateLimit-Reset` win when the server sends them.
  `WithMaxRetries`, `WithBackoff`.
- **Idempotency.** Writes the API keys (claims, releases, Worlds,
  relays, …) get an `Idempotency-Key`, the same on every retry of one call,
  so a retried claim never claims twice. Bring your own with
  `schedulebit.WithIdempotencyKey(ctx, "order-42")`.
- **Rate limits.** `OnRateLimit(func(schedulebit.RateLimit))` sees the
  `RateLimit-*` headers of every answer; `WithRateLimit(perMinute)` spaces
  requests on the client side.
- **Timeouts and cancellation.** Each attempt is bounded (30 s,
  `WithTimeout`, or `schedulebit.WithCallTimeout(ctx, d)` for one call); the
  context cancels everything, waits included.
- **Typed errors.** `*APIError` (any failed answer), `*Refusal` (409: the
  engine's reason, `Reason.By` and its members), `*LocalTimeError` (422: a
  local time the zone repeats or skips, with candidates), `*NetworkError` (no
  answer after every retry). `errors.As` finds them; a Refusal and a
  LocalTimeError are APIErrors too.
- **Masks.** `schedulebit.Mask` is a set of slots of any width:
  `MustMask("0x600").Slots()` is `[9 10]`; `Union`, `Intersect`, `Without`.
- **Local time.** `schedulebit.LocalDay(t)` / `LocalHour(t)` name a position
  in t's own zone; `.Earlier()` / `.Later()` pick a repeated hour at a clock
  change; `LocalTimeError.Choose` reads a candidate back.
- **Watch.** `c.Watch(ctx, world, opts)` streams a World's events over a
  WebSocket, reconnects with the same back-off, reads the journal after a
  drop so no commit is missed, and never yields an event twice.
- **Director.** `c.Assist(ctx, body)` streams a run's events.
- **No secrets in errors.** Keys (`sbk_`, `sbo_`, `sbv_`, `sk-ant-`,
  `whsec_`, `Authorization`) are redacted from every error.

## Configuration

| Option | Default |
|---|---|
| `WithBaseURL` | `https://api.schedule-bit.io` |
| `WithAPIKey` | `SCHEDULE_BIT_API_KEY` |
| `WithTimeout` | 30 s per attempt |
| `WithMaxRetries` | 4 |
| `WithBackoff` | 500 ms, 30 s |
| `WithRateLimit` | off |
| `WithHTTPClient` | a plain `*http.Client` |

## Versions

Go 1.24+. This module follows semantic versioning; see
[CHANGELOG.md](CHANGELOG.md). It is generated from, and tested against, the
deployed API's contract (`/v1/openapi.json`).

## Licence

MIT.
