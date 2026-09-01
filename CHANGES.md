# Changes on top of upstream

Fork of [hyperliquid-dex/order_book_server](https://github.com/hyperliquid-dex/order_book_server),
forked at `8b4f237`. Timeline of what was changed and why, oldest first.

## d56b035 (2026-08-31) — Fix per-connection panics, subscribe snapshot, validation throttle

**Bug.** `Trade::from_fills` paired fills by position and unwrapped; a taker sweeping
several makers produces adjacent same-side fills, so the connection task panicked and
the client socket dropped every few seconds. Before: WS clients disconnected constantly.
After: fills pair by tid, incomplete pairs are dropped.

**Feature.** Subscribing to l2Book now sends an immediate snapshot. Before: a
reconnecting client showed a stale book until the next block-driven frame. After: the
current book arrives on subscribe.

**Feature.** `--snapshot-validation-interval-secs` (default 10, the old cadence). Each
validation makes the node dump its full L4 state, heavy enough to stall block execution
at 10s. Before: fixed 10s. After: tunable; seeding still retries every 10s.

## bfce557 (2026-08-31) — Trades snapshot on subscribe; central trade assembly

**Feature.** Trades are assembled once in the listener and broadcast pre-built; a
per-coin ring (last 100) seeds new trades subscribers, matching public HL WS. Before:
every connection re-parsed each fills batch, and a new subscriber saw an empty panel
until the next trade. After: one assembly pass, instant history on subscribe.

**Bug.** Upstream declared a `last_fill` guard but never assigned it, so torn-read
rewinds re-parsed completed blocks and double-broadcast their trades. After: the guard
works; replayed blocks are skipped.

## b2db2d5 (2026-08-31) — Exit on panic; fix torn-line log slice panic

**Bug.** The error logger sliced `line[..100]`; a torn read shorter than 100 bytes made
it panic. Tokio swallows panics in spawned tasks, so the listener died while the process
kept serving a frozen book — no exit, no restart, no alarm. Before: silent frozen books.
After: safe truncation, plus a panic hook that exits(1) so systemd restarts on any
future panic.

## 6e905d1 (2026-08-31) — `--l2-broadcast-min-interval-ms`: lossless l2Book coalescing

**Feature.** Every l2 broadcast is a full snapshot, so intermediates can be dropped
without data loss. Before: one broadcast (and one all-coins snapshot build) per block,
~13/s of CPU-heavy work. After: at most one per interval, subscribers at most one
interval behind. Default 0 keeps upstream behavior; subscribe snapshots and trades are
never throttled.

## a488fe2 (2026-08-31) — Presence-driven MQTT publisher to AWS IoT Core

**Feature.** Publishes book/trades frames to AWS IoT Core so browsers consume via MQTT
instead of connecting to this server directly (the node's WS no longer needs to be
internet-facing). Browsers heartbeat `order-book-presence/...`; only watched
(coin, variant) pairs are published (retained l2Book, live + retained-seed trades),
watches expire 90s after the last beat. mTLS, 80/s token bucket, 5k watch cap; MQTT
failures warn and back off, never taking the WS server down. Enabled by
`--mqtt-endpoint` + cert/key/CA paths.

## b3ca64b (2026-09-01) — Resync instead of exiting on snapshot validation mismatch

**Bug.** The node re-prices triggered stop orders as they enter the book, but the
status event only carries the placement-time px, so the reconstruction drifts whenever
a triggered stop rests — and validation then killed the process, every few minutes in
stop-heavy periods. Before: each mismatch dropped all WS/MQTT consumers for ~15s.
After: the book state resets and re-seeds from the next snapshot in-process;
connections and MQTT watches survive. Two silent early-returns in the validation task
are now logged.
