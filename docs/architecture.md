# Architecture

> Status: this document describes the system as it stands. Sections covering
> services that are not yet built are marked **(planned)** and say which phase
> delivers them. Nothing here describes behaviour that does not exist.

---

## 1. System diagram

```
                       OpenF1 public API (free, unauthenticated)
                                     |
                                     | HTTP, polled
                                     v
                      +--------------------------------+
                      |      Ingestion Service         |   (planned, Phase 2)
                      |  fetch -> validate -> envelope |
                      +--------------------------------+
                                     |
              Replay Service --------+           (planned, Phase 7)
              (historical -> replay clock)
                                     |
                                     v
                      +--------------------------------+
                      |   Redpanda (Kafka protocol)    |
                      |                                |
                      |  racestream.telemetry.v1   x6  |
                      |  racestream.position.v1    x6  |
                      |  racestream.timing.v1      x3  |
                      |  racestream.session.v1     x1  |
                      |  racestream.dlq.v1         x1  |
                      +--------------------------------+
                                     |
                      +--------------+-----------------+
                      |                                |
                      v                                v
        +--------------------------+      +--------------------------+
        |    Stream Processor      |      |   Additional consumer    |
        |  validate / derive /     |      |   groups scale here      |
        |  batch / persist         |      |                          |
        +--------------------------+      +--------------------------+
                      |
          +-----------+-----------+
          |                       |
          v                       v
  +----------------+     +--------------------+
  |  TimescaleDB   |     |  WebSocket Fanout  |   (planned, Phase 5)
  |  hypertables   |     |  in-process pub/sub|
  +----------------+     +--------------------+
          |                       |
          +-----------+-----------+
                      |
                      v
              +---------------+          +--------------+      +---------+
              |   FastAPI     |--/metrics--> Prometheus  |----->| Grafana |
              +---------------+          +--------------+      +---------+
                      |
                      v
              +---------------+
              | React (nginx) |
              +---------------+
```

---

## 2. Service responsibilities

| Service | Owns | Does not own | Status |
|---|---|---|---|
| **Ingestion** | Talking to OpenF1, normalising responses, building envelopes, publishing | Any derived value; any database write | Phase 2 |
| **Stream Processor** | Consuming topics, deriving sector/lap state, batching writes to TimescaleDB | Fetching from upstream; serving HTTP | Phase 4 |
| **Replay** | Reading a stored session, re-emitting it on a replay clock | Any downstream behaviour — it publishes to the same topics | Phase 7 |
| **API** | REST surface, WebSocket fanout, health, chaos controls, own metrics | Writing telemetry | **Built** |
| **Frontend** | Rendering, buffering, interpolation, connection state | Deciding what is healthy — it displays what the API reports | Shell built |

The split is along **failure boundaries**, not along data types. Ingestion can
die without losing what is already in Kafka. The processor can die and resume
from its committed offsets. The API can restart without interrupting ingestion.
Each is independently restartable, which is what makes the chaos testing in
Phase 9 meaningful rather than theatrical.

---

## 3. Data flow

1. **Fetch.** Ingestion requests a window from OpenF1, with bounded concurrency
   and retry on transient failure.
2. **Validate.** Each upstream record is parsed into a typed payload. A record
   that fails validation is counted and dropped — it never enters the pipeline.
3. **Envelope.** A valid payload is wrapped in an `Event`, stamped with
   `trace.ingest_ts`, and keyed `session_id:driver_id`.
4. **Publish.** The event routes to a topic by its `event_type`. `trace.produce_ts`
   is stamped at the produce call.
5. **Consume.** The processor reads batches, stamping `trace.consume_ts`. A
   message that fails to deserialise goes to the DLQ with its rejection reason,
   and the offset advances — one poison message never stalls a partition.
6. **Derive.** Per-car state (sector deltas, personal bests, stint) is computed.
   `trace.process_ts` is stamped.
7. **Persist.** Rows are batched and written with idempotent upserts.
   `trace.db_ts` is stamped.
8. **Fan out.** The same event is published to connected WebSocket clients,
   stamping `trace.ws_ts`.
9. **Render.** The frontend buffers, interpolates and repaints on a fixed budget.

Every stamp in that list is a real `datetime` written by the stage that did the
work. End-to-end latency is the difference between two of them — never an
estimate, and never defaulted when a stage did not run.

---

## 4. Topic and partition strategy

### Why these topics

Topics are split by **volume and consumer interest**, not one-per-event-type.

| Topic | Parts | Retention | Carries | Rationale |
|---|---|---|---|---|
| `racestream.telemetry.v1` | 6 | 6h | `car_telemetry` | ~99% of byte volume with position. Isolated so a consumer that only wants lap times never reads it. |
| `racestream.position.v1` | 6 | 6h | `position` | Separate from telemetry so the map consumer scales and restarts independently. |
| `racestream.timing.v1` | 3 | 24h | `lap`, `timing` | Low volume, high value. Kept a day. |
| `racestream.session.v1` | 1 | 7d | `weather`, `race_control`, `session_meta` | **One partition on purpose**: global ordering of race-control messages matters more than throughput. A red flag must not be reordered behind a yellow. |
| `racestream.dlq.v1` | 1 | 7d | rejected bytes + headers | Never blocks the main path. |

Retention is short on the telemetry topics because **TimescaleDB is the
archive**, not Kafka. Kafka here is a transport and a short replay buffer.

### Why this partition key

Every event is keyed `session_id:driver_id`.

Kafka guarantees ordering *within a partition*, not across a topic. Everything
this system derives — sector deltas, stint state, lap progression — is
**per-car**. Keying by car puts all of one car's events on one partition, which
delivers exactly the ordering guarantee the derivations need, and no more.

Keying by session alone would serialise the whole session onto one partition
and cap throughput at one consumer. Keying by event id would spread a single
car's events across partitions and destroy the ordering the derivations depend
on. Session-scoped events (weather, race control) have no driver and key on the
session, which is correct because they belong to the single-partition topic
anyway.

Six partitions for ~20 drivers means roughly 3-4 cars per partition and leaves
room to add processor instances without repartitioning.

---

## 5. Database architecture

Two tiers with different access patterns, so they get different treatment:

**Metadata tier** — `sessions`, `drivers`, `teams`, `stints`,
`race_control_events`. Small, mutable, queried by join. Ordinary Postgres tables
with conventional primary keys.

**Telemetry tier** — `car_telemetry`, `positions`, `timing`, `laps`, `weather`,
`pipeline_events`. Append-heavy, time-ordered. TimescaleDB hypertables, chunked
by time (1h for the high-frequency tables, 6h-1d for the rest).

### Query patterns the indexes serve

Indexes cost write throughput on an append-heavy table, so each one names its query:

| Index | Serves |
|---|---|
| `sessions (year DESC, date_start DESC)` | Session Explorer listing |
| `laps (session_id, driver_number, lap_number)` | Driver page lap list; analytics lap-time evolution |
| `race_control_events (session_id, event_time)` | Replay timeline markers |
| `pipeline_events (run_label, observed_at DESC) WHERE run_label IS NOT NULL` | Benchmark percentiles for one named run. Partial, because unlabelled rows are never queried this way. |

The hypertable primary keys — `(session_id, driver_number, sample_time)` — serve
the dominant read pattern (one driver's channel over a time range) directly, so
no secondary index is needed on the two largest tables.

### Compression

`car_telemetry` and `positions` are compressed after 7 days, segmented by
`(session_id, driver_number)` and ordered by `sample_time DESC`. Chunks are
immutable once a session is over, so this only ever touches closed chunks.

---

## 6. Failure handling

| Failure | Behaviour |
|---|---|
| Upstream returns 5xx or times out | Retried with exponential backoff, bounded attempts. Counted in `racestream_errors_total`. |
| One upstream record is malformed | Dropped, counted in `racestream_messages_dropped_total`. The batch continues. |
| A Kafka message fails to deserialise | Routed to `racestream.dlq.v1` with reason, origin topic and receipt time as headers. Offset advances. |
| Kafka unreachable at produce | Per-event failure isolation: the batch continues, the failure is counted. |
| Processor crashes | Uncommitted offsets are re-consumed on restart. Idempotent upserts absorb the duplicates. |
| Database write fails | Batch dropped and counted, not retried in memory — Kafka holds the offsets, and an unbounded retry queue is the failure this design avoids. |
| Consumer falls behind | Lag grows on the broker, which is visible and bounded by retention. No in-process buffer grows. |
| Database down at API start | The API still starts, because `/health` must be reachable to *report* the outage. Health returns `down` + HTTP 503. |

### Delivery semantics

**At-least-once**, made effectively-once by idempotent writes.

Offsets are committed only after a batch has been handled. A crash therefore
replays the tail of the log. Every write targets a natural key derived from the
source data — `(session_id, driver_number, sample_time)` — so the replayed rows
collide with what is already there and the second pass is a no-op.

`event_id` is deliberately **not** the deduplication key: it is a fresh UUID on
each ingestion run, so it would not deduplicate across a re-ingest or a replay.
This is verified in `tests/unit/test_schemas.py::TestIdempotencyKey`.

---

## 7. Backpressure

There is no unbounded queue anywhere in the pipeline.

- **Consumer**: the fetch is bounded by `max_poll_records`, and the loop awaits
  the handler before polling again. A slow handler simply stops fetching.
- **Writer**: buffers flush on size **or** timeout, whichever comes first.
  Queue occupancy is exported as `racestream_queue_depth`.
- **Frontend**: telemetry is held in fixed-size rolling buffers, and the render
  loop repaints on a fixed budget rather than per event.

When the producer outruns the consumer, the backlog accumulates **on the
broker**, where it is measurable (`racestream_consumer_lag`), bounded by
retention, and recoverable. It does not accumulate in a process heap, where it
would be invisible until the container was OOM-killed.

---

## 8. WebSocket architecture *(planned, Phase 5)*

One connection per client, not one per chart. The client subscribes to the
channels it needs; the server fans one event out to all interested connections.
A slow client is dropped rather than allowed to back up the fanout.

The frontend reconnects with exponential backoff and jitter, and distinguishes
`connected`, `stale` (socket open, no recent events) and `disconnected`. The
`stale` state exists because an open socket with no data is not "live", and
showing LIVE in that situation would be false.

---

## 9. Replay architecture *(planned, Phase 7)*

The replay engine reads a stored session and re-emits it on a replay clock with
a speed multiplier, preserving inter-event timing.

The single design rule: **replay publishes the same envelopes to the same topics
and is read by the same consumer groups as live ingestion.** The only difference
is `source: "replay"`, which exists so the UI can be honest about what it is
showing.

This is what makes replay valuable beyond a demo feature: a replay test
exercises the entire production path deterministically, which is otherwise very
hard to do for a real-time system.

---

## 10. Observability

Metrics are defined once in `racestream_common/obs/metrics.py` and used by every
service, so a counter means the same thing everywhere.

**Cardinality policy.** Labels come from small closed sets: `event_type` (7),
`topic` (5), `stage` (6), `component`, `reason`. `session_id` and `driver_id`
are **never** labels — a race weekend would add thousands of series. Per-driver
questions are answered from TimescaleDB through the API, which is the right tool
for them.

A target label must never collide with a metric label: a `component` label in
the scrape config would shadow the metric's own and silently push the real value
to `exported_component`, breaking every query that groups by it. The scrape
config therefore attaches no labels the metrics already carry.

Percentiles are computed once as Prometheus recording rules, so the System
Health screen and the Grafana dashboards cannot disagree.

---

## 11. Scaling strategy

| Bottleneck | Response |
|---|---|
| Processor CPU-bound | Add instances to the consumer group. Six telemetry partitions support six. |
| More than six instances needed | Repartition the topic (requires a `.v2` topic and a dual-read migration). |
| Database write throughput | Increase batch size; the cost is latency, and the tradeoff is measurable in `pipeline_events`. |
| WebSocket fanout | Fanout is in-process today. Beyond one API instance it needs a shared bus — deliberately deferred, because it is not the bottleneck at this scale and building it early would be speculative. |
| Storage growth | Compression after 7 days; retention policies per hypertable. |

Anything in the "response" column that has not been measured is a hypothesis,
and is labelled as such. Measured figures live in
[`benchmarks.md`](benchmarks.md).
