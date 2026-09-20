# Engineering decisions

Each entry states the problem, what was considered, what was chosen, why, and
what it costs. The tradeoff line is the important one: a decision with no cost
is usually a decision that was not examined.

---

## 1. Redpanda over Kafka, Pulsar or a queue

**Problem.** The pipeline needs durable, replayable, partitioned, ordered
transport that several independent consumer groups can read at their own pace.

**Options.**
- **RabbitMQ / Redis Streams** — simpler, but a queue deletes on acknowledgement.
  Replay would mean re-fetching from upstream, and the replay engine is a
  headline feature.
- **Apache Kafka** — the reference implementation, but a local stack needs a JVM
  and, on older versions, ZooKeeper. Heavy for something meant to start with one
  command on a laptop.
- **Apache Pulsar** — capable, but adds BookKeeper and a much larger operational
  surface for no benefit at this scale.
- **Redpanda** — Kafka wire-compatible, single binary, no JVM, no ZooKeeper.

**Decision.** Redpanda, accessed through standard Kafka clients (`aiokafka`).

**Why.** The Kafka *protocol* is what the design actually depends on — consumer
groups, offsets, partitions, retention. Redpanda provides those semantics with a
fraction of the footprint, and nothing in this codebase is Redpanda-specific, so
swapping in Kafka is a compose change.

**Tradeoff.** A smaller ecosystem than Kafka's, and one less battle-tested at
extreme scale. Neither matters here, and protocol compatibility keeps the exit
cheap.

---

## 2. TimescaleDB over InfluxDB, ClickHouse or plain Postgres

**Problem.** Store millions of high-frequency telemetry samples *and* the
relational session metadata they join against.

**Options.**
- **Plain PostgreSQL** — one system, but a multi-million-row append-only table
  degrades without partitioning, and partitioning by hand is what Timescale
  automates.
- **InfluxDB** — purpose-built for time series, but relational joins are awkward
  and the session/driver/stint model is genuinely relational.
- **ClickHouse** — excellent analytical throughput, but weaker on the
  single-row-lookup and update patterns the metadata tier needs.
- **TimescaleDB** — Postgres with automatic time partitioning and columnar
  compression.

**Decision.** PostgreSQL with the TimescaleDB extension.

**Why.** This workload is genuinely both shapes at once. Timescale lets the
telemetry tier be hypertables while the metadata tier stays ordinary relational
tables — in the same database, in the same transaction, joinable in one query.
Running two datastores to avoid one extension would be the more complex choice.

**Tradeoff.** Postgres will not match ClickHouse on a large analytical scan. For
this workload — bounded time ranges for one session and driver — the hypertable
primary key serves the query directly.

---

## 3. JSON on the wire, not Avro or Protobuf

**Problem.** Events must be serialised between services with some schema
guarantee.

**Options.**
- **Avro + Schema Registry** — compact, with real schema evolution, at the cost
  of another service and unreadable messages on the topic.
- **Protobuf** — compact and fast, with a code-generation step in every service.
- **JSON + Pydantic** — larger on the wire, validated in application code,
  readable directly off the topic.

**Decision.** JSON, validated by Pydantic v2 at every boundary.

**Why.** The thing that repeatedly saves time when debugging a pipeline is being
able to run `rpk topic consume` and *read the message*. Pydantic supplies the
validation a registry would otherwise provide, and `schema_version` rides in the
envelope so a version mismatch is detectable. For a system whose peak is
thousands of messages per second, not millions, the byte overhead does not bind.

**Tradeoff.** Roughly 3-5x the bytes of Avro, and schema evolution is enforced
by convention and tests rather than by a registry. If throughput ever became the
binding constraint, the serde module is the only file that would change.

---

## 4. One envelope for every topic

**Problem.** Should each topic carry its own bare message type?

**Decision.** A single `Event` envelope with a discriminated payload union.

**Why.** Consumers, metrics, tracing and replay all become uniform. A consumer
routes on `event_type` without deserialising the payload. `PipelineTrace` rides
along, so latency instrumentation is automatic rather than per-type. Adding an
event type is a payload class plus a routing entry.

**Tradeoff.** Envelope overhead on every message, and `event_type` duplicates
`payload.kind`. The duplication is deliberate — it allows routing without full
deserialisation — and because redundancy is only safe when checked, a validator
rejects any event where the two disagree.

---

## 5. Natural keys for idempotency, not `event_id`

**Problem.** At-least-once delivery means the same event can be processed twice.
Replay means the same *source data* can be processed on entirely separate runs.

**Options.**
- **`event_id`** — obvious, but it is generated at ingest. A second ingestion of
  the same source rows produces different UUIDs, so it does not deduplicate
  across runs.
- **A processed-ids table** — correct, but adds a lookup on the hot write path
  and a table that grows without bound.
- **Natural key** — `(session_id, driver_number, sample_time)`, derived from the
  source data itself.

**Decision.** The natural key, enforced as the table primary key with
`ON CONFLICT DO NOTHING`.

**Why.** It is stable across runs, requires no extra state, and pushes the
guarantee into the database where it cannot be bypassed. Combined with
at-least-once delivery it gives effectively-once end to end.

**Tradeoff.** Two genuinely distinct samples for the same car at the same
timestamp would collapse into one. At a 3.7 Hz sample rate with millisecond
timestamps this does not occur, and if upstream ever did emit it, the duplicate
would carry no new information.

Laps are the exception: they use `ON CONFLICT DO UPDATE` with `COALESCE`,
because upstream publishes a lap row when the lap starts and fills in sector
times later. A late-arriving `NULL` must not erase a value already known.

---

## 6. Explicit offset commits, not auto-commit

**Problem.** When should a consumer record that it has processed a message?

**Decision.** `enable_auto_commit=False`; commit after the handler returns.

**Why.** Auto-commit commits on a timer, independent of whether processing
succeeded. A crash between an auto-commit and a successful write loses events
permanently and silently. Committing after the handler makes the failure mode
duplicates instead of loss — and duplicates are already handled by decision 5.

**Tradeoff.** A crash re-processes the last batch. That is the intended cost.

---

## 7. Backpressure by not fetching, not by queueing

**Problem.** What happens when the producer outruns the consumer?

**Options.**
- **Unbounded in-process queue** — absorbs bursts until the container is
  OOM-killed, with no warning.
- **Bounded queue that drops** — loses data to protect the process.
- **Stop fetching** — let the backlog sit on the broker.

**Decision.** Stop fetching. The consumer awaits the handler before polling
again, so a slow handler naturally throttles the fetch.

**Why.** The backlog then lives on the broker, where it is *measurable*
(`racestream_consumer_lag`), bounded by retention, and fully recoverable. An
in-process backlog is invisible until it kills the process. This choice is what
makes the consumer-lag panel meaningful rather than decorative.

**Tradeoff.** Lag grows during a burst rather than being absorbed silently. That
is a feature: the growth is the signal.

---

## 8. A separate replay service, not a replay mode

**Problem.** Replay could be a flag on the ingestion service.

**Decision.** A separate service publishing to the same topics.

**Why.** Different lifecycle (on-demand, not continuous), different control
surface (play/pause/seek/speed), different source (database, not HTTP). Folding
both into one service would mean a single process with two unrelated state
machines. Separating them also proves the architectural claim that matters:
because replay is *just another producer*, the downstream path cannot special-case
it — which is exactly why a replay test is a real integration test.

**Tradeoff.** One more service to run. Worth it for the isolation.

---

## 9. WebSocket, not SSE or polling

**Problem.** Push telemetry to the browser at several hundred events per second.

**Options.**
- **Polling** — an extra database query per client per interval, and latency
  floored at the poll period. Unusable for this.
- **SSE** — genuinely good for one-way push, but unidirectional, so subscription
  changes need a side channel, and it has a per-browser connection cap.
- **WebSocket** — bidirectional, one connection, no cap concern.

**Decision.** A single WebSocket per client, multiplexing all channels.

**Why.** Subscription management (driver selection, channel choice) needs a
client-to-server path, and one connection per chart would exhaust browser
connection limits and multiply server state.

**Tradeoff.** More connection management than SSE: heartbeats, reconnect with
backoff, and explicit stale detection. That work is in `lib/config.ts` and the
socket hook.

Health, by contrast, *is* polled — it is a low-frequency question about the
system rather than a stream of events, and a WebSocket would be the wrong shape.

---

## 10. asyncpg over SQLAlchemy

**Problem.** Database access from the services.

**Decision.** asyncpg with hand-written SQL.

**Why.** The write path is batches of hundreds of rows via `executemany`; the
read path is hand-tuned time-series queries with Timescale-specific clauses. An
ORM would add a mapping layer over work already expressed most clearly as SQL,
and asyncpg's binary protocol is a material part of the write throughput.

**Tradeoff.** No migration framework and no compile-time query checking. Migrations
are versioned SQL in `db/init/`; every query is parameterised, so the injection
risk an ORM would mitigate is handled directly.

---

## 11. One Dockerfile for four Python services

**Problem.** Four services share a common library.

**Decision.** A single multi-stage Dockerfile with a target per service.

**Why.** They differ only in entrypoint, and all depend on `racestream_common`.
One build shares the dependency layers across all four and guarantees identical
library versions — which matters when they exchange serialised events. Four
Dockerfiles would be four copies of the same layer ordering, drifting apart.

**Tradeoff.** Each image carries all four services' source. That is a few
hundred kilobytes of Python against a shared base of hundreds of megabytes.

---

## 12. Absent values render as `N/A`, never as `0`

**Problem.** What does the UI show when a measurement was not taken?

**Decision.** `null` through every layer, rendered as an em-dash. Never
substituted with a zero, a placeholder or a last-known value.

**Why.** This is the load-bearing decision of the whole project. A dashboard
that shows `0 ms` when it did not measure latency is not a dashboard, it is a
liar — and every other number on the screen becomes untrustworthy. Distinguishing
"not measured" from "measured as zero" is what separates an instrument from a
decoration.

It is enforced at three levels: optional fields in the Pydantic models,
`elapsed_seconds()` returning `None` for unstamped stages, and formatters that
return the em-dash for `null`, `undefined` and `NaN` while passing a real `0`
through. Tested in `tests/unit/test_schemas.py` and
`frontend/src/test/shell.test.tsx`.

**Tradeoff.** More `null` handling in every layer. That is the price of not
lying.

---

## 13. Honest placeholders instead of mock dashboards

**Problem.** What should an unbuilt screen show?

**Decision.** A statement of what is not built yet and which phase delivers it.
Not a mock-up with plausible-looking telemetry.

**Why.** A screenshot of invented telemetry is indistinguishable from a
screenshot of real telemetry, and this project's central claim is that it does
not fabricate data. Rendering fake cars on a fake track to fill a screen would
contradict it on the very first screen a reviewer opens.

**Tradeoff.** The project looks less finished mid-build than it could. The README
build-status table is kept accurate for the same reason.
