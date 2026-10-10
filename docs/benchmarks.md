# Benchmarks

Every number on this page was produced by `load/run_benchmark.py` against the
running stack on the hardware described below. Nothing here is projected,
extrapolated, or carried over from another system. Where a figure could not be
measured it is written `n/a` rather than filled in.

Raw output is committed in [`load/`](../load) as JSON.

---

## 1. Test environment

| | |
|---|---|
| CPU | Intel Core i5-11320H @ 3.20GHz, 4 physical / 8 logical cores |
| RAM | 7.8 GB total; **Docker limited to 3.99 GB** |
| OS | Windows 11, Docker Desktop 29.6.1 (WSL2 backend) |
| Database | PostgreSQL 16.6 + TimescaleDB 2.17.2 |
| Broker | Redpanda v24.2.18, single node, `--smp=1 --memory=1G` |
| Python | 3.12 (containers), aiokafka 0.10, asyncpg |
| Dataset | Real `car_telemetry` rows from OpenF1 session 9472 (2024 Bahrain GP) |

**This is a laptop under a 4 GB Docker memory cap running an eight-container
stack, with the broker pinned to a single core.** The absolute numbers would be
considerably higher on a server. What the numbers are good for is the *shape* of
the system: where the bottleneck is, where it saturates, and what moves it.

### Method

The harness publishes real telemetry rows at a controlled rate, waits for the
consumer group to drain, then reads percentiles from the `pipeline_events`
table, which the processor populates from per-stage trace stamps on each event.

Rate control is the point. Publishing as fast as possible measures the
*producer* and reports a latency dominated by queueing — an event that waited 78
seconds in a backlog really did take 78 seconds, but that figure describes a
saturated pipeline, not a working one. Quoting it as "end-to-end latency" would
be meaningless.

"Kept up" means the producer sustained ≥95% of the target rate **and** the
consumer drained within 10 seconds afterwards. Anything else is saturation.

### Reference: what a real session actually produces

Measured against the live OpenF1 API, not assumed:

```
20 drivers x 3.9 Hz  (car_data)  =  ~78 events/s
20 drivers x 3.7 Hz  (location)  =  ~74 events/s
                                    -------------
                                     ~152 events/s
```

Every rate below is relative to that ~152 events/s figure.

---

## 2. Headline result

With one processor instance and the measured configuration:

| | |
|---|---|
| **p50 end-to-end latency at live rate (150/s)** | **45.4 ms** |
| p95 at live rate | 76.4 ms |
| p99 at live rate | 89.7 ms |
| **Sustained throughput, 1 instance** | **1,500 events/s** (~10x live rate) |
| **Sustained throughput, 3 instances** | **2,000 events/s** |
| Saturation point, 1 instance | between 1,500 and 2,000 events/s |

End-to-end is measured `ingest_ts → db_ts`: from the moment the event entered
the pipeline to the moment its database write committed.

---

## 3. Throughput sweep — 1 processor instance

Batch timeout 0.05s, 30s per run.

| Target | Achieved | p50 | p95 | p99 | max | Peak lag | Kept up |
|---:|---:|---:|---:|---:|---:|---:|:---:|
| 150/s *(live rate)* | 150/s | 45.4 ms | 76.4 ms | 89.7 ms | 96.0 ms | 2 | yes |
| 1,000/s | 999/s | 99.7 ms | 192.9 ms | 228.4 ms | 270.8 ms | 6 | yes |
| 1,250/s | 1,250/s | 120.8 ms | 230.7 ms | 354.1 ms | 483.0 ms | 6 | yes |
| 1,500/s | 1,500/s | 150.0 ms | 283.2 ms | 378.6 ms | 527.6 ms | 15 | yes |
| 2,000/s | 1,999/s | 11,440 ms | 28,481 ms | 29,432 ms | 29,942 ms | 27,077 | **no** |
| 3,000/s | 2,998/s | 30,611 ms | 61,421 ms | 63,804 ms | 64,224 ms | 60,979 | **no** |

Latency degrades gracefully up to 1,500/s and then collapses. That collapse is
the designed behaviour, not a failure: the consumer stops fetching, the backlog
accumulates on the broker where it is visible as `racestream_consumer_lag`, and
nothing is lost. Peak lag of 27,077 at 2,000/s is the backpressure signal doing
its job.

---

## 4. The finding: latency was a config value, not a limit

The first sweep showed a p50 of **303 ms** at live rate, with this stage
breakdown:

| Target | Kafka | Derive | DB write |
|---:|---:|---:|---:|
| 150/s | 9.5 ms | 0.1 ms | **292.4 ms** |

The database write appeared to account for 292 ms of a 303 ms total — which did
not match the 2 ms round-trip the health check reports.

The `process_to_db` stage does not measure the write. It measures **buffer wait
plus write**: how long an event sits in the writer's batch before the flush that
carries it. The writer flushes on batch size (500 rows) *or* batch timeout,
whichever comes first. At 150 events/s a 500-row batch takes 3.3 seconds to
fill, so every flush was triggered by the 0.5s timeout, and the average event
waited roughly half of it.

So the dominant term in end-to-end latency was a tuning constant.

**Experiment:** drop `DB_WRITE_BATCH_TIMEOUT_S` from 0.5 to 0.05 and re-run
the identical sweep.

| Rate | p50 @ 0.5s | p50 @ 0.05s | Change |
|---:|---:|---:|---:|
| 150/s | 303.3 ms | **45.4 ms** | **6.7x faster** |
| 1,000/s | 354.1 ms | **99.7 ms** | **3.6x faster** |

Peak lag at 1,000/s also fell from 1,072 to 6.

The cost is more, smaller write operations. At the live rate that is roughly 20
flushes per second instead of 2 — immaterial here, and the saturation point did
not move (still between 1,500 and 2,000/s), confirming the timeout affects
latency rather than throughput. 0.05s is now the default, and
[`.env.example`](../.env.example) records why.

**The general lesson:** the stage names in a latency breakdown describe where
time was *spent*, not what *caused* it. "DB write: 292 ms" was true and
misleading at the same time. The instrumentation found the number; reading it
carefully found the cause.

---

## 5. Horizontal scaling

`docker compose up -d --scale processor=3`. The consumer group rebalanced to 3
members with 2 partitions each and `TOTAL-LAG 0`, verified with
`rpk group describe`.

| Rate | 1 instance | 3 instances | |
|---:|---:|---:|---|
| 2,000/s | p50 11,440 ms, lag 27,077, **saturated** | p50 **99.7 ms**, lag 18, keeps up | ✅ |
| 3,000/s | p50 30,611 ms, lag 60,979, **saturated** | p50 152.6 ms, p95 3,437 ms, lag 4,510, drains in 6.4s | marginal |

Scaling resolves saturation at 2,000/s completely. At 3,000/s three instances
keep up by the stated definition, but the p95 of 3.4 s and a peak lag of 4,510
show it is at the edge rather than comfortable.

**Scaling is not linear.** 3x the instances bought roughly 1.3-2x the
throughput, because all three write to the same PostgreSQL instance and contend
there. Partition count is not the limit — six partitions would allow six
instances. The next constraint is the database, which on this hardware shares
four physical cores with the broker, three processors, the API and the frontend.

I have not tested beyond three instances, so I make no claim about four.

---

## 6. Where the time actually goes

Stage p50, batch timeout 0.05s, one instance:

| Rate | Kafka (produce→consume) | Derive | Buffer + DB write |
|---:|---:|---:|---:|
| 150/s | 11.7 ms | 0.1 ms | 33.4 ms |
| 1,000/s | 18.6 ms | 0.5 ms | 78.9 ms |
| 1,500/s | 19.0 ms | 0.8 ms | 118.2 ms |
| 2,000/s *(saturated)* | 11,317 ms | 11.2 ms | 118.3 ms |

Two things stand out:

1. **Derivation is free.** Sub-millisecond at every sustainable rate. The
   session-state machine — sector merging, personal and session bests, gap
   handling — is not a bottleneck and would not be worth optimising.
2. **Saturation shows up entirely in the Kafka stage.** At 2,000/s the write
   stage is unchanged at 118 ms while the Kafka stage jumps four orders of
   magnitude. That is queue wait, not slow I/O: the events are sitting in the
   log waiting for a consumer. This is exactly what the backpressure design
   predicts, and it is why consumer lag is the metric that matters.

---

## 7. Correctness under load

Verified on the live stack, not asserted:

**Idempotency at scale.** A full session ingest was run twice — 214,920 events
published, consumed and persisted on each pass:

| Table | After pass 1 | After pass 2 |
|---|---:|---:|
| `car_telemetry` | 89,920 | 89,920 |
| `positions` | 93,100 | 93,100 |
| `timing` | 30,421 | 30,421 |
| `laps` | 1,129 | 1,129 |
| `weather` | 157 | 157 |
| `race_control_events` | 71 | 71 |

Not one duplicate row. This is the natural-key upsert described in
[`engineering-decisions.md`](engineering-decisions.md#5-natural-keys-for-idempotency-not-event_id)
doing its job on real data at real volume.

**Mapping fidelity.** All seven OpenF1 endpoints mapped with **0 records
dropped** across ~76,000 records.

**Backlog recovery.** A 125,007-event backlog drained to zero in ~150 seconds
with no loss, at roughly 830 events/s.

---

## 8. Limitations

Stated plainly, because a benchmark without its caveats is marketing:

- **Single-node everything.** One Redpanda broker (`--smp=1`), one PostgreSQL
  instance, replication factor 1. No fault tolerance is being measured here.
- **Laptop hardware under a 4 GB Docker cap**, with every component competing
  for the same four physical cores. A dedicated host would change the absolute
  numbers substantially.
- **30-second runs.** Long enough for a stable percentile at these rates, not
  long enough to show compaction, chunk rollover, or thermal effects.
- **Car telemetry only** in the throughput sweep. It is the highest-volume
  channel, but a mixed workload would exercise more write paths.
- **Latency is sampled 1-in-20.** Prometheus histograms see every event; the
  exact percentiles here come from a sample. At 3,000 samples per run the
  p50 and p95 are solid; treat p99 as indicative.
- **The WebSocket stage is not in these numbers.** `ingest_ts → db_ts` is what
  is measured today. Delivery to the browser is a further hop and will be
  measured separately once that service lands.
- **Untested beyond 3 processor instances** and beyond 3,000 events/s.

---

## 9. Reproducing

```bash
docker compose up -d
python -m racestream_ingestion.main --session 9472   # ~215k events
python load/run_benchmark.py --sweep 150,1000,1500,2000 --duration 30
```

Scaling run:

```bash
docker compose up -d --scale processor=3 processor
python load/run_benchmark.py --sweep 2000,3000 --duration 30
```
