<div align="center">

# RACESTREAM

**Real-Time F1 Telemetry & Race Operations Platform**

A distributed streaming system that ingests public Formula 1 timing data, moves it through
Redpanda into TimescaleDB and a WebSocket fanout, and renders it as a race-engineering
console — with replay, latency instrumentation, and deliberate failure injection.

</div>

---

> ### Positioning — please read
>
> RaceStream is an **independent engineering project** built on **publicly available**
> F1 timing and telemetry data from the free [OpenF1](https://openf1.org) API.
>
> - It is **not** official F1 software and is not affiliated with Formula 1, the FIA, or any team.
> - It does **not** use, reproduce, or approximate proprietary F1 team systems or team telemetry.
> - All telemetry shown is real data from the public API. **Nothing is fabricated.** Where a data
>   channel is unavailable for a session, the UI says so instead of inventing values.
> - All performance figures in [`docs/benchmarks.md`](docs/benchmarks.md) come from benchmark runs
>   on hardware documented in that file. No number in this repository is aspirational.

---

## The problem this solves

A live race session produces a continuous, out-of-order, lossy stream of telemetry from twenty
cars at once. Turning that into something a race engineer can act on within a corner's worth of
time is not a charting problem — it is a distributed systems problem:

- **Volume**: car telemetry arrives at roughly 3.7 Hz per driver across six channels, plus
  positional data at a similar rate. A race session is millions of samples.
- **Ordering**: derived values (sector deltas, stint state, gaps) depend on per-car ordering,
  but nothing upstream guarantees it.
- **Latency**: a number that arrives late is worse than useless, so latency must be *measured*
  end to end, not assumed.
- **Failure**: consumers die mid-session. The system has to notice, recover, and not lose events.
- **Replay**: the ability to re-run a past session through the *real* pipeline is what makes any
  of the above testable.

RaceStream is built to demonstrate each of those explicitly, and to measure rather than claim.

## Architecture

```
                    OpenF1 public API  /  recorded session fixtures
                                  |
                    +-------------+-------------+
                    |                           |
             Ingestion Service            Replay Service
          (live poll -> envelope)     (historical -> replay clock)
                    |                           |
                    +-------------+-------------+
                                  |
                          Redpanda (Kafka API)
                telemetry.v1 | position.v1 | timing.v1 | session.v1 | dlq.v1
                                  |
                    +-------------+-------------+
                    |                           |
             Stream Processor              (other consumer
        validate / derive / batch           groups scale here)
                    |
        +-----------+-----------+
        |                       |
   TimescaleDB            WebSocket Fanout
  (hypertables)           (in-process pub/sub)
        |                       |
        +-----------+-----------+
                    |
                 FastAPI  ---- /metrics ----> Prometheus ----> Grafana
                    |
              React Frontend
```

Both live ingestion and replay publish **the same envelope to the same topics**, and are consumed
by **the same consumer groups**. The frontend cannot tell the difference except by reading the
`source` field — which is precisely the point: replay exercises the production path, so a replay
test is a real integration test.

Full detail: [`docs/architecture.md`](docs/architecture.md) ·
Event schemas: [`docs/data-contracts.md`](docs/data-contracts.md) ·
Rationale for every major choice: [`docs/engineering-decisions.md`](docs/engineering-decisions.md)

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Streaming | Redpanda (Kafka API) | Kafka semantics — consumer groups, offsets, replay — without the JVM/ZooKeeper footprint for a local stack |
| Processing | Python 3.11+, asyncio, aiokafka | The pipeline is I/O-bound, not CPU-bound |
| Contracts | Pydantic v2 | Validation at every boundary; a typed envelope instead of loose dicts |
| Storage | PostgreSQL + TimescaleDB | Hypertables for high-frequency telemetry, ordinary relational tables for metadata |
| API | FastAPI | Async, native Pydantic response models, OpenAPI for free |
| Transport | WebSocket | Push, not poll — one connection per client, fanned out to every panel |
| Frontend | React + TypeScript + Vite + Tailwind | Rolling buffers and memoised components over a high-frequency feed |
| Observability | Prometheus + Grafana | Real metrics from real counters |
| Infra | Docker Compose | `docker compose up` brings up the whole system |

## Quick start

**Requirements:** Docker with Compose v2. Nothing else — no API key, no account. OpenF1 is free
and unauthenticated.

```bash
git clone https://github.com/kaushikendluri/racestream.git
cd racestream
cp .env.example .env
make dev            # or: docker compose up --build
```

| Service | URL |
|---|---|
| Frontend | http://localhost:5173 |
| API + OpenAPI docs | http://localhost:8000/docs |
| Redpanda Console | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

Then load a session and replay it:

```bash
make ingest SESSION=9158     # pull a historical session into TimescaleDB
make replay SESSION=9158     # stream it through Redpanda at 1x
```

## Build status

This project is built in phases, and this table is kept honest — a row is only ticked once the
feature runs and has been verified against a live stack.

| # | Phase | Status |
|---|---|---|
| 1 | Repo, Compose, TimescaleDB, FastAPI health, React shell, Redpanda | in progress |
| 2 | OpenF1 ingestion, schemas, validation | not started |
| 3 | Redpanda producer/consumer, topics, consumer groups | not started |
| 4 | Stream processor, derived metrics, persistence | not started |
| 5 | WebSocket service, real-time frontend updates | not started |
| 6 | Dashboard, timing tower, track map, telemetry charts | not started |
| 7 | Replay engine, speed control, seek, event markers | not started |
| 8 | Prometheus, Grafana, latency + throughput instrumentation | not started |
| 9 | Failure simulation and recovery verification | not started |
| 10 | Load testing and benchmarking | not started |
| 11 | Unit, integration, end-to-end and chaos tests | not started |
| 12 | Documentation and demo | not started |

## Data source and attribution

Telemetry comes from [OpenF1](https://openf1.org), a free and open API providing real-time and
historical Formula 1 data. RaceStream is not affiliated with OpenF1.

F1, FORMULA 1, and related marks are trademarks of Formula One Licensing BV. This project uses
no F1 trademarks or branding and claims no association.

## License

[MIT](LICENSE)
