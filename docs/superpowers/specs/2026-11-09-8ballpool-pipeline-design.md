# 8 Ball Pool Real-Time Event Pipeline — Design

Date: 2026-11-09
Status: complete, awaiting the candidate's approval

## 1. Intent and why

This is the take-home assessment for a **Senior Data Engineer** interview at Miniclip.
The candidate will submit the code and then **defend it in an interview**, so every part
must be something they can explain line by line. Reviewers will judge correctness,
design judgement, code quality (the task says "all code should be commented"),
and how easy it is to run ("self-contained").

The task (from `DE Task.pdf`):

- The 8 Ball Pool game server emits three event types: `init` (app opened, always the
  first event for a user), `match` (sent at the end of a 2-player match) and
  `in-app-purchase`. Event shapes are fixed by three JSON Schemas (draft-03) supplied
  with the task: `schemas/init.json`, `schemas/match.json`, `schemas/in-app-purchase.json`.
- **Beginner:** run Kafka; produce real-time init/match/in-app events to a topic
  (auto-created topics are fine). Spark batch job: **daily number of distinct users by
  country and platform**.
- **Semi-pro:** a real-time Data Quality component with a set of rules that transform
  columns. Required transforms: **uppercase a field** (e.g. platform) and **map an ID to a
  name** (e.g. country_id → country_name). Must be **generic / extensible** to new fields
  and new transforms. Suggested: read from a Kafka topic, write to another Kafka topic.
- **Pro:** Spark Streaming real-time aggregator, **per minute**: count of purchases, sum
  of revenue, number of distinct users, revenue by country (country already transformed),
  number of matches by country (country already transformed).
- Language is free; we chose **Python**. Must be self-contained; Docker Compose is one of
  the suggested options.

## 2. Goals, non-goals, anti-goals

**Goals**
- All three levels implemented and runnable with one command (`docker compose up`).
- Clean, commented, tested Python code that a reviewer can read in one sitting.
- A README that explains how to run, the architecture, every assumption about the
  ambiguous spec, and known limitations / what would change in production.

**Non-goals**
- Schema Registry / Avro / Protobuf (events stay JSON, validated by the given schemas).
- Multi-broker HA, security (TLS/SASL), cloud deployment, Kubernetes.
- Dashboards (Grafana/Metabase). Results are queryable in Postgres; that is enough.

**Anti-goals** (would count as failure even if it "works")
- Clever code the candidate cannot explain in the interview.
- Anything that needs manual steps beyond installing Docker to see results.
- Silently dropping bad data: invalid events must be visible (dead-letter topic + counts).

## 3. Constraints

- Candidate's machine: Windows 11, Python 3.12/3.13, git. **No Java, no Docker, no WSL
  yet.** The candidate will install Docker Desktop before integration work. Anything
  needing a JVM (Kafka, Spark, PySpark tests) runs inside containers.
- Pure-Python logic (validation, rules engine, enrichment, generator) must be unit-testable
  locally **without** Kafka or Spark.
- Library versions current at time of writing: `confluent-kafka` 2.16.x, `pyspark` 4.x,
  `jsonschema` 4.26.x (has `Draft3Validator`).

## 4. Approaches considered

| | Approach | Trade-off | Decision |
|---|---|---|---|
| A | DQ service on **Quix Streams** (Python Kafka-Streams-like lib with RocksDB state + changelog topics) | Closest analogue to Kafka Streams, state handled for us; but a less-known library the candidate must explain | Rejected |
| B | DQ service as a hand-written **consume-transform-produce loop on `confluent-kafka`**, state in a compacted Kafka topic | More code, we own state + offset handling; every line explainable, shows Kafka fundamentals | **Chosen by the candidate** |
| C | Do DQ inside Spark as well | Ignores the "Kafka → Kafka" suggestion; stream-stream join of init with later events needs unbounded state | Rejected |

## 5. Architecture

```
generator ──► raw-events ──► dq-service ──► clean-events ──► spark-streaming (minute) ──► Postgres
                                 │  ▲                   └──► spark-batch (daily)     ──► Postgres
                                 │  └── user-profiles (compacted: user-id → country, platform)
                                 └──► dead-letter (invalid events + reason)
```

Containers (Docker Compose): `kafka` (single broker, KRaft, official `apache/kafka` image),
`postgres`, `generator`, `dq-service`, `spark-streaming`, `spark-batch` (run on demand),
and a `tests` service that runs the full pytest suite (including Spark tests) in a JVM
container.

### 5.1 Generator (producer)

- Simulates a population of users. Each user's **first** event is always `init`, then a
  random mix of `match` and `in-app-purchase` events. Configurable event rate and user
  count via env vars.
- `init.country` is emitted as an **ISO-3166 alpha-2 code** (e.g. `TR`, `GB`) and
  `platform` in lowercase/mixed case (e.g. `android`, `iOS`) so the DQ transforms have
  visible effect. (The schema has a free-text `country` and no `country_id`; we interpret
  the "map ID to name" rule as "country code → country name".)
- `time` = current epoch **seconds** (schema says integer ≥ 0; unit unspecified).
- A configurable fraction (default ~1%) of deliberately invalid events (missing field,
  negative value, wrong type) to exercise the dead-letter path.
- Messages keyed by `user-id` (for `match`: `user-a`) so a user's events stay ordered
  within a partition. All event types go to **one topic** `raw-events` (the task says
  "send them to a topic").

### 5.2 DQ service (semi-pro) — the core of the design

A Python service using `confluent-kafka`. Per message:

1. **Parse** JSON. Unparseable → dead-letter.
2. **Validate** against the matching JSON Schema (`event-type` selects the schema) with
   `jsonschema.Draft3Validator`. Invalid → dead-letter with the validation error(s).
3. **Transform** with the rules engine (5.3).
4. **Enrich** with user profile state (5.4).
5. **Produce** to `clean-events`.

**Delivery semantics: exactly-once** using Kafka transactions: transactional producer,
`send_offsets_to_transaction`, commit per batch; consumers downstream use
`isolation.level=read_committed`. Rationale: the minute aggregator sums revenue and counts
purchases, so duplicates would corrupt reports.

Exactly-once is required, not optional: events carry no unique ID (and the schemas forbid
adding one via `additionalProperties: false`), so downstream deduplication is impossible and
at-least-once would double count revenue. State consistency on abort: profile updates made
inside a transaction are buffered and applied to the in-memory map **only after the
transaction commits**; on abort they are discarded and the batch is re-consumed from the last
committed offsets.

**Partitioning and scaling (Claude's call):** `raw-events` has **1 partition** and the DQ
service runs as **1 instance** (Kafka auto-create defaults, which the task says are enough).
This guarantees `init` is processed before any later event of the same user, including
`user-b` of a match. The README documents the scale-out path: N partitions, N instances, each
instance continuously tailing `user-profiles` in a background thread (true GlobalKTable
semantics), and a retry/delay topic for events whose user profile has not arrived yet.

The service is written so the processing core (`validate → transform → enrich`) is a pure
function over a dict plus a state object; the Kafka loop is a thin shell around it. This is
what makes local unit tests possible.

### 5.3 Rules engine (generic, extensible)

Rules live in a YAML file, not in code. Each rule = event type(s) + field path + transform
name + optional params. Field paths use dot notation for nested fields
(`user-a-postmatch-info.platform`). Example:

```yaml
rules:
  - events: [init]
    field: platform
    transform: upper
  - events: [match]
    field: user-a-postmatch-info.platform
    transform: upper
  - events: [match]
    field: user-b-postmatch-info.platform   # optional object; rule skipped if absent
    transform: upper
  - events: [init]
    field: country
    transform: map
    params: { mapping_file: country_codes.csv, on_missing: keep }   # keep | null | "UNKNOWN"
```

Transforms are plain functions registered in a registry (`@register("upper")`). Adding a
transform = one decorated function; adding a field = one YAML entry. Shipped transforms:
`upper`, `lower`, `trim`, `map`. The country mapping is a static CSV checked into the repo
(no extra dependency). Rules are validated at startup (unknown transform name or bad params
→ fail fast).

### 5.4 Enrichment state (user profiles)

`in-app-purchase` and `match` events carry no country, but the Pro aggregations need
country. The DQ service keeps a **user-id → {country, platform}** map built from `init`
events (after transformation):

- On every `init`, the map is updated and the profile is written to the **compacted topic
  `user-profiles`** (key = user-id), inside the same transaction.
- On startup the service reads `user-profiles` from the beginning to rebuild the map before
  processing `raw-events` (same idea as a Kafka Streams GlobalKTable).
- Enrichment adds fields to the clean event:
  - `in-app-purchase` → `country`, `platform` of the buyer.
  - `match` → `user-a-country`, `user-b-country`, and `user-a-platform`, `user-b-platform`
    (amended 2026-11-09 after live data: post-match platform if present, else the
    participant's profile platform, else `UNKNOWN` — ~10% of matches omit the optional
    `user-b-postmatch-info`, which otherwise inflated the daily `UNKNOWN` platform bucket).
- Unknown user (no init seen) → country `UNKNOWN`; counted and logged, not dropped.
- **Clean events are a new contract (Claude's call).** The supplied schemas describe what the
  game server sends (`raw-events`); enriched events intentionally carry extra fields. We ship
  `schemas/clean/*.json` describing the `clean-events` format, and tests check DQ output
  against them.
- `user-id` is normalised to string everywhere (schemas allow string or integer in init/IAP
  but only string in match).

### 5.5 Spark Streaming minute aggregator (pro)

PySpark Structured Streaming reading `clean-events` (`read_committed`). Event-time =
`time`, tumbling **1-minute** windows, **2-minute watermark** for late data. Per minute:

| Metric | Definition (our interpretation) |
|---|---|
| purchases_count | number of `in-app-purchase` events |
| revenue_sum | sum of `purchase_value` |
| distinct_users | distinct users with **any** event in that minute (both players of a match count) |
| revenue_by_country | sum of `purchase_value` grouped by (enriched, transformed) country |
| matches_by_country | number of matches grouped by participant country; a match counts **once per distinct participant country** (TR vs GB → +1 TR, +1 GB; TR vs TR → +1 TR) |

**Live and exact (candidate's decision).** All three queries run in **update output mode**,
so a minute's rows appear after the first micro-batch and are refined on every trigger until
the watermark closes the window; the final value equals what append mode would produce.

Distinct users are computed exactly with `F.size(F.collect_set("user_id"))`.
`countDistinct` is rejected on streams (`UnsupportedOperationChecker`: "Distinct
aggregations are not supported on streaming DataFrames/Datasets"), but `collect_set` is a
plain aggregate; Spark's own `StreamingAggregationSuite` test `"count distinct"` uses exactly
`size(collect_set(...))` under `Update` mode (verified in Spark master source, 2026-11-09).
Cost, to state in the README: state holds the set of user IDs for each open window (~3-4
windows), fine at this scale; at millions of users/minute we would switch to HLL
(`approx_count_distinct` / `hll_sketch_agg`). **First integration test must confirm this runs
on the pinned Spark 4.x image**; if it does not, fall back to append mode with chained
aggregations (exact, ~3 min latency) and record why.

Rejected alternatives (from research, for the README): chained `groupBy` (append-only, ~3
min latency), `approx_count_distinct` (not exact: ~4.6% error at default rsd), a
(minute, user) pairs table in Postgres (moves the distinct logic out of Spark),
`transformWithStateInPandas` (exact and live but ~60 lines + RocksDB; too clever here).

Results are written to Postgres via `foreachBatch` with `INSERT ... ON CONFLICT (...) DO
UPDATE SET ... = EXCLUDED....` keyed on window start (and country). Update mode emits full
current values, not deltas, so replays of a batch rewrite the same values: idempotent without
a batchId ledger. Writes use psycopg (Spark's JDBC writer has no upsert). Tables:
`minute_summary`, `minute_revenue_by_country`, `minute_matches_by_country`, each one streaming
query with its own checkpoint.

README "what I'd do in production" note: distinct counts cannot be summed across minutes;
storing mergeable `hll_sketch_agg` sketches would allow hourly/daily distinct roll-ups.

### 5.6 Spark batch daily aggregator (beginner)

PySpark batch job. To keep "one command" true, the `spark-batch` service runs in a loop
under `docker compose up`, recomputing **today (UTC)** every 5 minutes (idempotent, so
re-running over a partial day is safe; the row for a day is final once the day has passed).
It can also be run on demand for a given day (`docker compose run spark-batch --date
2026-11-09`). Reads `clean-events` from Kafka in batch mode (earliest → latest),
filters to the day (UTC, by event `time`), explodes matches into their two participants,
and computes **distinct users per (day, country, platform)**. Country/platform come from the
enriched/transformed fields (for match participants: `user-X-country` and `user-X-platform`
from enrichment, see 5.4). Writes to Postgres table `daily_users_by_country_platform`, replacing
that day's rows (idempotent re-runs).

### 5.7 Testing

- **Unit (local, no JVM):** schema validation, every transform, rules loading/validation,
  enrichment + unknown-user path, generator invariants (init first; valid events validate).
- **Spark logic:** aggregation functions take/return DataFrames and are tested with a local
  SparkSession on small fixed inputs (runs in the `tests` container).
- **End-to-end smoke:** compose up, generator runs for ~2 minutes, assert rows appear in the
  Postgres tables and dead-letter is non-empty.
- Approach: test-first for the pure logic.

### 5.8 Repo layout (indicative)

```
docker-compose.yml   README.md   Makefile (or run scripts)
config/rules.yaml    config/country_codes.csv   schemas/*.json
src/pipeline/{generator,dq,spark}/...           tests/...
docs/superpowers/specs/...
```

## 6. Decisions and who made them

**Made by the candidate**
- Language: Python.
- DQ approach: B, hand-written `confluent-kafka` consume-transform-produce.
- Minute metrics: live (update mode) **and** exact via `size(collect_set(user_id))`.

**Proposed by Claude (candidate can override)**
- Docker Compose as the self-contained packaging; Postgres as the report sink.
- One `raw-events` topic; `clean-events`, `user-profiles` (compacted), `dead-letter`.
- Exactly-once via Kafka transactions; profile updates applied to memory only after commit.
- `raw-events` = 1 partition, 1 DQ instance; scale-out path documented in README.
- `spark-batch` loops every 5 min over today under `docker compose up`; also runnable per date.
- `clean-events` gets its own schemas (`schemas/clean/*.json`).
- Corrected copy of `match.json` (`game-tier` 1..5) used for validation; original kept.
- Country as ISO code → name via static CSV; platform uppercased.
- `time` in epoch seconds; 1-min tumbling windows, 2-min watermark; UTC days.
- Metric interpretations in 5.5 (distinct users = any event; match counted per distinct
  participant country).
- Unknown user → `UNKNOWN` country rather than dropping or buffering.
- Code, comments and README in **English** (it is submitted to Miniclip).

## 7. Known schema issues (to document in README as assumptions)

- `match.json` `game-tier` declares `"minimum"` twice (`1` and `5`); JSON parsers keep the
  last one, so as written the schema requires `game-tier >= 5`. Intended meaning is almost
  certainly `1 ≤ tier ≤ 5`. We ship the original schemas untouched in `schemas/` and a
  corrected copy used for validation, and say so.
- `user-id` is string-or-integer in init/IAP, string-only in match → normalised to string.
- `user-b-postmatch-info` is optional; rules and enrichment must tolerate its absence.
- No `country_id` exists in the schemas; see 5.1.

## 8. Left to the builder

- Exact module/function names, CLI flags, env var names.
- Transaction batch size / commit interval for the DQ service.
- How Spark gets the Kafka connector (baked into the image vs `--packages`), as long as
  `docker compose up` works offline after the first build.
- Postgres schema details (column types, indexes), within the tables named above.
- Whether to use a Makefile or small shell/PowerShell scripts for convenience commands.
