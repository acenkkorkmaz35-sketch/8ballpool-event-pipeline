# 8 Ball Pool real-time event pipeline

Kafka -> real-time data-quality service -> Spark (streaming per minute, batch per day) ->
Postgres, for the three 8 Ball Pool event types (`init`, `match`, `in-app-purchase`).
All three assessment levels are implemented, written in Python, and start with one command.

```
generator ──► raw-events ──► dq-service ──► clean-events ──► spark-streaming (per minute) ──► Postgres
                                 │  ▲                   └──► spark-batch (per day)        ──► Postgres
                                 │  └── user-profiles (compacted: user-id -> country, platform)
                                 └──► dead-letter (invalid events + reasons)
```

## Where each assessment level lives

| Level | Component | Main files | Output |
|---|---|---|---|
| Beginner | Kafka (KRaft, single broker) + event generator | `docker-compose.yml`, `docker/init-topics.sh`, `src/pipeline/generator/` | topic `raw-events` |
| Beginner | Spark batch: daily distinct users by country and platform | `src/pipeline/spark/daily_job.py`, `aggregations.py` (`daily_users`), `sink.py` (`replace_day`) | table `daily_users_by_country_platform` |
| Semi-Pro | Real-time DQ service: validate, YAML rules (upper / map ...), enrich | `src/pipeline/dq/`, `config/rules.yaml`, `config/country_codes.csv`, `schemas/raw/` | topics `clean-events`, `dead-letter`, `user-profiles` |
| Pro | Spark Structured Streaming minute aggregator | `src/pipeline/spark/streaming_job.py`, `aggregations.py`, `parsing.py`, `sink.py` | tables `minute_summary`, `minute_revenue_by_country`, `minute_matches_by_country` |

## Quick start

Requirements: Docker Desktop (on Windows with the WSL2 backend) and about 6 GB of RAM for
the stack. Nothing else is needed on the host; Java, Spark and Kafka run in containers.

```bash
docker compose up -d --build
```

The first build takes a few minutes (JDK, pyspark and the Kafka connector are downloaded
once; the connector jars are baked into the image, so containers need no network at
runtime). After the containers start, rows appear within about a minute: the generator
emits ~50 events/s, the minute tables are upserted every 10 s, and the daily job runs
immediately and then every 5 minutes.

Query the results (Postgres is also reachable from the host on `127.0.0.1:5433`, user,
password and database all `pipeline`):

```bash
# Per minute: purchases, revenue, exact distinct users
docker compose exec postgres psql -U pipeline -d pipeline -c "SELECT window_start, purchases_count, revenue_sum, distinct_users FROM minute_summary ORDER BY window_start DESC LIMIT 5"

# Per minute: revenue by country
docker compose exec postgres psql -U pipeline -d pipeline -c "SELECT window_start, country, revenue_sum FROM minute_revenue_by_country ORDER BY window_start DESC, revenue_sum DESC LIMIT 10"

# Per minute: matches by country
docker compose exec postgres psql -U pipeline -d pipeline -c "SELECT window_start, country, matches_count FROM minute_matches_by_country ORDER BY window_start DESC, matches_count DESC LIMIT 10"

# Per day: distinct users by country and platform
docker compose exec postgres psql -U pipeline -d pipeline -c "SELECT day, country, platform, distinct_users FROM daily_users_by_country_platform ORDER BY day DESC, distinct_users DESC LIMIT 10"
```

Look at the Kafka side (dead-letter records carry the reasons and the original payload):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dead-letter --from-beginning --max-messages 3
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clean-events --isolation-level read_committed --from-beginning --max-messages 3
docker compose logs -f dq-service   # one line per batch: consumed / clean / invalid / unknown_user / profiles
```

In Git Bash on Windows, run `export MSYS_NO_PATHCONV=1` first; otherwise Git Bash rewrites
`/opt/kafka/...` into a Windows path. PowerShell and Linux/macOS shells need nothing.

Run the daily job for a specific UTC day, then exit:

```bash
docker compose run --rm spark-batch --date 2026-11-09
```

If that day computes to zero rows the stored rows are left untouched (the day may simply be
gone from Kafka after retention, and deleting would wipe good data); add `--allow-empty` to
a single run to clear the day on purpose. Loop mode never clears: it skips an empty day with
a log line.

Stop and delete everything (topics, tables, checkpoints): `docker compose down -v`.

### Services

| Service | Image | Role |
|---|---|---|
| `kafka` | `apache/kafka:4.3.2` | single node, KRaft (broker + controller), host listener `127.0.0.1:29092` |
| `init-topics` | `apache/kafka:4.3.2` | one-shot: creates the four topics (1 partition each; `user-profiles` compacted) |
| `generator` | `pipeline-app:dev` (`python:3.12.16-slim`) | simulated game server; env `EVENTS_PER_SECOND=50`, `INVALID_RATE=0.01`, `SEED=42` |
| `dq-service` | `pipeline-app:dev` | exactly-once consume-transform-produce loop |
| `postgres` | `postgres:16.15` | report tables, created by `sql/init.sql`; host port `127.0.0.1:5433` |
| `spark-streaming` | `pipeline-spark:dev` | minute aggregator (3 streaming queries) |
| `spark-batch` | `pipeline-spark:dev` | daily job, `--loop` every `DAILY_LOOP_SECONDS` (default 300) |
| `tests` | `pipeline-spark:dev` | test runner, `test` profile only (not started by `up`) |

`pipeline-spark:dev` is `python:3.12.16-slim` + OpenJDK 21 JRE + `pyspark==4.3.0`, with the
`spark-sql-kafka` connector and its dependencies copied into pyspark's `jars/` at build time
(`docker/bake_kafka_connector.py`). The official `apache/spark` 4.3.0 Python image ships
Python 3.10; this package requires 3.12. Host ports are bound to loopback only. Spark runs in
local mode (`local[2]`, 1 GB driver) with container limits of 2 GB / 1.5 GB.

Configuration is by environment variable (`src/pipeline/settings.py`):
`KAFKA_BOOTSTRAP_SERVERS`, `RAW_TOPIC`, `CLEAN_TOPIC`, `PROFILES_TOPIC`, `DEAD_LETTER_TOPIC`,
`EVENTS_PER_SECOND`, `INVALID_RATE`, `SEED`, `MAX_USERS` (generator user population cap,
default 1000), `POSTGRES_DSN`, `RULES_PATH`, `SCHEMAS_DIR`. `DAILY_LOOP_SECONDS` (default
300) is read by the daily job directly, not by `Settings`.

## Tests

| Suite | What it covers | Where it runs |
|---|---|---|
| `tests/unit` | schemas, rules engine, transforms, profile store, enrichment, DQ processor, simulator, settings, SQL builders, daily-job loop | locally, no JVM or broker |
| `tests/kafka_unit` | the DQ service's exactly-once control flow (commit / abort / rewind ordering) with fake Producer, Consumer and store | `tests` container |
| `tests/spark` | parsing, minute and daily aggregations, update mode with `collect_set` (incl. out-of-order and too-late events), connector baked in | `tests` container (local SparkSession) |
| `tests/integration` | DQ service against the real broker; minute tables filling; Postgres upsert / replace-day; daily job end to end | `tests` container, stack must be up |

```bash
# Locally (Python 3.12). Calling the venv's interpreter needs no activation:
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dq,dev]"     # Linux/macOS: .venv/bin/python
.venv/Scripts/python -m pytest tests/unit -q
# Or activate it first: Git Bash `source .venv/Scripts/activate`; PowerShell
# `.venv\Scripts\Activate.ps1` (may need `Set-ExecutionPolicy -Scope Process Bypass`);
# Linux/macOS `source .venv/bin/activate`.

# In the container (same image as the Spark jobs)
docker compose --profile test run --rm tests                                  # unit
docker compose --profile test run --rm tests pytest tests/kafka_unit -v
docker compose --profile test run --rm tests pytest tests/spark -v
docker compose --profile test run --rm tests pytest tests/integration -v      # after `up -d --build`
docker compose --profile test run --rm tests pytest -v                        # everything
```

The integration tests poll (no fixed sleeps) for up to 4 minutes for data to arrive. The
daily-job integration test starts its own Spark JVM inside the `tests` container; on a
machine with little memory, `docker compose stop spark-batch` first.

## Design decisions

### Exactly-once in the DQ service (`src/pipeline/dq/service.py`)

The minute aggregator sums revenue and counts purchases. Events carry no unique ID and the
schemas forbid adding one (`additionalProperties: false`), so a duplicate can never be
recognised downstream: at-least-once would silently double count revenue. The service
therefore uses Kafka transactions:

1. Consume up to 500 messages; `begin_transaction()`.
2. Each message goes through the pure core `process()` (parse -> validate -> plausibility ->
   rules -> enrich)
   and is produced to `clean-events` or `dead-letter`; profiles from `init` events are
   produced to `user-profiles`.
3. `send_offsets_to_transaction()` commits the consumed offsets *inside* the transaction,
   then `commit_transaction()`. Outputs and input progress become visible atomically.
4. All consumers (the DQ service itself, both Spark jobs, the profile bootstrap) read with
   `isolation.level=read_committed`, so aborted records are invisible.

Failure handling: a non-fatal Kafka error (or `BufferError`, a full local producer queue)
aborts the transaction, discards the staged profiles and seeks the consumer back to the
committed offsets, so the batch is re-read. Retriable commit/abort errors are first retried
in place (`_retry_if_retriable`). A fatal Kafka error, or any unexpected exception (a bug), aborts the transaction best-effort
and exits the process non-zero; compose (`restart: on-failure`) restarts it.
`init_transactions()` with the fixed `transactional.id` fences any zombie instance. The
control flow is covered by `tests/kafka_unit/test_service_control_flow.py`.

Dead-letter records are bounded: `raw` is cut to 10,000 characters with `truncated` and
`raw_size_bytes` fields, and the error list is capped (20 entries of at most 500
characters). Without that, one huge invalid message would produce a dead-letter record
above Kafka's message size limit, the transaction would fail forever and the loop would
wedge (found and fixed during testing).

**Schema validity is not semantic validity.** After the schema check, `process()` rejects
three kinds of schema-valid but implausible values (they go to `dead-letter` with a clear
reason, and an `init` that fails them stages no profile):

- *Non-finite numbers* anywhere in the event (`1e400` parses to `inf`): `inf`/`nan` cannot be
  represented in JSON or in Postgres.
- *`time` more than 300 s in the future* (`MAX_FUTURE_SKEW_SECONDS`): catches millisecond
  timestamps and skewed client clocks. The Spark event-time watermark is the maximum event
  time seen and lives in the checkpoint, so one far-future event would make every later real
  event "late" and silently dropped, for good. There is no lower bound: old events are
  harmless (Spark drops them as late).
- *`purchase_value` above 10,000* (`MAX_PURCHASE_VALUE`): the column is `numeric(14,2)`; an
  overflow makes the sink write fail, and a failing micro-batch is replayed forever (a
  poison batch).

### Delivery semantics per hop

"Exactly-once" is not a property of the whole chain; it differs per hop:

| Hop | Mechanism | Guarantee |
|---|---|---|
| generator -> `raw-events` | idempotent producer (`enable.idempotence`) | no duplicates from producer retries; not transactional, so exactly-once starts at `raw-events` |
| `raw-events` -> `clean-events` / `user-profiles` / `dead-letter` | Kafka transactions (outputs + consumed offsets commit atomically, `read_committed` readers) | exactly-once |
| `clean-events` -> Postgres | at-least-once `foreachBatch` + idempotent full-value upserts (minute tables) / day replace (daily table) | effectively-once: a replayed micro-batch rewrites the same values |

### Profile store: a GlobalKTable-like map (`profiles.py`, `enrich.py`)

Purchases and matches carry no country, yet the Pro metrics need one. The service keeps
`user-id -> {country, platform}` in memory, built from transformed `init` events:

- Each `init` stages its profile; the profile is also written to the **compacted** topic
  `user-profiles` in the same transaction. Staged profiles are visible to later events in
  the same batch, and become committed only after `commit_transaction()` succeeds; on abort
  they are dropped. Memory never runs ahead of what Kafka durably holds.
- On startup the service re-reads `user-profiles` from the beginning (`read_committed`)
  before consuming `raw-events`, the same idea as a Kafka Streams GlobalKTable. A restart
  therefore loses no enrichment state.
- Enrichment: purchases get `country`, `platform`; matches get `user-a-country`,
  `user-b-country`, `user-a-platform`, `user-b-platform`. A participant's platform is the
  post-match info platform if present, else the profile platform, else `UNKNOWN`. (This was
  amended after live data: ~10% of matches omit the optional `user-b-postmatch-info`, which
  made `UNKNOWN` the largest platform bucket in the daily table; it is now under 1%.)
- A user with no known profile gets country `UNKNOWN`; the event is kept and counted
  (`unknown_user` in the batch log), not dropped.

### Rules engine (`rules.py`, `transforms.py`, `config/rules.yaml`)

Rules are data, not code: each rule names event types, a dot-notation field path, a
registered transform and optional params. A rule is skipped when the field is absent (for
example the optional `user-b-postmatch-info`). The whole file is validated at startup
(unknown transform, bad params, missing mapping file -> the service refuses to start).
Shipped transforms: `upper`, `lower`, `trim`, `map` (`code,name` CSV, `on_missing`:
`keep` (default) | `null` (the string, or an unquoted YAML `null`) | any other string as a
literal replacement). Validation uses the supplied draft-03 schemas with
`jsonschema.Draft3Validator`; the event's `event-type` selects the schema.

### Minute aggregator: live, exact, idempotent (`streaming_job.py`, `aggregations.py`)

- Event time = `time` (epoch seconds), 1-minute tumbling windows, 2-minute watermark, 10 s
  trigger. Three queries, one per table, each with its own checkpoint in the
  `spark-checkpoints` volume.
- **Update output mode**: a minute's row appears after the first trigger and is refined on
  every trigger until the watermark closes the window. The final value equals what append
  mode would produce, without waiting for the window to close.
- **Exact distinct users** with `F.size(F.collect_set("user_id"))`. `countDistinct` is
  rejected on streaming DataFrames ("Distinct aggregations are not supported"), but
  `collect_set` is an ordinary aggregate; Spark's own `StreamingAggregationSuite` test
  "count distinct" uses this pattern in update mode. `tests/spark/test_streaming_update_mode.py`
  confirms it on the pinned Spark 4.3.0: a refined minute shows the exact count (not a
  delta), an out-of-order event inside the watermark refines its minute, and a too-late
  event is dropped. Cost: state holds the set of user IDs of each open window (about 3-4
  windows); fine at this scale (the generator caps the population at 1,000 users).
- **Sink**: `foreachBatch` -> psycopg `INSERT ... ON CONFLICT (key) DO UPDATE` in one
  transaction per micro-batch. Update mode emits full current values, not deltas, so a
  replayed micro-batch rewrites the same values: idempotent without a batch-id ledger. The
  hop is at-least-once delivery made effectively-once by the idempotent write (see the
  delivery semantics table above). (Spark's JDBC writer has no upsert, hence psycopg.)

Rejected alternatives:

| Alternative | Why not |
|---|---|
| Chained aggregations in append mode | exact, but a minute only appears after the watermark passes (~3 min latency) |
| `approx_count_distinct` | not exact (~4.6% error at the default `rsd`) |
| (minute, user) pairs table in Postgres | moves the distinct logic out of Spark into the sink |
| `transformWithState` | exact and live, but ~60 lines plus RocksDB state; too much machinery for this |

### Daily job (`daily_job.py`)

Reads the whole `clean-events` topic in batch mode (`read_committed`), expands matches into
both participants, keeps one UTC day by event time and computes `countDistinct(user_id)`
per (country, platform); `countDistinct` is fine in batch. The write is DELETE that day +
INSERT in one transaction, behind a `pg_advisory_xact_lock` keyed by (table, day), so
re-runs are idempotent and concurrent runs for the same day serialise. Under compose it loops
every 300 s over today (UTC) to keep "one command" true; when the UTC date changes it
recomputes the finished day once more (retried until it succeeds) so that day is complete.

### Other choices

- Python throughout; the DQ core is a pure function over a dict plus the store, so all of
  its logic is unit-tested without Kafka.
- Hand-written `confluent-kafka` loop instead of a streams library (Quix Streams): more code,
  but every line is explainable. DQ inside Spark was rejected: joining `init` with later
  events would be a stream-stream join with unbounded state.
- One `raw-events` topic for all event types, keyed by `user-id` (`user-a` for matches).
- Postgres as the report sink: results are queryable with plain SQL, no dashboard needed.

## Assumptions

- **Schema bug:** `match.json` declares `"minimum"` twice for `game-tier` (1 and 5); JSON
  parsers keep the last, so as written it requires `game-tier >= 5`. The intent is clearly
  1 <= tier <= 5. The supplied files are kept untouched in `schemas/original/`; validation
  uses `schemas/raw/`, where only that line is corrected (`"maximum": 5`) and a description
  records why.
- **`user-id`** is string-or-integer in `init` / `in-app-purchase` but string-only in
  `match`; it is normalised to a string everywhere.
- **`user-b-postmatch-info`** is optional in the schema. Rules skip it when absent, and
  user-b's platform then falls back to the platform from their profile (`init`), else
  `UNKNOWN`.
- **`time`** is epoch **seconds** (the schemas only say integer >= 0). Days and minute
  windows are UTC.
- **"Map an ID to a name":** the schemas have no `country_id`; `init.country` is treated as
  an ISO 3166-1 alpha-2 code and mapped to the country name with `config/country_codes.csv`
  (unknown codes are kept as-is). Platform is uppercased (`android`, `iOS` -> `ANDROID`, `IOS`).
- **Clean events are a new contract:** enriched events carry fields the supplied schemas
  forbid, so `schemas/clean/*.json` describes `clean-events`, and tests check DQ output
  against it.
- **Metric definitions** (per minute, by event time):
  - `purchases_count` / `revenue_sum`: `in-app-purchase` events and their `purchase_value`
    (no currency is given; values are summed as-is). `revenue_sum` is 0 for a minute without
    purchases.
  - `distinct_users`: users with **any** event in the minute; both players of a match count.
  - revenue by country: the buyer's (enriched, mapped) country.
  - matches by country: a match counts once per distinct participant country (TR vs GB ->
    TR +1, GB +1; TR vs TR -> TR +1).
  - daily users by country and platform: users with any event that day; a user seen with two
    platforms counts once in each group.
- **UNKNOWN policy:** an event whose user has no `init` yet (or whose `init` was invalid) is
  kept with country `UNKNOWN` rather than dropped or buffered; the generator deliberately
  corrupts some `init` events to exercise this path.
- **Invalid events** (non-JSON, unknown `event-type`, schema violations) go to `dead-letter`
  with the reasons; nothing is dropped silently. The generator corrupts ~1% of events.
- **Partitions:** all topics have one partition and the DQ service runs as one instance.
  This guarantees a user's `init` is processed before their later events, including as
  `user-b` of a match. See the scale-out path below.

## Extending the rules engine

A new field is one YAML entry in `config/rules.yaml`:

```yaml
  - events: [in-app-purchase]
    field: product-id
    transform: lower
```

A new transform is one decorated factory in `src/pipeline/dq/transforms.py`. The factory
receives the rule's `params` and the rules file's directory, validates them once at load
time and returns the per-value function:

```python
@register("round")
def _round_factory(params: dict, base_dir: Path) -> Callable[[Any], Any]:
    digits = params.get("digits", 2)
    if not isinstance(digits, int):
        raise ValueError("round param 'digits' must be an integer")
    return lambda v: round(v, digits) if isinstance(v, (int, float)) else v
```

Then `transform: round` with `params: { digits: 1 }` can be used in any rule. Rules apply
to the raw event before enrichment, so a transformed `init.country` / `platform` is what
ends up in the profile store and in every enriched purchase and match.

## Limitations and production next steps

- **Scale-out.** One partition and one DQ instance cap throughput. The path: N partitions
  keyed by user, N DQ instances, each tailing `user-profiles` continuously in a background
  thread (true GlobalKTable semantics, since a match's `user-b` may live on another
  partition), plus a retry/delay topic for events whose user's profile has not arrived yet
  instead of labelling them `UNKNOWN` immediately.
- **Tombstones** on `user-profiles` are skipped at bootstrap, not treated as deletes.
- **Distinct counts cannot be summed.** Hourly or daily distinct users cannot be derived
  from the minute table. Storing mergeable `hll_sketch_agg` sketches per minute would allow
  roll-ups (approximate).
- **State size.** `collect_set` keeps every user ID of each open window; at millions of
  users per minute switch to HLL (`approx_count_distinct` / sketches) and accept the error.
- **Daily job re-reads the whole topic** on every run. At scale it would read a lake table
  partitioned by date (or the minute stream would persist per-day sketches).
- **Schemas:** JSON validated in the consumer. Production would use a Schema Registry with
  Avro or Protobuf and compatibility checks at produce time.
- **Monitoring:** only logs today. Alert on dead-letter rate, `unknown_user` rate, consumer
  lag on `raw-events` and `clean-events`, and streaming query progress / state size.
- **Poison micro-batches replay by design.** A deterministic failure inside the Postgres sink
  replays the same micro-batch forever: the job fails loudly rather than skipping data. The
  operator must fix the data or the code and restart. The DQ plausibility checks exist to keep
  such values out of `clean-events` in the first place.
- **Operations:** single broker with replication factor 1, no TLS/SASL, Spark in local
  mode. Fine for a self-contained demo, not for production.

## Repository layout

```
docker-compose.yml        the whole stack + the `tests` service (profile `test`)
docker/                   app and Spark Dockerfiles, topic creation, connector bake script
config/                   rules.yaml, country_codes.csv
schemas/original|raw|clean  supplied schemas, corrected validation copy, clean-events contract
sql/init.sql              the four Postgres tables
src/pipeline/constants.py shared constants (UNKNOWN label, log format)
src/pipeline/generator/   event simulator + paced Kafka producer
src/pipeline/dq/          processor (pure core), rules, transforms, enrichment, profile store, service (Kafka shell)
src/pipeline/spark/       parsing, aggregations, sink, streaming_job, daily_job
tests/                    unit, kafka_unit, spark, integration
docs/superpowers/         design spec and implementation plan
```

## Verified on

2026-11-09, clean slate on Windows 11 + Docker Desktop (WSL2, 8 CPU, ~7.4 GB):
`docker compose down -v`, `docker compose up -d --build`, about 7 minutes of running, then
`docker compose --profile test run --rm tests pytest -v`: **163 passed** (unit 127,
kafka_unit 5, spark 25, integration 6) in 113 s. A millisecond-timestamp purchase produced to
`raw-events` landed in `dead-letter` with the "in the future" reason, and `minute_summary`
kept receiving rows for the following minutes. Row counts after that (about 14 minutes of
running):

| Table | Rows |
|---|---|
| `minute_summary` | 15 |
| `minute_revenue_by_country` | 232 |
| `minute_matches_by_country` | 237 |
| `daily_users_by_country_platform` | 33 |

Platform `UNKNOWN` was 0.60% of the day's distinct users (sum over the daily table's rows).
