# 8 Ball Pool Real-Time Event Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A self-contained (Docker Compose) Python pipeline: event generator → Kafka → exactly-once DQ/enrichment service → Spark Structured Streaming minute aggregates + Spark daily batch → Postgres.

**Architecture:** Pure-Python core (schema validation, rules engine, profile store, enrichment, processor, simulator) is unit-tested locally with no JVM. Thin shells wrap it: a `confluent-kafka` transactional consume-transform-produce loop, and PySpark jobs whose aggregation logic lives in pure DataFrame functions tested in a Spark container.

**Tech Stack:** Python 3.12, confluent-kafka 2.16.x, jsonschema 4.26.x (`Draft3Validator`), PyYAML, PySpark 4.x (+ `spark-sql-kafka-0-10_2.13`), psycopg 3, PostgreSQL 16, Apache Kafka (KRaft, official `apache/kafka` image), pytest.

**Spec:** `docs/superpowers/specs/2026-11-09-8ballpool-pipeline-design.md` (read it before any task).

## Global Constraints

- All code, comments, docstrings, README in English. Every module has a module docstring; every public function/class has a docstring ("all code should be commented" is a task requirement).
- Topics: `raw-events` (1 partition), `clean-events`, `user-profiles` (cleanup.policy=compact), `dead-letter`.
- Postgres tables: `minute_summary`, `minute_revenue_by_country`, `minute_matches_by_country`, `daily_users_by_country_platform`.
- `time` = epoch **seconds**; windows 1 minute tumbling; watermark **2 minutes**; days in **UTC**.
- `user-id` / `user-a` / `user-b` are normalised to **string** in clean events.
- Unknown user → country `"UNKNOWN"`; missing platform in Spark outputs → `"UNKNOWN"`.
- Exactly-once in DQ: Kafka transactions; downstream consumers use `isolation.level=read_committed` (Spark option `kafka.isolation.level`).
- Minute queries: **update** output mode; distinct users via `F.size(F.collect_set("user_id"))`; Postgres writes are `INSERT ... ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col`.
- Original task schemas stay byte-for-byte untouched in `schemas/original/`.
- Pin every dependency and image tag to an exact version that exists (verify the tag before using it).
- Pure-Python tests run locally: `python -m pytest tests/unit -v`. JVM/Kafka/Postgres tests run in containers only.

## Review Focus

1. A purchase/match whose user's `init` is in the **same consumed batch** must be enriched with that init's profile (staged, not yet committed). → test in Task 3 and Task 4.
2. DQ crash/abort mid-batch must not leave half-applied profile state or duplicate clean events. → `ProfileStore.abort` test in Task 3; kill-and-restart check in Task 6.
3. `match` **without** `user-b-postmatch-info`: rules skip silently, enrichment still sets `user-b-country`, Spark platform for user-b = `"UNKNOWN"`. → tests in Tasks 2, 4, 7, 9.
4. Same user as integer `user-id` in init and string `user-b` in match must be one user. → tests in Tasks 3 and 5.
5. A late event inside the watermark must update an already-emitted minute row (update mode); a minute row must never be emitted twice with different keys. → streaming test in Task 7.

---

## File Structure

```
pyproject.toml                         deps (extras: dq, spark, dev), pytest config, src layout
config/rules.yaml                      DQ rules (spec 5.3)
config/country_codes.csv               code,name for all ISO-3166-1 alpha-2 codes
schemas/original/*.json                untouched task schemas
schemas/raw/*.json                     schemas used for validation (match.json: game-tier 1..5)
schemas/clean/*.json                   clean-events contract
sql/init.sql                           Postgres tables
src/pipeline/settings.py               env-driven settings dataclass
src/pipeline/schemas.py                SchemaRegistry
src/pipeline/dq/fieldpath.py           dot-path get/set helpers
src/pipeline/dq/transforms.py          transform registry + built-ins
src/pipeline/dq/rules.py               Rule, RuleSet, load_rules
src/pipeline/dq/profiles.py            Profile, ProfileStore
src/pipeline/dq/enrich.py              enrich()
src/pipeline/dq/processor.py           process() -> Clean | Invalid
src/pipeline/dq/service.py             Kafka transactional loop (entrypoint)
src/pipeline/generator/simulator.py    UserSimulator
src/pipeline/generator/main.py         producer loop (entrypoint)
src/pipeline/spark/parsing.py          parse_clean_events, to_participants, match_countries
src/pipeline/spark/aggregations.py     minute_summary, revenue_by_country, matches_by_country, daily_users
src/pipeline/spark/sink.py             build_upsert_sql, upsert_rows, replace_day
src/pipeline/spark/streaming_job.py    minute aggregator (entrypoint)
src/pipeline/spark/daily_job.py        daily batch (entrypoint, --date / --loop)
docker/app.Dockerfile                  python:3.12-slim image for generator + dq
docker/spark.Dockerfile                official Spark image + kafka connector jars baked in + psycopg
docker-compose.yml                     kafka, postgres, generator, dq-service, spark-streaming, spark-batch, tests
tests/unit/...                         local, no JVM
tests/spark/...                        run in `tests` container
tests/integration/...                  run against the compose stack
README.md
```

---

### Task 1: Project scaffold and schema registry

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `schemas/original/{init,match,in-app-purchase}.json` (copied verbatim from the task folder), `schemas/raw/{init,match,in-app-purchase}.json`, `src/pipeline/__init__.py`, `src/pipeline/schemas.py`
- Test: `tests/unit/test_schemas.py`, `tests/unit/conftest.py` (fixtures `valid_init`, `valid_match`, `valid_purchase` dicts)

**Interfaces:**
- Produces: `SchemaRegistry.from_dir(path: Path) -> SchemaRegistry`; `SchemaRegistry.validate(event: dict) -> list[str]` (empty list = valid; selects schema by `event["event-type"]`; unknown/missing type → one error). Fixtures `valid_init`, `valid_match`, `valid_purchase` in `tests/unit/conftest.py`.

- [ ] **Step 1:** `git init`; create `pyproject.toml` (src layout, package `pipeline`, extras `dq` = confluent-kafka, jsonschema, PyYAML; `spark` = pyspark, psycopg[binary]; `dev` = pytest). Copy the three schemas into `schemas/original/` and `schemas/raw/`; in `schemas/raw/match.json` change `game-tier` to `"minimum": 1, "maximum": 5` and add a top-level `"description"` noting the correction. Create a venv and `pip install -e .[dq,dev]`.
- [ ] **Step 2: Write failing tests** in `tests/unit/test_schemas.py`:
  - `test_valid_events_pass` — each fixture → `[]`.
  - `test_init_accepts_integer_user_id` — `valid_init | {"user-id": 42}` → `[]`.
  - `test_match_rejects_integer_user` — `valid_match | {"user-a": 42}` → non-empty.
  - `test_game_tier_bounds` — tier 1 and 5 valid; 0 and 6 invalid.
  - `test_original_match_schema_is_buggy` — validating with `schemas/original` rejects tier 3 (documents the bug).
  - `test_missing_required_field`, `test_additional_property_rejected`, `test_negative_purchase_value_rejected`, `test_unknown_event_type` (`{"event-type": "logout"}` → exactly one error mentioning `logout`), `test_user_b_postmatch_info_optional`.
- [ ] **Step 3:** Run `python -m pytest tests/unit/test_schemas.py -v` → FAIL (import error).
- [ ] **Step 4:** Implement `SchemaRegistry` with `jsonschema.Draft3Validator`, keyed by the schema's `event-type` enum value.
- [ ] **Step 5:** Run tests → PASS.
- [ ] **Step 6:** Commit `feat: project scaffold and schema registry`.

### Task 2: Transforms and rules engine

**Files:**
- Create: `src/pipeline/dq/__init__.py`, `src/pipeline/dq/fieldpath.py`, `src/pipeline/dq/transforms.py`, `src/pipeline/dq/rules.py`, `config/rules.yaml` (exactly the four rules in spec 5.3), `config/country_codes.csv`
- Test: `tests/unit/test_transforms.py`, `tests/unit/test_rules.py`

**Interfaces:**
- Produces:
  - `fieldpath.get(d: dict, path: str) -> Any` (raises `KeyError` if any segment missing); `fieldpath.set(d: dict, path: str, value) -> None`; `fieldpath.has(d, path) -> bool`.
  - `transforms.register(name: str)` decorator for factories `(params: dict, base_dir: Path) -> Callable[[Any], Any]`; `transforms.build(name, params, base_dir) -> Callable` (unknown name → `ValueError`).
  - Built-ins: `upper`, `lower`, `trim`, `map` (params `mapping_file` relative to `base_dir`, CSV `code,name`; `on_missing`: `"keep"` | `"null"` | any other string = literal replacement; default `"keep"`). Non-string input to `upper/lower/trim` is returned unchanged.
  - `rules.RuleSet.apply(event: dict) -> dict` (returns a deep copy; rule skipped if `event-type` not in `events` or path absent); `rules.load_rules(path: Path) -> RuleSet` (base_dir = the YAML's directory; invalid → `ValueError` at load time).

- [ ] **Step 1: Write failing tests:**
  - transforms: `test_upper`, `test_upper_non_string_passthrough`, `test_map_known_code` (`"TR"` → `"Turkey"`, `"GB"` → `"United Kingdom"`), `test_map_on_missing_keep|null|literal` (`"ZZ"` → `"ZZ"` / `None` / `"UNKNOWN"`), `test_unknown_transform_raises`, `test_custom_transform_registration` (register `"reverse"` in the test, build it, apply).
  - rules: `test_init_platform_uppercased_and_country_mapped` (`{"platform": "android", "country": "TR"}` → `"ANDROID"`, `"Turkey"`), `test_nested_match_platform_uppercased`, `test_missing_user_b_info_skipped` (no exception, no key created), `test_rule_ignores_other_event_types`, `test_apply_does_not_mutate_input`, `test_load_rejects_unknown_transform` (tmp YAML), `test_shipped_rules_load` (loads `config/rules.yaml`).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement. `country_codes.csv`: header `code,name`, all ISO-3166-1 alpha-2 codes, names per ISO short name in common English form (e.g. `GB,United Kingdom`, `US,United States`, `TR,Turkey`).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat: generic rules engine with upper/lower/trim/map transforms`.

### Task 3: Profile store and enrichment

**Files:**
- Create: `src/pipeline/dq/profiles.py`, `src/pipeline/dq/enrich.py`
- Test: `tests/unit/test_profiles.py`, `tests/unit/test_enrich.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) Profile(country: str, platform: str)`; `Profile.to_json() -> bytes`; `Profile.from_json(b: bytes) -> Profile`.
  - `ProfileStore`: `get(user_id: str) -> Profile | None` (staged first, then committed); `stage(user_id: str, profile: Profile)`; `staged() -> dict[str, Profile]`; `commit()`; `abort()`; `load(items: Iterable[tuple[str, Profile]])` (bulk committed load for bootstrap).
  - `enrich(event: dict, store: ProfileStore) -> dict` (deep copy). Normalises `user-id`, `user-a`, `user-b` to `str`. `in-app-purchase` → adds `country`, `platform`; `match` → adds `user-a-country`, `user-b-country`; `init` → only normalisation. Unknown user → `"UNKNOWN"` for country (and platform).

- [ ] **Step 1: Write failing tests:** `test_staged_visible_before_commit`, `test_abort_discards_staged`, `test_commit_persists`, `test_load_bootstrap`, `test_profile_json_roundtrip`, `test_enrich_purchase`, `test_enrich_match_both_users`, `test_enrich_match_without_user_b_info` (still sets `user-b-country`), `test_enrich_unknown_user` (→ `"UNKNOWN"`), `test_integer_user_id_normalised` (store has `"42"`; purchase with `"user-id": 42` → enriched, and output `user-id == "42"`).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat: transactional profile store and enrichment`.

### Task 4: Processor and clean-events schemas

**Files:**
- Create: `src/pipeline/dq/processor.py`, `schemas/clean/{init,match,in-app-purchase}.json` (raw schema + added fields from Task 3; user ids `type: string`; `country`/`platform` on purchase and `user-a-country`/`user-b-country` on match required)
- Test: `tests/unit/test_processor.py`

**Interfaces:**
- Consumes: `SchemaRegistry` (T1), `RuleSet` (T2), `ProfileStore`, `Profile`, `enrich` (T3).
- Produces: `@dataclass Clean(event: dict, key: str, profile_update: tuple[str, Profile] | None)`; `@dataclass Invalid(raw: bytes, errors: list[str])` with `Invalid.to_dead_letter() -> bytes` (JSON `{"errors": [...], "raw": "<utf-8 text, replacement on decode errors>"}`); `process(raw: bytes, registry: SchemaRegistry, rules: RuleSet, store: ProfileStore) -> Clean | Invalid`. Order: parse → validate (raw schemas) → `rules.apply` → for init: `store.stage(user_id, Profile(country, platform))` using **transformed** values → `enrich`. `key` = user-id (init/IAP) or user-a (match), as str.

- [ ] **Step 1: Write failing tests:** `test_non_json_is_invalid`, `test_schema_error_is_invalid` (errors non-empty), `test_init_stages_transformed_profile` (`TR`/`android` → staged `Profile("Turkey", "ANDROID")`), `test_purchase_after_init_same_batch_is_enriched` (two `process` calls, no commit in between → purchase `country == "Turkey"`), `test_clean_events_validate_against_clean_schemas` (each fixture through `process`, then `SchemaRegistry.from_dir(schemas/clean)` → `[]`), `test_dead_letter_payload`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run `python -m pytest tests/unit -v` → all PASS.
- [ ] **Step 5:** Commit `feat: pure DQ processor and clean-events contract`.

### Task 5: Event simulator

**Files:**
- Create: `src/pipeline/generator/__init__.py`, `src/pipeline/generator/simulator.py`
- Test: `tests/unit/test_simulator.py`

**Interfaces:**
- Produces: `UserSimulator(seed: int, max_users: int = 1000, invalid_rate: float = 0.01, new_user_prob: float = 0.05)`; `next_event(now: int) -> tuple[str, bytes]` (key, JSON value). Behaviour: user ids are numeric strings (`"100000"`…); a user's first event is always `init`; afterwards ~70% `match` (opponent = another already-initialised user, distinct), ~30% `in-app-purchase` (`purchase_value` 0.99–49.99, 2 dp); in init/IAP the `user-id` is emitted as `int` with probability 0.5; `country` from a weighted ISO code list (≥10 codes, include `TR`, `GB`, `US`); `platform` from `["android", "ios", "Android", "iOS"]`; `user-b-postmatch-info` omitted with probability 0.1; `game-tier` 1–5; `time = now - randint(0, 30)` (late jitter); with probability `invalid_rate` the event is corrupted (one of: drop a required field, negative `purchase_value`/`time`, `time` as string, unknown `event-type`, non-JSON bytes).

- [ ] **Step 1: Write failing tests:** `test_first_event_per_user_is_init`, `test_valid_events_pass_raw_schemas` (`invalid_rate=0`, 2,000 events → all `[]`), `test_invalid_events_fail` (`invalid_rate=1`, 200 events → none valid), `test_match_users_distinct_and_initialised`, `test_deterministic_with_seed`, `test_integer_and_string_ids_refer_to_same_user` (int-id init followed by a match referencing the same user as string).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat: deterministic 8ballpool event simulator`.

### Task 6: Kafka shells, DQ service, compose (Kafka part)

**Requires Docker Desktop.**

**Files:**
- Create: `src/pipeline/settings.py`, `src/pipeline/generator/main.py`, `src/pipeline/dq/service.py`, `docker/app.Dockerfile`, `docker-compose.yml` (services `kafka`, `init-topics`, `generator`, `dq-service`), `tests/integration/test_dq_service.py`
- Modify: `pyproject.toml` (console scripts `pipeline-generator`, `pipeline-dq`)

**Interfaces:**
- Consumes: T1–T5.
- Produces: `Settings.from_env() -> Settings` (fields: `bootstrap_servers`, `raw_topic`, `clean_topic`, `profiles_topic`, `dead_letter_topic`, `events_per_second`, `invalid_rate`, `seed`, `postgres_dsn`, `rules_path`, `schemas_dir`); `service.bootstrap_profiles(settings) -> ProfileStore` (assign all `user-profiles` partitions from beginning, read to high watermark, `read_committed`); `service.run(settings) -> None`.
- Service loop (algorithm the signature does not determine):
  ```
  producer.init_transactions()
  loop: msgs = consumer.consume(num_messages=500, timeout=1.0); skip if empty
        producer.begin_transaction()
        for m in msgs: r = process(...); produce Clean→clean_topic (key) / Invalid→dead_letter
        for uid, p in store.staged().items(): produce p→profiles_topic (key=uid)
        producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                             consumer.consumer_group_metadata())
        producer.commit_transaction(); store.commit()
        on KafkaException: producer.abort_transaction(); store.abort();
                           seek each assigned partition to consumer.committed(...) offsets
  ```
  Consumer: `enable.auto.commit=False`, `isolation.level=read_committed`, `auto.offset.reset=earliest`. Producer: `transactional.id=dq-service-1`, `enable.idempotence=True`. Log per-batch counts (clean / invalid / unknown-user).
- `init-topics` one-shot container creates the four topics (raw-events 1 partition; user-profiles `cleanup.policy=compact`); `dq-service` and `generator` depend on it completing.

- [ ] **Step 1: Write failing integration test** `tests/integration/test_dq_service.py` (marker `integration`): produce a hand-built init (`TR`, `android`, user `"900001"`), a purchase for `900001`, and one non-JSON message to `raw-events`; consume `clean-events` (`read_committed`) until 2 messages or 60 s → purchase has `country == "Turkey"`, `platform == "ANDROID"`; `dead-letter` has 1 message; `user-profiles` has key `900001`.
- [ ] **Step 2:** `docker compose up -d kafka init-topics dq-service`; run `docker compose run --rm tests pytest tests/integration/test_dq_service.py -v` (the `tests` service is added here using `app.Dockerfile` with dev extras) → FAIL before implementation.
- [ ] **Step 3:** Implement settings, service, generator main, Dockerfile, compose.
- [ ] **Step 4:** Re-run → PASS. Then `docker compose up -d generator` and check `docker compose logs dq-service` shows non-zero clean and invalid counts.
- [ ] **Step 5: Restart check (Review Focus 2):** `docker compose restart dq-service`; logs show profiles bootstrapped (count > 0) and processing resumes with no exception.
- [ ] **Step 6:** Commit `feat: exactly-once DQ service, generator and Kafka compose stack`.

### Task 7: Spark parsing and aggregations

**Files:**
- Create: `src/pipeline/spark/__init__.py`, `src/pipeline/spark/parsing.py`, `src/pipeline/spark/aggregations.py`, `docker/spark.Dockerfile`, `tests/spark/conftest.py` (session-scoped local `SparkSession`), `tests/spark/test_parsing.py`, `tests/spark/test_minute_aggregations.py`, `tests/spark/test_streaming_update_mode.py`
- Modify: `docker-compose.yml` (`tests` service switches to the Spark image so one container runs unit + spark + integration tests)

**Interfaces:**
- Produces:
  - `parse_clean_events(df: DataFrame) -> DataFrame` from a DataFrame with a string/binary `value` column → columns `event_type, event_time (timestamp from time), user_id, user_a, user_b, country, platform, purchase_value (double), user_a_country, user_b_country, user_a_platform, user_b_platform` (nulls where not applicable).
  - `to_participants(events) -> DataFrame[event_time, event_type, user_id, country, platform, purchase_value]` — init/IAP → 1 row; match → 2 rows (user-a with `user_a_country`/`user_a_platform`, user-b with `user_b_country`/`user_b_platform`); null platform/country → `"UNKNOWN"`; `purchase_value` null except IAP.
  - `minute_summary(participants) -> DataFrame[window_start, window_end, purchases_count, revenue_sum, distinct_users]` — `F.window("event_time", "1 minute")`; `purchases_count` = count of IAP rows; `revenue_sum` = sum of `purchase_value`, 0.0 when none; `distinct_users = F.size(F.collect_set("user_id"))`.
  - `revenue_by_country(participants) -> DataFrame[window_start, country, revenue_sum]` (IAP only).
  - `matches_by_country(events) -> DataFrame[window_start, country, matches_count]` — explode `F.array_distinct(F.array(user_a_country, user_b_country))`.
- Spark image: official `apache/spark` 4.x Python image (tag verified to exist); bake `spark-sql-kafka-0-10_2.13` matching the Spark version and its dependencies into `$SPARK_HOME/jars`; install the package with `spark` and `dev` extras.

- [ ] **Step 1: Write failing tests** (build input with `spark.createDataFrame` of JSON strings in a `value` column):
  - `test_parse_match_without_user_b_info` → `user_b_platform` null.
  - `test_participants_match_explodes_to_two_rows` and `..._unknown_platform_for_missing_user_b_info`.
  - `test_minute_summary` — minute 12:00: user A purchase 1.99 at :05, user A purchase 3.00 at :40, user B init at :10, match C vs D at :20; minute 12:01: user A init at 12:01:10 → 12:00 row: `purchases_count=2, revenue_sum=4.99, distinct_users=4`; 12:01 row: `purchases_count=0, revenue_sum=0.0, distinct_users=1`.
  - `test_revenue_by_country`, `test_matches_by_country_counts_once_per_distinct_country` (TR vs GB → TR 1, GB 1; TR vs TR → TR 1).
  - `test_streaming_update_mode_collect_set` (Review Focus 5; **also confirms the spec's key assumption**): file-source stream over a tmp dir of JSON-lines (`value` column), `withWatermark("event_time", "2 minutes")`, `minute_summary` in `outputMode("update")`, `foreachBatch` appending rows to a Python list, `trigger(availableNow=True)`; write batch 1 (2 users in minute M) → run → M has `distinct_users=2`; write batch 2 (1 new user in M, 1 repeat) → run again with same checkpoint → latest M row has `distinct_users=3`.
- [ ] **Step 2:** `docker compose build tests && docker compose run --rm tests pytest tests/spark -v` → FAIL.
- [ ] **Step 3:** Implement. If `collect_set` in update mode is rejected at runtime, STOP and report to the human before switching to the spec's append-mode fallback.
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Commit `feat: spark parsing and minute aggregations (exact distinct, update mode)`.

### Task 8: Postgres sink and streaming job

**Files:**
- Create: `sql/init.sql`, `src/pipeline/spark/sink.py`, `src/pipeline/spark/streaming_job.py`, `tests/unit/test_sink_sql.py`, `tests/integration/test_minute_tables.py`
- Modify: `docker-compose.yml` (`postgres` with `sql/init.sql` mounted to `/docker-entrypoint-initdb.d`, `spark-streaming` with a checkpoint volume)

**Interfaces:**
- Consumes: T7 functions, `Settings`.
- Produces: `build_upsert_sql(table: str, columns: list[str], key_columns: list[str]) -> str`; `upsert_rows(dsn: str, table: str, key_columns: list[str], rows: list[dict]) -> int` (psycopg, one transaction, `executemany`; no-op on empty); `replace_day(dsn, table, day: date, rows) -> int` (DELETE that day + INSERT in one transaction; used by T9).
- Tables (`sql/init.sql`): `minute_summary(window_start timestamptz PK, window_end timestamptz, purchases_count bigint, revenue_sum numeric(14,2), distinct_users bigint, updated_at timestamptz default now())`; `minute_revenue_by_country(window_start, country text, revenue_sum numeric(14,2), updated_at, PK(window_start, country))`; `minute_matches_by_country(window_start, country, matches_count bigint, updated_at, PK(window_start, country))`; `daily_users_by_country_platform(day date, country text, platform text, distinct_users bigint, updated_at, PK(day, country, platform))`. Upserts also set `updated_at = now()`.
- `streaming_job.main()`: read Kafka `clean-events` (`startingOffsets=earliest`, `kafka.isolation.level=read_committed`) → `parse_clean_events` → `withWatermark("event_time", "2 minutes")` → three queries (update mode, `trigger(processingTime="10 seconds")`, checkpoint `/checkpoints/<table>`), each `foreachBatch` → `upsert_rows(..., [r.asDict() for r in batch.collect()])`; `spark.streams.awaitAnyTermination()`.

- [ ] **Step 1: Write failing tests:** `test_build_upsert_sql` (exact expected string for `minute_revenue_by_country` with keys `window_start,country`, including `ON CONFLICT (window_start, country) DO UPDATE SET revenue_sum = EXCLUDED.revenue_sum, updated_at = now()`); integration `test_minute_tables_fill` (stack running ≥ 3 minutes → each minute table has ≥ 1 row; `minute_summary.distinct_users > 0`; at least one country in `minute_revenue_by_country` is a full name, e.g. `Turkey`).
- [ ] **Step 2:** Run unit test locally → FAIL. **Step 3:** Implement. **Step 4:** unit → PASS; `docker compose up -d`, wait 3 min, `docker compose run --rm tests pytest tests/integration/test_minute_tables.py -v` → PASS.
- [ ] **Step 5:** Commit `feat: live minute aggregator with idempotent Postgres upserts`.

### Task 9: Daily batch job

**Files:**
- Create: `src/pipeline/spark/daily_job.py`, `tests/spark/test_daily_users.py`, `tests/integration/test_daily_table.py`
- Modify: `src/pipeline/spark/aggregations.py` (add `daily_users`), `docker-compose.yml` (`spark-batch` service runs `--loop`)

**Interfaces:**
- Produces: `daily_users(participants: DataFrame, day: date) -> DataFrame[day, country, platform, distinct_users]` (UTC day filter on `event_time`; exact `countDistinct` — batch, so allowed); `daily_job.main(argv)` with `--date YYYY-MM-DD` (default today UTC) and `--loop` (recompute today every 300 s); reads Kafka in batch (`startingOffsets=earliest`, `endingOffsets=latest`, `read_committed`) → `parse_clean_events` → `to_participants` → `daily_users` → `replace_day`.

- [ ] **Step 1: Write failing tests:** `test_daily_users_by_country_platform` (same user with 3 events → counted once), `test_daily_users_utc_boundaries` (23:59:59 and 00:00:00 UTC land on different days), `test_daily_users_includes_both_match_players`, `test_daily_users_missing_user_b_info_platform_unknown`; integration `test_daily_table_fill` (after `docker compose run --rm spark-batch --date <today>` → rows for today; re-run → same row count).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat: daily distinct users batch job`.

### Task 10: README and final verification

**Files:**
- Create: `README.md`

- [ ] **Step 1:** Write README sections: Overview + architecture diagram (spec §5); Quick start (`docker compose up -d --build`, how long until data appears, example `psql` queries for each table via `docker compose exec postgres psql ...`); How to run tests (local unit; container spark/integration); Design decisions with the *why* (exactly-once + why at-least-once is unsafe without event IDs; GlobalKTable-like profile store; update mode + `collect_set` with the Spark source evidence and its state cost; rejected alternatives from spec §5.5); Assumptions (spec §7 schema issues, `time` unit, metric interpretations, ISO codes, UNKNOWN policy); How to extend the rules engine (add a YAML rule; register a transform — 5-line example); Limitations and production next steps (scale-out path from spec §5.2, HLL sketches for roll-ups, Schema Registry, monitoring/alerting on dead-letter rate).
- [ ] **Step 2:** Clean-slate verification: `docker compose down -v`, `docker compose up -d --build`, wait 4 minutes, run `docker compose run --rm tests pytest -v` (all suites) → all PASS; paste the four tables' row counts into the README "Verified on" note.
- [ ] **Step 3:** Commit `docs: README with design rationale, assumptions and runbook`.
