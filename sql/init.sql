-- Schema of the pipeline's serving tables. Postgres runs this file once, when the data
-- directory is first initialised (docker-entrypoint-initdb.d); `docker compose down -v`
-- resets the volume so it runs again. Every statement is idempotent anyway.
--
-- The minute tables are written by the Spark streaming job with upserts (update output
-- mode: a minute's row is refined on every trigger until its window closes), so the primary
-- key is exactly the grouping key and replaying a micro-batch is harmless. The daily table
-- is rewritten per day by the batch job (delete that day + insert, in one transaction).

CREATE TABLE IF NOT EXISTS minute_summary (
    window_start    timestamptz   PRIMARY KEY,
    window_end      timestamptz   NOT NULL,
    purchases_count bigint        NOT NULL,
    revenue_sum     numeric(14,2) NOT NULL,
    distinct_users  bigint        NOT NULL,
    updated_at      timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS minute_revenue_by_country (
    window_start timestamptz   NOT NULL,
    country      text          NOT NULL,
    revenue_sum  numeric(14,2) NOT NULL,
    updated_at   timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, country)
);

CREATE TABLE IF NOT EXISTS minute_matches_by_country (
    window_start  timestamptz NOT NULL,
    country       text        NOT NULL,
    matches_count bigint      NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, country)
);

CREATE TABLE IF NOT EXISTS daily_users_by_country_platform (
    day            date        NOT NULL,
    country        text        NOT NULL,
    platform       text        NOT NULL,
    distinct_users bigint      NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, country, platform)
);
