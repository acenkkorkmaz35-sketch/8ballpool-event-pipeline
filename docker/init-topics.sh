#!/bin/sh
# One-shot job: create the pipeline's Kafka topics (idempotent via --if-not-exists).
#
# Every topic has ONE partition: the DQ service must see an `init` event before any later
# event of the same user (including `user-b` of a match), and a single partition gives that
# total order without extra machinery. See the README for the scale-out path.
set -eu

KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

create() {
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --partitions 1 --replication-factor 1 "$@"
}

create --topic "${RAW_TOPIC:-raw-events}"
create --topic "${CLEAN_TOPIC:-clean-events}"
create --topic "${DEAD_LETTER_TOPIC:-dead-letter}"
# Compacted: only the latest profile per user is kept, so the DQ service can rebuild its
# user-id -> {country, platform} map by reading the topic from the beginning.
create --topic "${PROFILES_TOPIC:-user-profiles}" --config cleanup.policy=compact

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
