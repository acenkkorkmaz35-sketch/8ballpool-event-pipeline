"""Unit tests for :class:`pipeline.settings.Settings` environment parsing."""

from pathlib import Path

from pipeline.settings import Settings


def test_defaults_suit_the_compose_stack():
    """With an empty environment the defaults match the docker-compose setup."""
    s = Settings.from_env({})
    assert s.bootstrap_servers == "kafka:9092"
    assert s.raw_topic == "raw-events"
    assert s.clean_topic == "clean-events"
    assert s.profiles_topic == "user-profiles"
    assert s.dead_letter_topic == "dead-letter"
    assert s.events_per_second == 50
    assert s.invalid_rate == 0.01
    assert s.seed == 42
    assert s.max_users == 1000
    assert s.postgres_dsn == "postgresql://pipeline:pipeline@postgres:5432/pipeline"
    assert s.rules_path == Path("config/rules.yaml")
    assert s.schemas_dir == Path("schemas/raw")


def test_environment_overrides_defaults_and_casts_types():
    """Environment variables override defaults and are cast to the field types."""
    s = Settings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
            "RAW_TOPIC": "raw-x",
            "EVENTS_PER_SECOND": "200",
            "INVALID_RATE": "0.25",
            "SEED": "7",
            "RULES_PATH": "/etc/rules.yaml",
            "SCHEMAS_DIR": "/etc/schemas",
        }
    )
    assert s.bootstrap_servers == "localhost:29092"
    assert s.raw_topic == "raw-x"
    assert s.events_per_second == 200
    assert s.invalid_rate == 0.25
    assert s.seed == 7
    assert s.rules_path == Path("/etc/rules.yaml")
    assert s.schemas_dir == Path("/etc/schemas")
    # Untouched fields keep their defaults.
    assert s.clean_topic == "clean-events"


def test_empty_values_fall_back_to_defaults():
    """An empty variable (e.g. ``SEED=`` in compose) is treated as unset."""
    assert Settings.from_env({"SEED": ""}).seed == 42


def test_max_users_is_configurable():
    """``MAX_USERS`` caps the generator's user population."""
    assert Settings.from_env({"MAX_USERS": "50"}).max_users == 50
