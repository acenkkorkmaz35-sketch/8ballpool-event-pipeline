"""Runtime configuration shared by the event generator, the DQ service and the Spark jobs.

All values come from environment variables so the same image runs unchanged in
docker-compose and locally. The defaults match the compose stack (broker reachable as
``kafka:9092``, paths relative to the image working directory ``/app``).
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Immutable configuration; build it with :meth:`from_env`."""

    bootstrap_servers: str = "kafka:9092"
    raw_topic: str = "raw-events"
    clean_topic: str = "clean-events"
    profiles_topic: str = "user-profiles"
    dead_letter_topic: str = "dead-letter"
    events_per_second: int = 50
    invalid_rate: float = 0.01
    seed: int = 42
    max_users: int = 1000
    postgres_dsn: str = "postgresql://pipeline:pipeline@postgres:5432/pipeline"
    rules_path: Path = Path("config/rules.yaml")
    schemas_dir: Path = Path("schemas/raw")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        """Read settings from ``env`` (default: ``os.environ``).

        Unset or empty variables fall back to the field defaults. Numeric values are
        cast; a malformed number raises ``ValueError`` so misconfiguration fails fast.
        """
        env = os.environ if env is None else env
        d = cls()  # defaults live in one place: the dataclass fields

        def get(name: str, default):
            """Return the env var ``name``, or ``default`` when it is unset or empty."""
            value = env.get(name)
            return default if value is None or value == "" else value

        return cls(
            bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            raw_topic=get("RAW_TOPIC", d.raw_topic),
            clean_topic=get("CLEAN_TOPIC", d.clean_topic),
            profiles_topic=get("PROFILES_TOPIC", d.profiles_topic),
            dead_letter_topic=get("DEAD_LETTER_TOPIC", d.dead_letter_topic),
            events_per_second=int(get("EVENTS_PER_SECOND", d.events_per_second)),
            invalid_rate=float(get("INVALID_RATE", d.invalid_rate)),
            seed=int(get("SEED", d.seed)),
            max_users=int(get("MAX_USERS", d.max_users)),
            postgres_dsn=get("POSTGRES_DSN", d.postgres_dsn),
            rules_path=Path(get("RULES_PATH", d.rules_path)),
            schemas_dir=Path(get("SCHEMAS_DIR", d.schemas_dir)),
        )
