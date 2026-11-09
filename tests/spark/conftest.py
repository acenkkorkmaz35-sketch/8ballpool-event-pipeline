"""Spark test fixtures: one small local SparkSession shared by the whole test session.

These tests need a JVM, so they run in the ``tests`` container. Where pyspark is not
installed (e.g. the local venv used for the unit tests) the test modules in this
directory are skipped at collection time instead of failing.
"""

import importlib.util
import json
from typing import Any

import pytest

if importlib.util.find_spec("pyspark") is None:
    collect_ignore_glob = ["test_*.py"]


@pytest.fixture(scope="session")
def spark():
    """A tiny local SparkSession (UTC, 2 cores, 2 shuffle partitions, no UI)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-spark-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def make_events(spark):
    """Build a raw ``value``-column DataFrame from clean-event dicts (as Kafka would deliver)."""

    def _make(events: list[dict[str, Any]]):
        return spark.createDataFrame([(json.dumps(e),) for e in events], ["value"])

    return _make
