"""Build-time helper: copy the Spark Kafka connector and its dependencies into pyspark's jars/.

Starts a throw-away local SparkSession with ``spark.jars.packages`` so Ivy resolves the
connector with its transitive dependencies, then copies the resolved jars next to the jars
that ship with pyspark. Jars whose artifact is already bundled in pyspark are skipped to
avoid duplicate classes on the classpath. Runs once at image build; never at runtime.
"""

import glob
import os
import re
import shutil
import sys

import pyspark
from pyspark.sql import SparkSession

IVY_DIR = "/tmp/ivy"


# Ivy names resolved jars "<group>_<artifact>-<version>.jar" with a dotted group id
# ("org.apache.kafka_kafka-clients-3.9.2.jar"); jars shipped by pyspark have no group prefix.
# Every group segment starts with a letter, so an artifact such as
# "netty-...-2.0.76.Final-linux-x86_64.jar" is not mistaken for a group.
_IVY_GROUP_PREFIX = re.compile(r"^(?:[a-z][a-z0-9-]*\.)+[a-z][a-z0-9-]*_")
# Trailing "-<version>" plus optional "-<qualifier>" parts ("-4.3.0", "-33.4.8-jre",
# "-4.1.100.Final-linux-x86_64"). A "_2.13" Scala suffix is part of the artifact name and stays.
_VERSION_SUFFIX = re.compile(r"-\d[\w.]*(?:-[A-Za-z][\w.]*)*$")


def artifact(jar_name: str) -> str:
    """Artifact name without Ivy group prefix and version, e.g. 'kafka-clients'.

    >>> artifact("org.apache.kafka_kafka-clients-3.9.2.jar")
    'kafka-clients'
    >>> artifact("scala-parallel-collections_2.13-1.2.0.jar")
    'scala-parallel-collections_2.13'
    """
    stem = re.sub(r"\.jar$", "", jar_name)
    stem = _IVY_GROUP_PREFIX.sub("", stem)
    return _VERSION_SUFFIX.sub("", stem)


def main() -> None:
    spark_version = pyspark.__version__
    package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", package)
        .config("spark.jars.ivy", IVY_DIR)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.stop()

    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    shipped = {artifact(name) for name in os.listdir(jars_dir)}
    resolved = sorted(glob.glob(f"{IVY_DIR}/jars/*.jar"))
    if not resolved:
        sys.exit(f"Ivy resolved no jars for {package}")
    for path in resolved:
        name = os.path.basename(path)
        if artifact(name) in shipped:
            print(f"skip (already in pyspark): {name}")
            continue
        shutil.copy(path, jars_dir)
        print(f"baked: {name}")
    shutil.rmtree(IVY_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
