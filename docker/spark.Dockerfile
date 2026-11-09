# Image for the Spark jobs (minute streaming aggregator, daily batch) AND the full test runner
# (unit + spark + integration tests), so one container covers every test layer.
#
#   docker build -f docker/spark.Dockerfile -t pipeline-spark:dev .
#
# Why not the official apache/spark image: it ships Python 3.10, this package needs >= 3.12.
# So: python:3.12 + a JRE + the self-contained pyspark wheel (it bundles the Spark jars).
FROM python:3.12.16-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Spark 4.3 runs on Java 21 (Debian trixie has no Java 17). A fixed JAVA_HOME symlink keeps
# the path independent of the CPU architecture (amd64 / arm64).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java

WORKDIR /app

# Step 1 (cached until pyproject.toml changes): install pyspark at the version pinned in
# pyproject.toml and bake the Kafka connector into its jars/ at BUILD time, so containers
# need no network (and no spark.jars.packages) at runtime. Ivy resolves the connector plus
# its transitive dependencies; jars pyspark already ships are not duplicated.
COPY pyproject.toml ./
COPY docker/bake_kafka_connector.py /tmp/bake_kafka_connector.py
RUN pip install "$(grep -o 'pyspark==[0-9.]*' pyproject.toml)" \
    && python /tmp/bake_kafka_connector.py \
    && rm -rf /tmp/bake_kafka_connector.py /tmp/ivy /root/.ivy2 /root/.m2

# Step 2: the package with all extras (pyspark is already satisfied), then the test inputs.
COPY src ./src
RUN pip install -e ".[dq,spark,dev]"

COPY config ./config
COPY schemas ./schemas
COPY tests ./tests

# /checkpoints exists (owned by `app`) in the image so a named volume mounted there inherits
# that ownership and the streaming job can write its checkpoints as the non-root user.
RUN useradd --create-home --uid 1000 app \
    && mkdir /checkpoints \
    && chown -R app:app /app /checkpoints
USER app

CMD ["pytest", "tests/unit", "-q"]
