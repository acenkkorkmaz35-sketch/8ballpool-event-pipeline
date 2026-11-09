# Image for the Python services (DQ service, event generator).
#
#   docker build -f docker/app.Dockerfile .                            # runtime: dq extra only
#   docker build -f docker/app.Dockerfile --build-arg EXTRAS=dq,dev .   # + pytest, for ad-hoc use
#
# The `spark` extra is deliberately NOT installed here: it needs a JVM and lives in the
# Spark image. The tests (unit, kafka_unit, spark, integration) run in that Spark image via
# the `tests` compose service, so this image does not ship tests/.
FROM python:3.12.16-slim

ARG EXTRAS=dq

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies and the package first so this layer is cached until
# pyproject.toml or src/ change (config/schemas/tests changes do not reinstall).
COPY pyproject.toml ./
COPY src ./src
RUN pip install -e ".[${EXTRAS}]"

# Runtime files: relative paths in Settings (config/rules.yaml, schemas/raw) resolve
# against /app.
COPY config ./config
COPY schemas ./schemas

RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

# Overridden per service in docker-compose.yml (pipeline-dq / pipeline-generator).
CMD ["pipeline-dq"]
