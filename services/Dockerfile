# Shared image for every Python service.
#
# One Dockerfile rather than four: the services differ only in their entrypoint
# and they all depend on racestream_common, so a single build keeps the common
# layer cached across all of them and guarantees they run identical library
# versions - which matters when they are exchanging serialised events.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Dependency layers first, ordered least- to most-frequently changed.
COPY common/pyproject.toml /app/common/pyproject.toml
COPY common/racestream_common/__init__.py /app/common/racestream_common/__init__.py
RUN pip install -e /app/common

COPY api/requirements.txt /app/api/requirements.txt
RUN pip install -r /app/api/requirements.txt

COPY ingestion/requirements.txt /app/ingestion/requirements.txt
RUN pip install -r /app/ingestion/requirements.txt

# Source last: a code change rebuilds only from here down.
COPY common /app/common
COPY api /app/api
COPY ingestion /app/ingestion
COPY processor /app/processor
COPY replay /app/replay

# Run as a non-root user. Nothing here needs privileges.
RUN useradd --create-home --uid 10001 racestream \
 && chown -R racestream:racestream /app
USER racestream

# --- api -------------------------------------------------------------------
FROM base AS api
WORKDIR /app/api
EXPOSE 8000 9100
HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=4 \
  CMD curl -fsS http://localhost:8000/api/system/live >/dev/null || exit 1
CMD ["uvicorn", "racestream_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning"]

# --- ingestion -------------------------------------------------------------
FROM base AS ingestion
WORKDIR /app/ingestion
EXPOSE 9101
CMD ["python", "-m", "racestream_ingestion.main"]

# --- processor -------------------------------------------------------------
FROM base AS processor
WORKDIR /app/processor
EXPOSE 9102
CMD ["python", "-m", "racestream_processor.main"]

# --- replay ----------------------------------------------------------------
FROM base AS replay
WORKDIR /app/replay
EXPOSE 9103
CMD ["python", "-m", "racestream_replay.main"]
