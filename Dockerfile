# syntax=docker/dockerfile:1

FROM python:3.12-slim@sha256:12316debbfb86890f6b2d825820612741ba527ad5a44e3902759141d572bfe8f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN addgroup --system cml && adduser --system --ingroup cml cml

COPY pyproject.toml requirements-runtime.lock ./

# Keep third-party dependencies in a layer keyed by the dependency lock. A
# source-only change can then rebuild the small local wheel without resolving
# or downloading pyarrow, asyncpg, and the other runtime packages again.
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -r requirements-runtime.lock && python -m pip check

COPY SYSTEM.md ./
COPY src ./src
COPY configs ./configs
COPY alembic.ini ./
COPY alembic ./alembic
COPY deploy/live-runtime.yaml ./deploy/live-runtime.yaml
COPY docker/local-healthcheck /usr/local/bin/cml-local-healthcheck

RUN chmod 0755 /usr/local/bin/cml-local-healthcheck

RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation . \
    && python -m pip check

# Keep the release identity below the dependency layer. Changing only the
# commit must not invalidate dependency resolution or download layers.
ARG CML_CODE_COMMIT
ENV CML_CODE_COMMIT=${CML_CODE_COMMIT}

RUN mkdir -p /app/data /run/cml/health && chown -R cml:cml /app /run/cml

USER cml

ENTRYPOINT ["cml-market-data"]
CMD ["run-market-data"]
