FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

ARG CML_CODE_COMMIT

RUN addgroup --system cml && adduser --system --ingroup cml cml

COPY pyproject.toml ./

# Keep third-party dependencies in a layer keyed only by pyproject.toml. A
# source-only change can then rebuild the small local wheel without resolving
# or downloading pyarrow, asyncpg, and the other runtime packages again.
RUN python -c "import subprocess,sys,tomllib; deps=tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']; subprocess.check_call([sys.executable,'-m','pip','install','hatchling>=1.27,<2',*deps])"

COPY README.md ./
COPY src ./src
COPY configs ./configs
COPY alembic.ini ./
COPY alembic ./alembic

RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation .

ENV CML_CODE_COMMIT=${CML_CODE_COMMIT}

RUN mkdir -p /app/data && chown -R cml:cml /app

USER cml

ENTRYPOINT ["cml-market-data"]
CMD ["run-market-data"]
