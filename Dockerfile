FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

ARG CML_CODE_COMMIT

RUN addgroup --system cml && adduser --system --ingroup cml cml

COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
COPY alembic.ini ./
COPY alembic ./alembic

RUN python -m pip install --no-cache-dir .

ENV CML_CODE_COMMIT=${CML_CODE_COMMIT}

RUN mkdir -p /app/data && chown -R cml:cml /app

USER cml

ENTRYPOINT ["cml-market-data"]
CMD ["run-market-data"]
