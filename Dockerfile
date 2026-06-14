FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
COPY alembic.ini ./
COPY alembic ./alembic

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["cml-market-data"]
CMD ["run-universe-scheduler"]
