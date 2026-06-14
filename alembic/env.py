import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context
from crypto_momentum_lab.persistence.postgres import models  # noqa: F401
from crypto_momentum_lab.persistence.postgres.base import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def sync_database_url() -> str:
    return os.environ["CML_DATABASE_URL"].replace(
        "postgresql+asyncpg",
        "postgresql+psycopg",
    )


def run_migrations_offline() -> None:
    context.configure(
        url=sync_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = sync_database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
