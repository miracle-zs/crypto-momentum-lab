"""Exercise the production inventory query against real PostgreSQL partitions."""

from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from deploy.ops.cml_ops_monitor import MonitorConfig, OpsMonitor

pytestmark = pytest.mark.integration


def test_inventory_includes_unlisted_tables_and_groups_partition_children(
    async_database_url, tmp_path
):
    engine = create_engine(
        async_database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )
    name = "growth_" + uuid4().hex
    with engine.connect() as connection, connection.begin():
        connection.execute(
            text(
                f"CREATE TABLE public.{name}(id int, payload text) PARTITION BY RANGE(id)"
            )
        )
        for suffix, low, high in [("a", 0, 10), ("b", 10, 20)]:
            connection.execute(
                text(
                    f"CREATE TABLE public.{name}_{suffix} PARTITION OF public.{name} FOR VALUES FROM ({low}) TO ({high})"
                )
            )
        connection.execute(
            text(
                f"INSERT INTO public.{name} VALUES (1,repeat('a',1000)),(11,repeat('b',1000))"
            )
        )
        connection.execute(text(f"CREATE TABLE public.{name}_other(id int)"))
        connection.execute(text(f"INSERT INTO public.{name}_other VALUES (1)"))
        connection.execute(text(f"CREATE TEMP TABLE {name}_temporary(id int)"))
        connection.execute(text(f"INSERT INTO {name}_temporary VALUES (1)"))

        class Runner:
            def run(self, args, **kwargs):
                return connection.execute(text(args[-1])).scalar_one()

        monitor = OpsMonitor(
            MonitorConfig(state_path=tmp_path / "state.json"), runner=Runner()
        )
        footprint = monitor._database_storage_footprint("postgres", now=100)
        sizes = footprint["relations"]
        expected = connection.execute(
            text(
                f"SELECT pg_total_relation_size('public.{name}_a')+pg_total_relation_size('public.{name}_b')"
            )
        ).scalar_one()
        assert sizes[name] == expected
        assert sizes[name + "_other"] > 0
        assert name + "_a" not in sizes and name + "_b" not in sizes
        assert name + "_temporary" not in sizes
        assert isinstance(footprint["database_bytes"], int)
    engine.dispose()
