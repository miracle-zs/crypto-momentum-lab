import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    parse_user_data_event,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.models import AccountUserDataJournalRow
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_journal_is_idempotent_ordered_paged_and_account_scoped(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresAccountRepository(factory)
    environment = "test-journal-" + uuid4().hex[:12]
    now = datetime(2026, 10, 1, tzinfo=UTC)
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1790784000000,
            "T": 1790784000000,
            "u": 9,
            "pu": 8,
            "a": {"B": [{"a": "USDT", "wb": "101", "cw": "80"}], "P": []},
        },
        received_at=now,
    )
    next_event = parse_user_data_event(
        {"e": "ACCOUNT_CONFIG_UPDATE", "E": 1790784001000},
        received_at=now + timedelta(seconds=1),
    )

    async def append(account, item, session="receiver-a", token=1, env=None):
        return await repository.append_user_data_event(
            environment=env or environment,
            account_label=account,
            receiver_session_id=session,
            stream_token=token,
            event=item,
        )

    try:
        first = await append("account-3", event)
        duplicate = await append(
            "account-3",
            replace(event, received_at=now + timedelta(seconds=2)),
            "receiver-b",
            2,
        )
        assert duplicate == first
        second = await append("account-3", next_event)
        assert second > first
        rows = await repository.load_user_data_events(
            environment=environment, account_label="account-3", limit=1
        )
        assert len(rows) == 1
        assert rows[0].event == event
        assert rows[0].receiver_session_id == "receiver-a"
        assert rows[0].stream_token == 1
        page = await repository.load_user_data_events(
            environment=environment, account_label="account-3", after_sequence=first
        )
        assert [row.event for row in page] == [next_event]
        other = await append("account-4", event)
        other_env = await append("account-3", event, env=environment + "-other")
        assert len({first, second, other, other_env}) == 4
        duplicates = await asyncio.gather(
            append("account-4", next_event), append("account-4", next_event)
        )
        assert duplicates[0] == duplicates[1]
        # A new repository instance sees the receipts; no daemon memory is needed.
        reopened = PostgresAccountRepository(factory)
        rows = await reopened.load_user_data_events(
            environment=environment, account_label="account-3"
        )
        assert [row.sequence for row in rows] == [first, second]
        assert (
            await reopened.load_user_data_events(
                environment=environment, account_label="unknown"
            )
            == ()
        )
        with pytest.raises(ValueError):
            await reopened.load_user_data_events(
                environment=environment, account_label="account-3", after_sequence=-1
            )
    finally:
        async with factory() as session:
            async with session.begin():
                await session.execute(
                    delete(AccountUserDataJournalRow).where(
                        AccountUserDataJournalRow.environment.in_(
                            [environment, environment + "-other"]
                        )
                    )
                )
        await engine.dispose()
