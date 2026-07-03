from datetime import UTC, datetime

from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateCursor,
)
from crypto_momentum_lab.strategy_runner.live_source import (
    PaperLiveSourceConfig,
    PostgresPaperMarketStateSource,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


class FakeLoader:
    def __init__(self, batches) -> None:
        self.batches = list(batches)
        self.cursors: list[RuntimeStateCursor] = []

    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
    ):
        self.cursors.append(cursor)
        if not self.batches:
            return ()
        return self.batches.pop(0)


def test_postgres_paper_source_yields_in_order_and_advances_cursor() -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("ETHUSDT", 0)
    loader = FakeLoader(
        [
            tuple(
                sorted(
                    (second, first),
                    key=lambda item: (item.bucket_start, item.symbol),
                )
            ),
            (),
        ]
    )
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=0,
            idle_timeout_seconds=0,
            max_states=3,
            batch_size=10,
        ),
    )

    states = tuple(source)

    assert tuple(state.symbol for state in states) == ("BTCUSDT", "ETHUSDT")
    assert loader.cursors[0] == RuntimeStateCursor()
    assert loader.cursors[-1] == RuntimeStateCursor(
        bucket_start=second.bucket_start,
        symbol="ETHUSDT",
    )


def test_postgres_paper_source_stops_after_idle_timeout() -> None:
    loader = FakeLoader([()])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            poll_interval_seconds=0,
            idle_timeout_seconds=0,
            max_states=10,
            batch_size=10,
        ),
    )

    assert tuple(source) == ()
    assert loader.cursors == [
        RuntimeStateCursor(
            bucket_start=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            symbol="",
        )
    ]
