from dataclasses import dataclass
from pathlib import Path

from crypto_momentum_lab.market_data.aggregation import aggregate_market_states_15s
from crypto_momentum_lab.market_data.normalization import normalize_binance_envelope
from crypto_momentum_lab.persistence.parquet import (
    DerivedDatasetManifest,
    write_market_events_dataset,
    write_market_states_15s_dataset,
)
from crypto_momentum_lab.persistence.raw_files.reader import replay_envelopes


@dataclass(frozen=True, slots=True)
class DerivedMarketDatasets:
    market_event_manifests: tuple[DerivedDatasetManifest, ...]
    market_state_manifests: tuple[DerivedDatasetManifest, ...]


def derive_market_datasets(
    *,
    raw_paths: tuple[Path, ...],
    output_root: Path,
) -> DerivedMarketDatasets:
    envelopes = replay_envelopes(raw_paths)
    events = tuple(normalize_binance_envelope(envelope) for envelope in envelopes)
    states = aggregate_market_states_15s(events)
    return DerivedMarketDatasets(
        market_event_manifests=write_market_events_dataset(
            root=output_root,
            events=events,
            input_paths=raw_paths,
        ),
        market_state_manifests=write_market_states_15s_dataset(
            root=output_root,
            states=states,
            input_paths=raw_paths,
        ),
    )
