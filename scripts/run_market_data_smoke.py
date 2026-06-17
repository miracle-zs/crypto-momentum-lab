import argparse
import asyncio
from pathlib import Path

from crypto_momentum_lab.apps.market_data.main import run_market_data_for


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/environments/research.yaml"),
    )
    parser.add_argument("--seconds", type=int, default=1800)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await run_market_data_for(args.config, seconds=args.seconds)


if __name__ == "__main__":
    asyncio.run(main())
