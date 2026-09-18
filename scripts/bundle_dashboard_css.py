#!/usr/bin/env python3
"""Assemble modular CSS source files into the production dashboard.css bundle."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "src/crypto_momentum_lab/operator_dashboard/static"
STYLES = STATIC / "styles"

MODULES = [
    "tokens.css",
    "base.css",
    "layout.css",
    "components.css",
    "charts.css",
    "sections/overview.css",
    "sections/universe.css",
    "sections/risk.css",
    "sections/account.css",
    "sections/strategy.css",
    "sections/reports.css",
    "sections/collector.css",
    "sections/performance.css",
]

def bundle_css() -> str:
    header_modules = "\n".join(f"   - ./styles/{m}" for m in MODULES)
    parts = [
        f"""/* {"=" * 74}
   CML Flight Deck Operator Dashboard
   Production High-Performance Bundle (Zero-Waterfall)
   Source modules:
{header_modules}
   {"=" * 74} */"""
    ]

    for mod in MODULES:
        path = STYLES / mod
        if not path.is_file():
            raise FileNotFoundError(f"Missing modular stylesheet: {path}")
        content = path.read_text(encoding="utf-8").strip()
        parts.append(f"\n/* --- Module: ./styles/{mod} --- */\n{content}")

    return "\n".join(parts) + "\n"

def main() -> None:
    bundle = bundle_css()
    target = STATIC / "dashboard.css"
    target.write_text(bundle, encoding="utf-8")
    line_count = len(bundle.splitlines())
    byte_count = len(bundle.encode("utf-8"))
    print(
        f"Bundled {len(MODULES)} modules into {target.name} "
        f"({line_count} lines, {byte_count}B)"
    )

if __name__ == "__main__":
    main()
