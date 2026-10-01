---
target: src/crypto_momentum_lab/operator_dashboard/static/index.html
total_score: 23
max_score: 40
na_heuristics: 
p0_count: 1
p1_count: 1
target_identity: "file:/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/operator_dashboard/static/index.html"
target_fingerprint: "sha256:f5f48502ddd8c013d8563e74807633a3c24b9f90498991549cd81bac6bd0b492"
target_path: /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/operator_dashboard/static/index.html
timestamp: 2026-10-01T14-51-48Z
slug: lab-operator-dashboard-static-index-html-72a64e72
closed: true
---
# CML Flight Deck Operator Dashboard Critique

## Design Health Score

| # | Heuristic | Score | Key Finding |
|---|-----------|:-----:|-------------|
| 1 | Visibility of System Status | 3 | Dynamic readiness classification, but network timeouts lack prominent alerting and fallback cache is hidden in tooltip |
| 2 | Match System / Real World | 3 | Authentic quant trading microstructure domain vocabulary, but Chinese/English labels mix incoherently and backend leaks (e.g. "240 桶") |
| 3 | User Control and Freedom | 2 | Hash navigation is solid, but zero manual refresh control and View 06 (Actions) buttons are permanently disabled |
| 4 | Consistency and Standards | 2 | Double `:root` definitions in tokens.css, status naming discrepancies between readiness strip and sidebar legend |
| 5 | Error Prevention | 2 | Read-only prevents accidental writes, but time zones alternate (UTC vs UTC+8) risking operational misinterpretation |
| 6 | Recognition Rather Than Recall | 3 | Nav status dots and signal telemetry clear; order hashes truncated without one-click copy buttons |
| 7 | Flexibility and Efficiency | 2 | Keyboard-accessible tabs, but zero global accelerators (1-6) or table column sorting |
| 8 | Aesthetic and Minimalist Design | 2 | Topbar overstuffed with >8 competing indicators; repetitive card headers and decorative eyebrows/section numbering |
| 9 | Error Recovery | 2 | Clear halt codes and symbol gaps, but zero actionable recovery steps or daemon log links |
| 10 | Help and Documentation | 2 | Helpful inline reconciliation notes, but missing operational runbooks or parameter glossary |
| **Total** | | **23/40** | **Acceptable (57.5%)** |

## Design Specificity Verdict

### LLM Assessment (Assessment A)
The UI demonstrates genuine quantitative trading microstructure DNA (liquidation notional, impulse return, compression range, aggressive imbalance, multi-account live fleet). However, it is weakened by token schizophrenia (two competing `:root` themes) and the passive "observation-only" nature of View 06 where all action buttons are permanently disabled without CLI fallbacks.

### Deterministic Scan (Assessment B)
- **Side-tab accent borders (`border-left` > 1px)**: 8 occurrences across `components.css:459`, `components.css:398`, `collector.css:11`, `account.css:154`, `account.css:252`, `account.css:289`, `universe.css:38`, `charts.css:108`.
- **Layout property animations (`transition: width`)**: 3 occurrences in `components.css:264` (search expand), `collector.css:71`, `performance.css:136`.
- **Craft floor violations**: Eyebrow/kicker labels (`.topbar-workspace-kicker`, `.poll-kicker`, `.readiness-kicker`, `.collector-kicker`), decorative section numbers (`<i>01</i>`–`<i>06</i>`), duplicate `:root` definitions in `tokens.css`.

## Priority Issues

- **[P0] Locked Cockpit: Disabled Emergency Action Controls**
  - Fix: Provide typed safety confirmation dialog and copyable CLI command generator.
  - Suggested Command: `/impeccable shape src/crypto_momentum_lab/operator_dashboard/static/index.html`

- **[P1] Crisis Context Fragmentation**
  - Fix: Auto-mounting Emergency Triage banner below topbar when readiness is `BLOCKED` or `REVIEW`.
  - Suggested Command: `/impeccable layout src/crypto_momentum_lab/operator_dashboard/static/styles/layout.css`

- **[P2] Token & Style Duplication Schizophrenia**
  - Fix: Merge duplicate `:root` in `tokens.css` into a single authoritative design system palette and deduplicate styles.
  - Suggested Command: `/impeccable distill src/crypto_momentum_lab/operator_dashboard/static/styles/tokens.css`

- **[P3] Mechanical Slop & Anti-patterns**
  - Fix: Remove `border-left` side-tab accents, eliminate `transition: width`, strip decorative eyebrows and section numbers.
  - Suggested Command: `/impeccable polish src/crypto_momentum_lab/operator_dashboard/static/styles/components.css`

## Persona Red Flags

- **Alex (Power Quant Operator)**: Cannot sort positions table by uPnL; no global keyboard shortcuts (1-6); order hashes cannot be copied.
- **Jordan (First-Timer)**: Overwhelmed by mixed-language abbreviations; responsive switcher hidden on smaller displays.
- **Morgan (Incident Responder)**: Cannot trigger emergency flatten from UI; must manually sum portfolio capital across 4 separate account cards during crash.
