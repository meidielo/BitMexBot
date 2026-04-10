# Lessons Learned — BitMexBot

## L01: Never disable structural rules based on small samples
**Context:** Settlement time filter reduced PF from 0.99→0.85 on 47 trades. Disabled it.
**Correction:** User: "You are discarding a structural rule based on noise."
**Rule:** If a rule is based on a real market mechanic, never toggle it off because performance dropped on <100 trades.

## L02: Inverse contracts must track BTC equity natively
**Context:** Used USD balance + end-conversion for XBTUSD backtest.
**Correction:** User: "Your backtest must track your account equity entirely in BTC."
**Rule:** `balance_btc` from start to finish. Position sizing: `contracts = risk_btc / abs(1/entry - 1/sl)`. PnL: `pnl_btc = contracts * (1/entry - 1/exit)` for LONG.

## L03: Verify every reported number before presenting
**Context:** `print_report` used first trade price ($8,948) instead of first candle price ($7,155) for starting BTC equity. Reported +5.2% when actual was -15.8%.
**Rule:** Double-check starting equity, PnL formulas, metric denominators. Trace the math manually on the first trade before trusting aggregate stats.

## L04: Funding Reset ≠ Funding Reversal
**Context:** V3 mean-reversion backtest searched for negative funding + short liquidation cascade in a bull market. Got zero LONG signals. Tried to short using positive funding + long liquidation cascade. 0% WR.
**Correction:** User: "In a strong structural uptrend, clearing leverage rarely causes funding to go negative. It simply resets from +0.05% back to +0.01%."
**Rule:** In a bull trend, don't wait for funding to reverse sign. The washout resets extreme funding to baseline, not to the opposite extreme. Trade the reset, not the reversal.

## L05: Don't combine mean-reversion trigger with trend-following filter
**Context:** Added EMA200 trend filter (bullish = close > EMA200) but kept mean-reversion signal (SHORT on positive funding extreme). Created logical paradox: filter demands bull regime, signal demands fading the bull. Zero tradeable signals.
**Correction:** User: "You took a mean-reversion trigger and slapped a trend-following filter on top of it. This creates a logical paradox."
**Rule:** Trend filter and signal direction must agree. Bull filter → LONG signals only. Bear filter → SHORT signals only. Mean reversion requires no trend filter, OR a range-bound filter.

## L06: Buy the blood of the greedy, not the panic of the fearful
**Context:** In bull market, positive funding + long liq cascade = retail longs got washed out. The correct trade is LONG (buy the dip after leverage clearing), not SHORT (fade the trend).
**Correction:** User: "Market makers push price down to hit their stops. That is when you enter LONG."
**Rule:** In a bull trend with extreme positive funding, a LONG liquidation spike is a buying opportunity. The over-leveraged are cleared out; the trend continues.

## L07: OI confirmation separates real washouts from fake-out wicks
**Context:** V4 without OI filter: 6 trades, 33% WR, PF 0.79. With OI filter: 4 trades, 50% WR, PF 1.58.
**Observation:** When OI increases on a liquidation spike day, new positions immediately replaced the liquidated ones — no real deleveraging occurred. When OI decreases, the leverage was actually destroyed.
**Rule:** Require negative OI delta on signal day. If OI rises despite liquidation spike, it's a fake-out — skip it.

## L08: 4 trades is proof of concept, not proof of profitability
**Context:** V4 produced PF 1.58 on 4 trades. Tempting to celebrate.
**Correction:** User: "A Profit Factor of 1.58 is entirely dependent on the specific depth of the wicks in those two winning trades. You cannot derive any mathematical confidence from these metrics."
**Rule:** Never derive statistical confidence from <30 trades. Don't tweak parameters to force more signals — that reintroduces false positives. Accept the sample limitation and let the forward collector accumulate data.

## L09: Backtests lie about execution in liquidity vacuums
**Context:** V4 backtest assumed perfect fill at next-day open. During cascade events, 5m candle ranges are 3-7× normal. Max 5m range on Nov 13 entry day was 1.71%.
**Correction:** User: "Your backtest assumes you get perfectly filled at the exact close. In reality, placing a market order during a cascade guarantees severe adverse slippage."
**Rule:** Always model slippage. Conservative: 0.3% entry, 0.1% exit. Impact on V4: PF dropped 1.58 → 1.45, BTC return 2.4% → 1.9%. Wins lost ~0.1R each. The edge survives but the margin is thinner than the frictionless backtest suggests.

## L10: Protect the data pipeline above all else
**Context:** 15m forward collector is building a data moat that Coinalyze won't retain (21-day rolling window). Every missed interval is permanently lost data.
**Rule:** Heartbeat file for external monitoring. Structured logging to DB. Request timeouts and retries. WAL + busy_timeout for crash safety. Check integrity periodically. The strategy can be redesigned; lost data cannot be recovered.

## L11: Slippage is not a constant — it's a function of order book destruction
**Context:** V4 backtest used 0.3% fixed slippage. User: "Your cost matrix is a fantasy. During a liquidation cascade, the bid side is annihilated. Market impact is an order of magnitude higher."
**Correction:** Fixed slippage is a lie during cascade events. The order book is depleted by the liquidation engine before your order arrives. Slippage is a dynamic function of position size × instantaneous liquidity, not a static percentage.
**Rule:** Always stress-test at catastrophic slippage (0.5R–0.8R). V4 breakeven: 3.26% entry slippage (≈0.54R). Below that, the edge survives. Above it, the edge was an illusion.

## L12: SL/TP must be anchored to fill price, not signal price
**Context:** V4 calculated SL/TP from signal day close, but actual entry was next-day open + slippage. With large slippage, fill was far from signal close, making SL distance larger and TP distance smaller than intended — silently degrading R:R.
**Rule:** Always recalculate SL and TP from the executed fill price. This preserves the intended ATR-based risk distance and R:R ratio regardless of slippage magnitude. Position sizing uses actual fill-to-SL distance.

## L13: Sharp cliffs in parameter sweeps are sample-size artifacts, not market features
**Context:** V4 slippage sweep showed PF 1.26 at 3.0% → PF 0.39 at 4.0%. Looks like a "cliff." In reality, with N=4 trades, the entire system's profitability hinges on one winning trade surviving its TP before SL. When slippage crosses the threshold that flips that single trade, the whole system dies instantly.
**Rule:** A robust strategy distributes returns across dozens of trades. If profitability depends on one or two specific trades navigating a narrow window, you have a fragile anomaly, not a tradeable edge. Do not deploy capital until N≥200 validates the distribution.

## L14: Entry slippage and exit slippage are asymmetric by nature
**Context:** Entries occur during liquidity vacuums (cascade events). Exits occur during normal market conditions (SL/TP hit days or weeks later). Modeling exit slippage as a fraction of entry slippage (~1/3) is structurally correct — the order book has rebuilt by exit time.
**Rule:** Never use symmetric slippage models. Entry during cascade = high impact. Exit during normal conditions = low impact. The 1/3 ratio is a reasonable heuristic for daily-timeframe trend-following.

## L15: Correlated assets do not provide independent samples
**Context:** Proposed adding ETH/SOL cascades to V4 to increase N. User: "During extreme leverage washouts, correlation across major crypto approaches 1.0. You are not increasing your sample size; you are just artificially inflating your trade count."
**Rule:** Adding correlated instruments to a macro strategy does not increase statistical power. If BTC cascades, ETH/SOL cascade simultaneously. N stays the same — you just have leveraged exposure to the same event.

## L16: Monte Carlo requires a representative seed distribution
**Context:** Proposed generating synthetic cascades from N=4 real events. User: "You cannot run a Monte Carlo simulation using an initial seed of four events. Perturbing four random wicks will only map the curve-fitted fantasy of your existing confirmation bias."
**Rule:** Monte Carlo is only valid when the seed distribution is representative of the population. With N=4, you have no idea what the tails look like. Synthetic perturbation of a tiny sample produces statistically meaningless results.

## L17: Building a better radar does not make more planes take off
**Context:** Proposed real-time cascade detection to "generate forward data points immediately." User: "A real-time detector does not cause cascades to happen. If the market produces 1.3 cascades per year, your system will sit silent for 364 days."
**Rule:** Do not confuse detection infrastructure with event frequency. A low-frequency macro strategy generates low-frequency signals regardless of how sophisticated the detection system is. To solve the N problem, you must target a high-frequency structural edge.

## L18: To solve N fast, target high-frequency mechanical edges
**Context:** V4 fires ~1.3 times/year. Reaching N=200 would take 150 years. User: "You must target a structural inefficiency that occurs at a high frequency. Funding is settled every 8 hours — 1,095 times a year."
**Rule:** When the bottleneck is sample size, pivot to a mechanically recurring market event. Funding settlement (3× daily) provides 1,095 independent events/year. N=200 is achievable in ~2 months. The edge may be smaller, but it's provable.

## L19: Publicly scheduled timestamps are fully arbitraged by HFT
**Context:** Funding settlement study showed 3.45× volume spike pre-settlement (mechanism is real) but zero tradeable price edge (p>0.05 on directional tests, win rates ~50%). Extreme funding events clustered in 2020-2021, near-zero in 2023+.
**Correction:** Market makers front-run the publicly scheduled settlement time, widen quotes, absorb the volume, and neutralize the price impact. Competing against latency arbitrageurs on a known timestamp is guaranteed loss.
**Rule:** Never build a strategy around a publicly scheduled, precisely timestamped market event. Institutional HFT has already internalized the risk. The mechanism exists; the edge does not.

## L20: Correlation is not co-integration
**Context:** Transitioning to pairs/statistical arbitrage. Two assets can be highly correlated (move in the same direction) yet drift infinitely apart in absolute value.
**Rule:** Pairs trading requires co-integration (Engle-Granger or Johansen test), not correlation. Co-integration guarantees the spread is stationary (mean-reverting). Trading a correlated-but-not-co-integrated spread leads to catastrophic drawdowns.

## L21: Major crypto pairs are correlated but not co-integrated
**Context:** Tested BTC/ETH, ETH/SOL, LTC/BTC, DOGE/XRP, LINK/ETH. All Hurst exponents ≈ 0.97 (deep trending). LTC/BTC passed EG (p=0.028) and Johansen but Hurst = 0.965 kills it. BTC/ETH is not even close (EG p=0.84).
**Rule:** Major crypto spreads drift with the macro cycle. They are directionally correlated but not level-stationary. Pairs trading in crypto requires either micro-cap structural pairs (protocol tokens with locked economic relationships) or cross-exchange arb, not cross-asset mean reversion on majors.

## L22: Hurst exponent is symmetrical — H > 0.5 validates momentum, not just invalidates mean-reversion
**Context:** Dismissed H ≈ 0.97 as "pairs trading fails." User: "A 160 IQ quant looks at H=0.97 and says 'this is one of the strongest persistence signals in modern finance.'"
**Rule:** H < 0.5 → mean-reversion (pairs). H > 0.5 → momentum (trend-following). H ≈ 0.97 across crypto spreads proves cross-sectional momentum is the structural inefficiency, not stat arb. Do not discard data because one thesis failed — invert the signal.

## L23: Chasing new alphas is procrastination when execution infrastructure is missing
**Context:** Ran funding settlement study and co-integration study instead of building V4 execution layer. Both studies were intellectually interesting but operationally irrelevant to deploying the only validated edge.
**Rule:** Strategy generation without execution capability is academic tourism. If you have a validated edge with a running data collector, the highest-EV engineering task is always the execution pipeline — not the next backtest.

## L24: When reducing turnover kills the return, the signal is noise not momentum
**Context:** Cross-sectional momentum backtest showed Sharpe 0.58 with 77% weekly turnover. Adding a 15% rank buffer cut turnover to 60% but Sharpe collapsed to 0.20, return from +45.8% to +7.5%. The "alpha" was short-term reversal churn, not structural momentum.
**Rule:** If dampening rebalance frequency destroys PnL, the signal is high-frequency noise that the algorithm was accidentally timing. True momentum persists through buffer periods. Test this by varying rebalance frequency before deploying.

## L25: Derivative universes are too curated for cross-sectional strategies
**Context:** BitMEX only lists perps for assets with massive existing liquidity. The 30-40 eligible symbols are structurally homogeneous (CSD stable at 7-9% across all regimes). True cross-sectional dispersion lives in micro-cap spot markets, not large-cap derivatives.
**Rule:** Cross-sectional strategies need large, diverse universes with genuine dispersion. A curated derivative exchange universe is too correlated and too arbitraged. CSD was -0.114 correlated with returns — completely independent.

## L26: Systematic elimination is the real product
**Context:** Killed V1-V3 indicators, V2 funding mean-reversion, V3 triple-condition, funding settlement arb, pairs/stat-arb, and cross-sectional momentum. Only V4 macro dip-buy survived.
**Rule:** The value of rigorous testing is not finding winners — it's confidently eliminating losers before they consume capital. Every dead strategy is a prevented loss. The survivor is stronger for having been the last one standing.

## L27: Infrastructure ≠ statistical validity (external audit, 2026-04-08)
**Context:** External audit found 16 items. Core finding: V4 has N=4 trades — PF 1.54 with 95% CI spanning 0.3 to 12+. Cannot distinguish edge from luck. OI filter may be overfitting (searched within N=6 for a better subset of 4). No OOS data was reserved. 15m execution loop contradicts 2-10 minute cascade thesis.
**Rule:** No amount of engineering excellence compensates for insufficient statistical evidence. Pre-register parameters before pulling new data. Expand sample via free historical sources (BitMEX public dumps 2016+, Binance OI) before considering mainnet deployment. Walk-forward validation mandatory. Log condition distances every loop — it's free forward data you're otherwise discarding.

## L30: V2 signal audit — funding regime structurally died mid-2024 (2026-04-09)

**Context:** Pre-Path-X audit of V2 signal (funding rate mean-reversion).
Question: is V2 broken or correctly strict? Ran `v2_signal_audit.py`
against 6,860 funding settlements (2020-01-01 → 2026-04-05, full XBTUSD
cache).

**Result — V2 is NOT broken. Fired 292 times historically at 0.05%
threshold. The signal logic is sound. The underlying regime died.**

Year-by-year % of settlements with |funding| >= 0.05%:
```
  2020: 8.7%   (95 fires — COVID crash + PlusToken era)
  2021: 11.9%  (130 fires — bull top retail euphoria)
  2022: 1.3%   (14 fires — post-LUNA deleveraging)
  2023: 1.6%   (18 fires — ETF accumulation phase)
  2024: 3.2%   (35 fires — halving + launch FOMO)
  2025: 0.1%   (1 fire  — STRUCTURAL DEATH)
  2026: 0.0%   (0 fires — max rate all year = 0.010%)
```

Max funding rate has collapsed from ±0.30% in 2021 to ±0.0569% in 2025
to ±0.0100% in 2026. Not a cyclical dip — a structural compression.

**Direction asymmetry:** 75.5% of ALL settlements are positive funding.
SHORT setups outnumber LONG setups 2.5:1 at 0.05% threshold. V2 was
always structurally a short-dominant strategy.

**Why regime died (hypothesis):** Institutional spot-perp basis trading
became mainstream in 2023. Funding rate edge has been fully arbitraged
by delta-neutral cash-and-carry funds. Matches L19 (publicly scheduled
mechanics get arbitraged) but extends it — even the underlying SETUP
CONDITION is compressed now, not just the tradeable edge.

**Rule:** Funding-rate-based strategies on BTC perpetuals are effectively
dead post-2024 regardless of specific thesis (mean-reversion, exhaustion,
settlement arb). The raw signal input — extreme funding — no longer
occurs. This applies across V2 (mean-reversion), V3 triple-condition,
V4 funding setup (also died — see year table, 0.03% threshold has only
7 fires in last 12m), and any planned Path X funding exhaustion strategy.

**Path X implication:** Before committing to funding exhaustion, note
that 0.03% setup level fires 7x in last 12m total. Any "N consecutive
settlements above threshold" rule will return N≈0 on recent data.
Path X would need to test on 2020-2022 data (where the regime existed)
and accept that the thesis may be historically valid but currently
unexploitable — same problem V4 has.

**Meta-finding:** This is the third strategy family confirmed dead by
regime change rather than flawed thesis. V4 (cascade dip-buy) needs
bull regime events that don't happen often enough. V2 (funding
mean-reversion) needs funding extremes that no longer occur. Vol regime
(L29) needs mean-reverting vol events that stopped mean-reverting.
A pattern: every dead strategy was thesis-valid in historical data
but broken in 2024-2026 market structure.

## L29: Vol Regime Mean-Reversion — DEAD (with salvage attempt) (2026-04-09)

**Thesis (V1):** Realized vol spike > 2σ above 90-day mean, then contraction
back to mean = directional entry. EMA200 for regime filter. Long in bull,
short in bear.

**V1 Result:** N=52, PF=0.95, Total R=-1.79, Max DD=-14.27R.
OOS N=30, Total R=-3.53 across 3 meaningful folds.
BULL WR 40% / BEAR WR 8.3% — looked like EMA200 had predictive power.

**Why V1 died:** Vol normalization tells you THAT vol is contracting, not
WHICH WAY price will move. Profitable in exactly one fold (2023-10 to
2025-01, steady bull run) — regime-dependent luck, not edge. In fold 3
(2025, also bull regime), 7/8 trades stopped out into sideways topping.

**Salvage attempt (V2 — 3 directional hypotheses tested):**

| Hypothesis | Rule | N | PF | Full R | OOS R |
|---|---|---|---|---|---|
| H1 Direction A | bull+fear→LONG, bear+blow-off→SHORT | 16 | 0.65 | -4.20 | -2.10 |
| H2 Always LONG | ignore regime, always long contraction | 53 | 1.28 | +9.20 | +1.47 |
| H2b Bear-only LONG | salvage hypothesis, bear regime only | 13 | 1.23 | +1.83 | -1.11 |
| H3 V1 Baseline | long bull / short bear | 52 | 0.95 | -1.79 | -3.53 |

**Key structural finding:** Vol spike quadrant distribution is asymmetric:
  - Bull+fear spikes: 16 (corrections in uptrends)
  - Bull+blow-off spikes: 44 (majority — FOMO tops)
  - Bear+fear spikes: 35 (capitulation)
  - Bear+blow-off spikes: 2 (dead cat bounces — essentially non-existent)

Direction A (H1) fails because bear+blow-off barely exists, and bull+fear
corrections don't reliably recover at 3R targets.

**Most interesting finding (H2):** "Always LONG on contraction" produces
PF 1.28 and positive OOS R=+1.47 — the FIRST positive OOS result in this
project. But fold 3 (2025-01→2026-04) collapses to -6.15R on 9 trades.
Both H2 and H3 lose fold 3, suggesting recent market structure (tariff
volatility, repeated re-spiking) broke the mean-reversion assumption.

**False salvage:** The initial "91.7% bear flip" observation was artifact.
Multiplying r_multiple by -1 assumes the flipped trade exits at the same
bar as the original. Real long/short pairs track differently through
TP/SL brackets. Proper re-simulation (H2b) showed bear-longs had WR 38.5%,
not 91.7%. Lesson: never trust a "what-if flipped" analysis without
re-running the actual trade simulation.

**Infrastructure gain:** First strategy in this project to achieve N=52
in backtest with meaningful walk-forward. The 5m OHLCV → daily realized
vol pipeline works, walk-forward framework works, pre-registration
workflow works. All reusable.

**Rule:** Vol contraction alone is a timing signal, not a directional one.
Any vol-based strategy needs either (a) a separate directional signal
independent of vol shape, or (b) options implementation where direction
doesn't matter. The "fear vs blow-off" decomposition doesn't work because
the quadrants are too asymmetric. Don't revive this thesis without a
genuinely independent direction feature — and even then, the fold-3
collapse suggests vol-regime strategies may be structurally broken in
post-2024 BTC markets where vol events don't mean-revert cleanly.

**Fold-3 autopsy (2026-04-09) — final nail:**

Pre-committed kill rule: filter must catch >=6/8 fold-3 losses AND remove
<3 of 20 folds 1+2 winners. Five hypotheses tested:

| # | Hypothesis | Best losses caught | F1+2 wins removed | Pass? |
|---|---|---|---|---|
| A | Vol re-spike within 5d | 1/8 | 8/20 | NO |
| B | ATR ratio >1.3-2.0x | 3/8 (@1.3) | 13/20 | NO |
| C | Calendar cluster | max 2/mo | — | NO |
| D | Weak contraction (vol_z > -0.75) | 7/8 | 19/20 | NO (kill switch, not filter) |
| E | Fast stop-out <=3d | 1/8 | — | NO |

**ZERO hypotheses cleared the bar.** Fold-3 losses were:
- Evenly spread (no calendar clustering)
- At normal ATR levels (not regime-too-wide)
- Held for normal durations (median 9d, same as winners)
- Uncorrelated with subsequent vol re-spikes

The losses are **structurally indistinguishable from the winners** on
every feature available. This is what noise looks like. The vol-regime
family is CLOSED. No H3. No further iteration.

**Meta-lesson (process):** Pre-committing the kill rule before looking at
data prevented spawning a third iteration on the second miss. Without the
rule, Hypothesis D (7/8 losses caught!) would have looked like a
discovery instead of the kill switch it actually is. The rule worked.
Use this pattern for every salvage attempt going forward.

## L28: Free historical liquidation data has a hard ceiling (2026-04-09)
**Context:** Implemented audit's recommendation to expand backtest to 2019-2026 via "BitMEX public dumps + Binance free API". Reality:
  1. **BitMEX public dumps DO NOT contain liquidation flags** — only `trdType=Regular` in trade.csv. The audit was wrong about this.
  2. **Binance Futures API is geo-blocked** in this region (HTTP 403). Bybit also blocks (TLS handshake failure).
  3. **Coinalyze daily data already extends to 2023-04-07** — we've been using everything available (1096 days, ~3 years).
  4. **Walk-forward on 3 years of data with N=4 trades** produces 1 OOS trade across 4 rolling folds. Statistically meaningless.
**Rule:** Free historical liquidation data effectively maxes out at ~3 years via Coinalyze. Going further requires either paid services (CryptoQuant ~$50/mo) or building from current point forward. The audit's "expand sample size" path is blocked at the data layer for free options. The only honest paths to more N: (a) buy historical data, (b) wait for forward data accumulation, (c) cross-asset replication if exchanges aren't geo-blocked, (d) test mechanism components on non-liquidation data with larger N.
