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
