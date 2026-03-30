# Investment Philosophy

## What This Is — And What It Isn't

This is **not a day trading bot**. It does not watch charts. It does not try to predict what the market will do tomorrow. It does not make dozens of trades a week chasing short-term price movements.

This is a **long-term investing system** — the kind that runs in the background, makes a handful of decisions per year, and compounds quietly over time.

---

## The Benchmark: Buy and Hold

The standard way most people invest is called **buy and hold**: you put money into a diversified mix of assets (stocks, gold, crypto) and you just... hold. You don't react to market crashes. You don't sell when things look bad. You wait.

This works. Over long enough time horizons, the market goes up. The problem is the ride is brutal — during a crash, you can watch your portfolio drop 30–50% and have nothing to do but wait.

**This bot is buy and hold — but smarter.**

---

## What the Bot Actually Does

### Rebalancing

Imagine you set a target: 50% stocks, 25% gold, 25% crypto. A year later, crypto has doubled — now it's 40% of your portfolio. The bot notices this and **sells some crypto, buys more of what fell behind**, bringing everything back to your target.

This sounds simple. The effect is powerful: you're systematically **selling high and buying low** without needing to predict anything. You're just enforcing discipline.

### Why It Beats Buy and Hold

- **When markets go up:** rebalancing harvests gains from your winners and redeploys them into assets that haven't caught up yet. You capture momentum without chasing it.
- **When markets go down:** the bot detects the shift (more on this below) and rebalances more frequently with tighter tolerances — trimming overexposed assets before losses compound. You lose less than someone who just holds.

Over time, the combination of capturing gains on the way up and limiting losses on the way down compounds into meaningfully better returns than pure buy and hold.

---

## The Regime System (Bull vs Bear)

The bot checks one simple thing: **is the S&P 500 above or below its 200-day average?**

- If it's **above** → we're likely in a bull market. Rebalance less often. Let winners run.
- If it's **below** → we're likely in a bear market. Rebalance more often. React faster to drift.

This is one of the oldest and most reliable signals in investing. It doesn't predict the future — it just describes the current environment and adjusts behavior accordingly.

---

## Dollar-Cost Averaging (DCA)

Instead of putting all your money in at once (and risking terrible timing), DCA means **adding a fixed amount every month**, no matter what the market is doing.

When prices are high, your fixed amount buys less. When prices are low, it buys more. Over time, this averages out your entry price and removes the anxiety of trying to "time" the market.

---

## Glossary — Plain English

| Term | What it means |
|------|---------------|
| **Portfolio** | Your collection of investments — stocks, gold, crypto, etc. |
| **Allocation** | What percentage of your portfolio is in each asset. E.g., 30% in stocks. |
| **Rebalancing** | Adjusting your portfolio back to your target percentages when things drift. |
| **Drift** | How far an asset has moved away from its target allocation. |
| **Bull market** | A period when prices are generally rising. |
| **Bear market** | A period when prices are generally falling. |
| **ETF** | Exchange-Traded Fund — a single stock that holds many assets inside it. SPY holds 500 US companies. GLD holds gold. |
| **DCA** | Dollar-Cost Averaging — investing a fixed amount on a regular schedule. |
| **Drawdown** | The biggest drop from a peak. A 20% drawdown means your portfolio fell 20% from its high before recovering. |
| **Sharpe Ratio** | A measure of return relative to risk. Higher is better. Above 1.0 is good. |
| **Buy and Hold** | The simplest investing strategy: buy a diversified portfolio and never sell regardless of market conditions. |
| **Regime** | The current state of the market — bull or bear — used to adapt strategy. |
| **SMA-200** | The 200-day Simple Moving Average — the average price over the last 200 days. A classic indicator of long-term trend direction. |

---

## Before You Put Real Money In

**Start with a paper account and backtest first.**

Alpaca offers free paper trading — a simulated account with fake money that behaves exactly like the real thing. Run the bot there for a few weeks. Watch how it behaves. Get comfortable with it.

Before that, run a backtest on historical data. The bot includes a backtesting engine for exactly this purpose — test your chosen allocations across different market periods (a bull run, a crash, a sideways market) and see how the strategy would have performed. If the numbers make you nervous, adjust your allocations before any real money is involved.

The sequence: **backtest → paper trade → real money.** Never skip steps.

---

## The Bottom Line

This bot is not trying to beat hedge funds or predict the next crash. It's trying to be a disciplined, automated version of what every long-term investor knows they should do but rarely does consistently:

> **Diversify. Rebalance regularly. Add money every month. React to market conditions without panicking.**

Set it up once. Let it run.

---

## Disclaimer

Nothing in this document or this project is investing advice. The strategy described here is shared for educational and informational purposes only.

The developer does not take responsibility for any financial loss — or gain — that results from using this software. Markets are unpredictable. Past performance, even in backtests, is not a guarantee of future results. You are solely responsible for any financial decisions you make.

When in doubt, consult a licensed financial advisor.

---

## Get Involved

This project is a work in progress and I'm genuinely open to ideas, feedback, and collaboration.

If you have thoughts on the strategy, the code, or the approach — **open an issue or leave a comment**. If you're building something similar or thinking about it, **feel free to reach out**. The goal is to make long-term, automated investing more accessible, and that's easier with more perspectives.
