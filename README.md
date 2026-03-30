# Trading Bot

A regime-aware portfolio rebalancing bot with backtesting, paper trading, and a live dashboard. Connects to [Alpaca](https://alpaca.markets) for order execution.

---

## How It Works

### 1. Macro Regime Detection

Every run, the bot fetches SPY and computes its 200-day SMA. This determines the market regime:

- **BULL**: SPY > SMA-200 → rebalance less frequently, let winners run
- **BEAR**: SPY < SMA-200 → rebalance more frequently, tighten drift thresholds

### 2. Rebalancing Triggers

Rebalancing fires when **either** condition is met:

| Trigger | Description |
|---------|-------------|
| **Time** | Every ~126 trading days (BULL) or ~21 trading days (BEAR) |
| **Drift** | Any asset deviates from its target allocation beyond a volatility-adjusted threshold |

Drift thresholds are computed per-asset from 1-year historical volatility:
```
threshold = vol_multiplier × annualized_volatility
           clamped to [min_pct, max_pct]
           × regime_factor (wider in BULL, tighter in BEAR)
```

### 3. Target Allocations

Configured in `config.yaml`. Example (edit freely):

```yaml
allocations:
  SPY: 0.30    # 30% — broad US market
  QQQ: 0.10    # 10% — tech-heavy
  VGK: 0.10    # 10% — international
  GLD: 0.25    # 25% — gold hedge
  BTC/USD: 0.15  # 15% — Bitcoin
  ETH/USD: 0.10  # 10% — Ethereum
```

### 4. DCA (Dollar-Cost Averaging)

Configured via `monthly_dca` in `config.yaml`. Once per calendar month, the configured amount is added to the portfolio value before calculating target weights, distributing the injection proportionally across all assets.

### 5. Order Execution

- Sells execute first (frees up cash), then buys
- Alpaca fractional shares used for stocks; crypto is traded natively
- Dust trades under $1 are skipped

---

## Strategies (Paper Trading / Backtesting)

Three pluggable signal strategies for active paper trading:

| Strategy | Description | Key Parameters |
|----------|-------------|----------------|
| `ma_crossover` | Golden/death cross on short vs long SMA | `short_window`, `long_window` |
| `grid` | Buy/sell at fixed price levels within a range | `grid_low`, `grid_high`, `num_grids` |
| `pairs` | Mean-reversion on the spread between two correlated assets | `entry_z`, `exit_z`, `lookback` |

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API keys

```bash
cp .env.example .env
# Edit .env and fill in your Alpaca API credentials
```

`.env` format:
```
APCA_API_KEY_ID=your_key
APCA_API_SECRET_KEY=your_secret
```

### 3. Edit config

```bash
cp config.yaml config.yaml  # edit allocations, capital, DCA amount
```

---

## Usage

```bash
# Backtest all strategies and compare
python3 run.py backtest --compare

# Backtest a single strategy
python3 run.py backtest --strategy ma_crossover

# Check paper account status
python3 run.py account

# Run a live paper trading signal check
python3 run.py paper --strategy ma_crossover

# Launch the dashboard (port 5050)
python3 app.py
```

### Automated Rebalancing (cron)

```bash
# Run manually
python3 cron_rebalance.py

# Add to crontab (daily at market open)
0 14 * * 1-5 /path/to/venv/bin/python3 /path/to/trading-bot/cron_rebalance.py
```

Optional Telegram notifications: set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_ID` in `.env`. Skip notifications are batched (default: every 3 days). Rebalance events and errors always notify immediately.

---

## Risk Management

Configurable in `config.yaml` under `risk:`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_risk_per_trade_pct` | 20% | Max portfolio % per trade |
| `stop_loss_pct` | 8% | Stop-loss trigger |
| `take_profit_pct` | 12% | Take-profit trigger |
| `max_drawdown_pct` | 30% | Portfolio kill switch |
| `max_open_positions` | 5 | Position limit |

---

## Adding a Strategy

1. Create `strategies/my_strategy.py` inheriting `Strategy` from `strategies/base.py`
2. Implement `generate_signals(df) -> df` — adds a `signal` column: `1` = buy, `-1` = sell, `0` = hold
3. Register it in `strategies/__init__.py`
4. Add a config section under `strategies:` in `config.yaml`

---

## Stack

- Python 3.10+
- [Alpaca Markets](https://alpaca.markets) (broker API — paper and live)
- pandas, yfinance, ta (technical analysis)
- Flask (dashboard)
