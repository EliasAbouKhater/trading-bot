#!/usr/bin/env python3
"""Cron runner for automated rebalancing.

Runs daily. Checks both time-based and drift-based triggers.
Sends Telegram notification to admin:
  - SKIP: batched summary every SKIP_SUMMARY_DAYS days (default 3)
  - REBALANCE / INITIAL_BUY: always immediate
  - ERROR: always immediate
Logs to ~/trading-bot/logs/rebalance.log.
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

# Project root
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

# Logging
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "rebalance.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ADMIN_ID  = int(os.environ["TELEGRAM_ADMIN_ID"])

# SKIP notifications are batched — only send a summary every N days.
# Actual rebalances and errors always notify immediately.
SKIP_SUMMARY_DAYS = 3


def send_telegram(text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")


def compute_drift_thresholds(allocations: dict, drift_cfg: dict) -> dict:
    """Compute per-asset vol-based drift thresholds from recent price history."""
    import numpy as np
    import yfinance as yf

    symbols = [s for s in allocations if s != "CASH"]
    multiplier = drift_cfg.get("vol_multiplier", 0.20)
    min_pct    = drift_cfg.get("min_pct", 2.0)
    max_pct    = drift_cfg.get("max_pct", 20.0)

    thresholds = {}
    for symbol in symbols:
        try:
            ticker = symbol.replace("/", "-")
            df = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=True)
            if len(df) < 20:
                thresholds[symbol] = min_pct
                continue
            returns = df["Close"].pct_change().dropna()
            ann_vol = float(returns.std()) * (252 ** 0.5) * 100
            raw = multiplier * ann_vol
            thresholds[symbol] = max(min_pct, min(max_pct, raw))
        except Exception:
            thresholds[symbol] = min_pct
    return thresholds


def check_drift(positions: list, allocations: dict, portfolio_value: float,
                thresholds: dict, regime: str,
                bull_factor: float, bear_factor: float) -> list:
    """Return list of assets that breached their drift threshold."""
    regime_factor = bull_factor if regime == "BULL" else bear_factor
    violators = []
    pos_map = {p["symbol"]: p for p in positions}

    for symbol in [s for s in allocations if s != "CASH"]:
        pos_sym = symbol.replace("/", "")
        market_val = float(pos_map[pos_sym]["market_value"]) if pos_sym in pos_map else 0.0
        current_weight = (market_val / portfolio_value) * 100 if portfolio_value > 0 else 0
        target_weight  = allocations[symbol] * 100
        drift = abs(current_weight - target_weight)
        threshold = thresholds.get(symbol, 5.0) * regime_factor
        if drift > threshold:
            violators.append({
                "symbol": symbol,
                "drift": round(drift, 2),
                "threshold": round(threshold, 2),
                "current_pct": round(current_weight, 2),
                "target_pct": round(target_weight, 2),
            })
    return violators


def main():
    log.info("=== Rebalance check started ===")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    try:
        cfg = yaml.safe_load((ROOT / "config.yaml").read_text())
        rb_cfg   = cfg["rebalance"]
        drift_cfg = rb_cfg.get("drift", {})
        allocations = rb_cfg["allocations"]
        capital     = rb_cfg["initial_capital"]
        monthly_dca = rb_cfg["monthly_dca"]

        from core.broker import AlpacaBroker
        from core.live_rebalance import (
            run_live_rebalance, get_macro_regime, load_state, save_state
        )

        broker = AlpacaBroker(paper=cfg["alpaca"].get("paper", True))
        acct   = broker.get_account()
        state  = load_state()
        macro  = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))
        regime = macro["regime"]
        if macro.get("fallback"):
            log.warning(f"get_macro_regime() fell back to last known regime: {regime} (network/data issue)")

        # Use deployed capital as portfolio value (not total equity incl. idle cash)
        positions = broker.get_positions()
        invested_value = sum(float(p["market_value"]) for p in positions)
        cash_alloc = allocations.get("CASH", 0)
        portfolio_value = invested_value + (float(acct["cash"]) * cash_alloc)
        if portfolio_value < 1.0:
            portfolio_value = capital  # fallback to config capital if no positions yet

        log.info(f"Deployed: ${portfolio_value:,.2f} | Total equity: ${float(acct['equity']):,.2f} | "
                 f"Regime: {regime} (SPY ${macro['spy_price']} vs SMA200 ${macro['sma_200']})")

        # Check drift
        thresholds = compute_drift_thresholds(allocations, drift_cfg)
        violators = check_drift(
            positions, allocations, portfolio_value, thresholds, regime,
            drift_cfg.get("bull_factor", 1.5),
            drift_cfg.get("bear_factor", 0.7),
        )

        drift_triggered = len(violators) > 0
        if drift_triggered:
            log.info(f"Drift trigger: {[v['symbol'] for v in violators]}")
            for v in violators:
                log.info(f"  {v['symbol']}: {v['current_pct']:.1f}% vs target "
                         f"{v['target_pct']:.1f}% (drift {v['drift']:.1f}pp > {v['threshold']:.1f}pp)")

        # If drift fired but there are already open orders, suppress force —
        # the pending fills will move the portfolio toward target on their own.
        if drift_triggered:
            open_orders = broker.get_orders("open")
            if open_orders:
                log.info(f"Drift suppressed: {len(open_orders)} open order(s) still pending — waiting for fills")
                drift_triggered = False

        # Run rebalance (force=True if drift triggered, else let time logic decide)
        result = run_live_rebalance(
            allocations=allocations,
            capital_to_deploy=capital,
            monthly_dca=monthly_dca,
            broker=broker,
            dry_run=False,
            force=drift_triggered,
            bull_days=cfg.get("rebalance", {}).get("bull_days", 126),
            bear_days=cfg.get("rebalance", {}).get("bear_days", 21),
        )

        action  = result["action"]
        orders  = result.get("orders", [])
        log.info(f"Action: {action} | Orders: {len(orders)}")
        for o in orders:
            fee_str = f" (fee ~${o['est_fee']:.3f})" if o.get('est_fee') else ""
            log.info(f"  {o['side']} {o['symbol']} ${o.get('notional', 0):.2f}{fee_str} → {o.get('status', '?')}")

        # Build Telegram message
        if action == "SKIP":
            # Accumulate skips — only send a summary every SKIP_SUMMARY_DAYS days
            state = load_state()
            skip_count = state.get("skip_count", 0) + 1
            last_skip_notif = state.get("last_skip_notification")

            if last_skip_notif:
                days_since = (datetime.now() - datetime.fromisoformat(last_skip_notif)).days
            else:
                days_since = SKIP_SUMMARY_DAYS  # first run: treat as due

            state["skip_count"] = skip_count
            if days_since >= SKIP_SUMMARY_DAYS:
                msg = (
                    f"*Trading Bot* — {now}\n"
                    f"⏭ Skipped ({skip_count}× in the last {days_since} day(s))\n"
                    f"_{result.get('reason', '')}_\n"
                    f"Market: {regime} (SPY ${macro['spy_price']} / SMA200 ${macro['sma_200']}, {macro['gap_pct']:+.1f}%)\n"
                    f"Portfolio: ${portfolio_value:,.2f}"
                )
                state["last_skip_notification"] = datetime.now().isoformat()
                state["skip_count"] = 0
                save_state(state)
                send_telegram(msg)
                log.info(f"Skip summary sent ({skip_count} skips over {days_since} days)")
            else:
                save_state(state)
                log.info(
                    f"SKIP #{skip_count} — skip notification suppressed "
                    f"(next summary in {SKIP_SUMMARY_DAYS - days_since} day(s))"
                )
        else:
            trigger = "DRIFT" if drift_triggered else f"TIME ({regime})"
            order_lines = ""
            for o in orders:
                status = o.get("status", "?")
                amt = f"${o.get('notional', 0):.2f}"
                fee_str = f" (~${o['est_fee']:.3f} fee)" if o.get('est_fee') else ""
                order_lines += f"\n  {'🟢' if o['side']=='BUY' else '🔴'} {o['side']} {o['symbol']} {amt}{fee_str} — {status}"

            drift_lines = ""
            if violators:
                drift_lines = "\nDrift breaches:"
                for v in violators:
                    drift_lines += f"\n  {v['symbol']}: {v['drift']:.1f}pp > {v['threshold']:.1f}pp"

            msg = (
                f"*Trading Bot* — {now}\n"
                f"{'🔄' if orders else '✅'} *{action}* (trigger: {trigger})\n"
                f"Market: {regime} (SPY ${macro['spy_price']} / SMA200 ${macro['sma_200']}, {macro['gap_pct']:+.1f}%)\n"
                f"Portfolio: ${portfolio_value:,.2f}"
                f"{drift_lines}"
                f"\nOrders ({len(orders)}):{order_lines if order_lines else ' none'}"
            )
            send_telegram(msg)
            log.info("Telegram notification sent")

    except Exception as e:
        err = traceback.format_exc()
        log.error(f"Rebalance check failed: {e}\n{err}")
        send_telegram(
            f"*Trading Bot* — {now}\n"
            f"❌ *ERROR*\n`{str(e)[:300]}`"
        )
        sys.exit(1)

    log.info("=== Rebalance check done ===\n")


if __name__ == "__main__":
    main()
