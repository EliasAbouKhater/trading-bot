#!/usr/bin/env python3
"""Cron runner — advances the rebalance state machine one step per run.

Runs hourly (0 * * * *).

Notifications:
  - One daily Telegram digest at 05:00 UTC (09:00 Dubai), always.
    Contains: uptime, regime, portfolio, positions, status, last rebalance summary.
  - Immediate Telegram only on unhandled crash (bot is dead — you need to know now).
  - No per-event notifications. Everything batches into the daily digest.

Logs to ~/trading-bot/logs/rebalance.log.
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

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


def send_telegram(text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")


def main():
    log.info("=== Cycle start ===")

    try:
        cfg         = yaml.safe_load((ROOT / "config.yaml").read_text())
        rb_cfg      = cfg["rebalance"]
        allocations = rb_cfg["allocations"]

        from core.broker import AlpacaBroker
        from core.live_rebalance import (
            advance_rebalance_cycle, build_daily_digest,
            should_send_digest, load_state, save_state,
        )

        broker = AlpacaBroker(paper=cfg["alpaca"].get("paper", True))

        # ── Advance state machine ──────────────────────────────────────────────
        result = advance_rebalance_cycle(
            allocations     = allocations,
            capital_to_deploy = rb_cfg["initial_capital"],
            monthly_dca     = rb_cfg["monthly_dca"],
            broker          = broker,
            dry_run         = False,
            force           = False,
            bull_days       = rb_cfg.get("bull_days", 126),
            bear_days       = rb_cfg.get("bear_days", 21),
            drift_cfg       = rb_cfg.get("drift", {}),
        )

        action = result.get("action", "?")
        phase  = result.get("phase", "?")
        orders = result.get("orders") or result.get("executed_now") or []
        log.info(f"Action: {action} | Phase: {phase} | Orders: {len(orders)}")

        for o in orders:
            fee_str = f" (fee ~${o['est_fee']:.3f})" if o.get("est_fee") else ""
            log.info(f"  {o.get('side','?')} {o.get('symbol','?')} "
                     f"${o.get('notional', 0):.2f}{fee_str} → {o.get('status', '?')}")

        if action == "DECISION_DEFERRED":
            deferred = result.get("deferred_until_open", [])
            log.info(f"  {len(deferred)} order(s) deferred — market opens {result.get('next_open','?')[:16]}")

        if action == "CONFIRMED":
            aw = result.get("actual_weights", {})
            if aw:
                log.info("  Fills confirmed. Actual weights: " +
                         ", ".join(f"{s}={v*100:.1f}%" for s, v in aw.items()))

        if result.get("note"):
            log.info(f"  Note: {result['note']}")

        macro = result.get("macro", {})
        if macro:
            log.info(f"  Regime: {macro.get('regime')} "
                     f"(SPY ${macro.get('spy_price')} / SMA200 ${macro.get('sma_200')}, "
                     f"{macro.get('gap_pct', 0):+.1f}%)"
                     + (" [fallback]" if macro.get("fallback") else ""))

        # ── Daily digest ───────────────────────────────────────────────────────
        state = load_state()
        if should_send_digest(state):
            log.info("Sending daily digest...")
            msg = build_daily_digest(state, broker, allocations, cfg)
            send_telegram(msg)
            state["last_digest_date"] = datetime.utcnow().date().isoformat()
            save_state(state)
            log.info("Daily digest sent.")

    except Exception as e:
        err = traceback.format_exc()
        log.error(f"Cycle crashed: {e}\n{err}")
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        send_telegram(
            f"*Trading Bot* — {now}\n"
            f"❌ *CRASH*\n`{str(e)[:300]}`"
        )
        sys.exit(1)

    log.info("=== Cycle done ===\n")


if __name__ == "__main__":
    main()
