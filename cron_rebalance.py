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
import re
import subprocess
import sys
import logging
import traceback
from datetime import datetime, timezone
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


VENV_PYTHON = ROOT / "venv" / "bin" / "python"
RUN_CMD     = f"cd {ROOT} && {VENV_PYTHON} {ROOT / 'cron_rebalance.py'} >> {ROOT / 'logs' / 'cron.log'} 2>&1"


def schedule_at_open(next_open_iso: str, state: dict, save_state_fn) -> None:
    """Schedule a one-shot run at the exact market open time using `at`.
    Cancels any previously scheduled at-open job first.
    """
    # Cancel previous at-open job if any
    prev_job = state.get("at_open_job_id")
    if prev_job:
        subprocess.run(["atrm", str(prev_job)], capture_output=True)
        state.pop("at_open_job_id", None)

    # Parse next_open — Alpaca returns UTC ISO string
    next_open = datetime.fromisoformat(next_open_iso).astimezone()
    at_time   = next_open.strftime("%H:%M %Y-%m-%d")  # HH:MM YYYY-MM-DD

    result = subprocess.run(
        ["at", at_time],
        input=RUN_CMD, text=True, capture_output=True,
    )
    m = re.search(r"job (\d+)", result.stderr)
    if m:
        state["at_open_job_id"] = int(m.group(1))
        save_state_fn(state)
        log.info(f"Scheduled at-open job #{m.group(1)} for {at_time}")
    else:
        log.warning(f"at scheduling failed: {result.stderr.strip()}")


def cancel_at_open_job(state: dict, save_state_fn) -> None:
    """Cancel the pending at-open job if one exists."""
    job_id = state.get("at_open_job_id")
    if job_id:
        subprocess.run(["atrm", str(job_id)], capture_output=True)
        state.pop("at_open_job_id", None)
        save_state_fn(state)
        log.info(f"Cancelled at-open job #{job_id}")


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
            deferred   = result.get("deferred_until_open", [])
            next_open  = result.get("next_open", "")
            log.info(f"  {len(deferred)} order(s) deferred — market opens {next_open[:16]}")
            if next_open:
                state = load_state()
                schedule_at_open(next_open, state, save_state)

        if action in ("REBALANCE", "INITIAL_BUY", "CONFIRMED", "AWAITING_FILLS"):
            # Transitioned out of PENDING_MARKET_OPEN — cancel any at-open job
            state = load_state()
            if state.get("at_open_job_id"):
                cancel_at_open_job(state, save_state)

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
                     f"(SPY ${macro.get('spy_price')} / EMA200 ${macro.get('ema_200')}, "
                     f"{macro.get('gap_pct', 0):+.1f}%)"
                     + (" [fallback]" if macro.get("fallback") else ""))

        # ── Daily digest ───────────────────────────────────────────────────────
        state = load_state()
        if should_send_digest(state):
            log.info("Sending daily digest...")
            msg = build_daily_digest(state, broker, allocations, cfg)
            send_telegram(msg)
            state["last_digest_date"] = datetime.now(timezone.utc).date().isoformat()
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
