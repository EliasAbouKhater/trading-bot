"""Live Adaptive Rebalancer — state-machine architecture.

Phases:
  IDLE               → check time/drift triggers, compute decision, route by market hours
  PENDING_MARKET_OPEN → crypto sells done; wait for NYSE open, then refresh + execute rest
  EXECUTING          → orders submitted; confirm fills, snapshot actual weights → IDLE

Every cron run calls advance_rebalance_cycle() which advances one step.
State is persisted in data/rebalance_state.json across runs.

Decision/execution separation:
  DECIDE → RECORD → EXECUTE
  State is written before any order is placed.  Execution errors do not roll
  back the recorded decision.

Post-rebalance baseline:
  After fills confirm, actual achieved weights are stored as the drift
  reference for the next cycle.  Order sizes still target config allocations.
  Baseline resets to config targets when the next rebalance decision fires.
"""

import json
import os
from datetime import datetime
from typing import Optional

import yfinance as yf

from core.broker import AlpacaBroker, is_crypto


STATE_FILE = "data/rebalance_state.json"
MIN_CRYPTO_NOTIONAL = 10.0
ROUND_TRIP_FEE  = {"stock": 0.0010, "crypto": 0.0060}
FEE_COVERAGE    = 2.0
STALE_ORDER_DAYS = 2
POOL_THRESHOLD   = 20.0

PHASE_IDLE         = "IDLE"
PHASE_PENDING_OPEN = "PENDING_MARKET_OPEN"
PHASE_EXECUTING    = "EXECUTING"


# ── State I/O ─────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


# ── Macro regime ──────────────────────────────────────────────────────────────

def get_macro_regime(last_known_regime: str = "BULL") -> dict:
    """Fetch SPY 200-day SMA. Falls back to last_known_regime on any failure."""
    try:
        df = yf.download("SPY", period="1y", interval="1d", progress=False)
        if len(df) < 200:
            return {"regime": last_known_regime, "spy_price": 0, "sma_200": 0,
                    "gap_pct": 0, "fallback": True}
        sma_200   = float(df["Close"].rolling(200).mean().iloc[-1].iloc[0])
        spy_price = float(df["Close"].iloc[-1].iloc[0])
        gap_pct   = (spy_price - sma_200) / sma_200 * 100
        return {
            "regime":    "BULL" if spy_price > sma_200 else "BEAR",
            "spy_price": round(spy_price, 2),
            "sma_200":   round(sma_200, 2),
            "gap_pct":   round(gap_pct, 2),
        }
    except Exception:
        return {"regime": last_known_regime, "spy_price": 0, "sma_200": 0,
                "gap_pct": 0, "fallback": True}


# ── Symbol helpers ────────────────────────────────────────────────────────────

def alpaca_symbol(symbol: str) -> str:
    return symbol


def position_symbol(symbol: str) -> str:
    """Config symbol → Alpaca position key (no slash for crypto)."""
    return symbol.replace("/", "")


# ── Drift thresholds (cached daily in state) ──────────────────────────────────

def compute_drift_thresholds(allocations: dict, drift_cfg: dict) -> dict:
    """Per-asset vol-based thresholds from 1y daily returns.
    Results are cached in state for the day — only recomputed once per 24h.
    """
    symbols    = [s for s in allocations if s != "CASH"]
    multiplier = drift_cfg.get("vol_multiplier", 0.20)
    min_pct    = drift_cfg.get("min_pct", 2.0)
    max_pct    = drift_cfg.get("max_pct", 20.0)

    thresholds = {}
    for symbol in symbols:
        try:
            ticker = symbol.replace("/", "-")
            df = yf.download(ticker, period="1y", interval="1d",
                             progress=False, auto_adjust=True)
            if len(df) < 20:
                thresholds[symbol] = min_pct
                continue
            returns = df["Close"].pct_change().dropna()
            ann_vol = float(returns.std()) * (252 ** 0.5) * 100
            thresholds[symbol] = max(min_pct, min(max_pct, multiplier * ann_vol))
        except Exception:
            thresholds[symbol] = min_pct
    return thresholds


def _get_thresholds(state: dict, allocations: dict, drift_cfg: dict) -> dict:
    """Return cached thresholds if computed today, else recompute and cache."""
    today  = datetime.now().date().isoformat()
    cached = state.get("cached_thresholds", {})
    if cached.get("computed_at") == today:
        return cached["thresholds"]
    thresholds = compute_drift_thresholds(allocations, drift_cfg)
    state["cached_thresholds"] = {"thresholds": thresholds, "computed_at": today}
    return thresholds


# ── Portfolio snapshot ────────────────────────────────────────────────────────

def _snapshot_portfolio(broker: AlpacaBroker, allocations: dict,
                        capital_to_deploy: float) -> dict:
    """Fetch live account, positions, and latest prices. Returns a snapshot dict."""
    acct          = broker.get_account()
    positions     = broker.get_positions()
    pos_map       = {p["symbol"]: p for p in positions}
    asset_symbols = [s for s in allocations if s != "CASH"]

    prices = {}
    for symbol in asset_symbols:
        try:
            prices[symbol] = broker.get_latest_price(symbol)
        except Exception as e:
            print(f"  [WARN] Price unavailable for {symbol}: {e}")
            prices[symbol] = None

    current_holdings = {}
    for symbol in asset_symbols:
        pos_sym = position_symbol(symbol)
        if pos_sym in pos_map:
            current_holdings[symbol] = {
                "qty":   pos_map[pos_sym]["qty"],
                "value": pos_map[pos_sym]["market_value"],
                "price": prices.get(symbol) or pos_map[pos_sym]["current_price"],
            }
        else:
            current_holdings[symbol] = {"qty": 0, "value": 0,
                                         "price": prices.get(symbol) or 0}

    invested_value = sum(h["value"] for h in current_holdings.values())
    is_initial     = invested_value < 10 and capital_to_deploy > 0
    portfolio_value = (capital_to_deploy if is_initial
                       else invested_value + float(acct["cash"]) * allocations.get("CASH", 0))

    return {
        "acct": acct, "positions": positions,
        "current_holdings": current_holdings,
        "invested_value": invested_value,
        "portfolio_value": portfolio_value,
        "is_initial": is_initial,
    }


# ── Order computation (pure decision) ────────────────────────────────────────

def _compute_orders(allocations: dict, portfolio_value: float,
                    current_holdings: dict) -> list:
    """Compute required buy/sell orders to reach config allocations.
    Fee-aware: skips trades where drift < round-trip cost × FEE_COVERAGE.
    Crypto buys are exempt from individual fee filter — pooling handles them.
    """
    orders = []
    for symbol, target_alloc in allocations.items():
        if symbol == "CASH":
            continue
        target_value  = portfolio_value * target_alloc
        current_value = current_holdings[symbol]["value"]
        price         = current_holdings[symbol]["price"]

        if not price or price <= 0:
            continue

        diff     = target_value - current_value
        fee_key  = "crypto" if is_crypto(symbol) else "stock"
        fee_rate = ROUND_TRIP_FEE[fee_key]
        est_fee  = round(abs(diff) * fee_rate, 4)

        is_crypto_buy = is_crypto(symbol) and diff > 0
        if not is_crypto_buy:
            if abs(diff) < max(1.0, portfolio_value * fee_rate * FEE_COVERAGE):
                continue

        if diff > 0:
            drift_pct = round(abs(current_value / portfolio_value - target_alloc) * 100, 2)
            orders.append({
                "symbol":    symbol,
                "side":      "BUY",
                "notional":  round(diff, 2),
                "drift_pct": drift_pct,
                "est_fee":   est_fee,
                "reason":    f"Under ${diff:.2f} (target ${target_value:.2f}, have ${current_value:.2f})",
            })
        else:
            qty_to_sell = abs(diff) / price
            if current_holdings[symbol]["qty"] > 0 and qty_to_sell > 0.0001:
                qty_to_sell = min(qty_to_sell, current_holdings[symbol]["qty"])
                orders.append({
                    "symbol":   symbol,
                    "side":     "SELL",
                    "qty":      round(qty_to_sell, 6),
                    "notional": round(abs(diff), 2),
                    "est_fee":  est_fee,
                    "reason":   f"Over ${abs(diff):.2f} (target ${target_value:.2f}, have ${current_value:.2f})",
                })
    return orders


# ── Order execution batch ─────────────────────────────────────────────────────

def _execute_batch(orders: list, broker: AlpacaBroker,
                   dry_run: bool, initial_cash: float) -> tuple:
    """Execute orders: sells → non-crypto buys → crypto buys (with pooling).
    Returns (executed_list, cash_remaining).
    """
    executed       = []
    cash_available = initial_cash

    sells           = [o for o in orders if o["side"] == "SELL"]
    non_crypto_buys = [o for o in orders if o["side"] == "BUY" and not is_crypto(o["symbol"])]
    crypto_buys     = sorted(
        [o for o in orders if o["side"] == "BUY" and is_crypto(o["symbol"])],
        key=lambda o: o.get("drift_pct", 0), reverse=True,
    )

    # Phase 1: sells
    for order in sells:
        if dry_run:
            executed.append({**order, "status": "DRY_RUN"})
            continue
        try:
            result = broker.sell(alpaca_symbol(order["symbol"]), qty=order["qty"])
            executed.append({**order, "status": result["status"], "order_id": result["id"]})
        except Exception as e:
            executed.append({**order, "status": f"ERROR: {e}"})

    # Re-fetch cash after sells (broker is source of truth)
    if not dry_run:
        cash_available = float(broker.get_account()["cash"])

    # Phase 2: non-crypto buys
    for order in non_crypto_buys:
        if dry_run:
            executed.append({**order, "status": "DRY_RUN"})
            continue
        try:
            notional = min(order["notional"], cash_available)
            if notional < 1.0:
                executed.append({**order, "status": "SKIPPED (insufficient cash)"})
                continue
            result = broker.buy(alpaca_symbol(order["symbol"]), notional=notional)
            cash_available -= notional
            executed.append({**order, "status": result["status"], "order_id": result["id"]})
        except Exception as e:
            executed.append({**order, "status": f"ERROR: {e}"})

    # Phase 3: crypto buys with pooling
    def _place_pooled_crypto(pool_orders):
        nonlocal cash_available
        if not pool_orders:
            return
        top      = pool_orders[0]
        raw      = round(sum(o["notional"] for o in pool_orders), 2)
        notional = round(min(max(raw, MIN_CRYPTO_NOTIONAL), cash_available), 2)
        if dry_run:
            for o in pool_orders:
                executed.append({**o, "status": f"DRY_RUN (pooled ${notional:.2f} → {top['symbol']})"})
            return
        try:
            if notional < MIN_CRYPTO_NOTIONAL:
                for o in pool_orders:
                    executed.append({**o, "status": f"DEFERRED (cash ${cash_available:.2f} < min ${MIN_CRYPTO_NOTIONAL})"})
                return
            result = broker.buy(alpaca_symbol(top["symbol"]), notional=notional)
            cash_available -= notional
            for o in pool_orders:
                executed.append({**o, "status": f"POOLED→{top['symbol']} ({result['status']})",
                                 "order_id": result["id"]})
        except Exception as e:
            for o in pool_orders:
                executed.append({**o, "status": f"ERROR (pooled): {e}"})

    total_crypto = round(sum(o["notional"] for o in crypto_buys), 2)
    if crypto_buys and total_crypto < POOL_THRESHOLD:
        _place_pooled_crypto(crypto_buys)
    else:
        large = [o for o in crypto_buys if o["notional"] >= MIN_CRYPTO_NOTIONAL]
        small = [o for o in crypto_buys if o["notional"] <  MIN_CRYPTO_NOTIONAL]
        for order in large:
            if dry_run:
                executed.append({**order, "status": "DRY_RUN"})
                continue
            try:
                notional = min(order["notional"], cash_available)
                if notional < MIN_CRYPTO_NOTIONAL:
                    executed.append({**order, "status": f"DEFERRED (cash ${cash_available:.2f} < min ${MIN_CRYPTO_NOTIONAL})"})
                    continue
                result = broker.buy(alpaca_symbol(order["symbol"]), notional=notional)
                cash_available -= notional
                executed.append({**order, "status": result["status"], "order_id": result["id"]})
            except Exception as e:
                executed.append({**order, "status": f"ERROR: {e}"})
        if small:
            _place_pooled_crypto(small)

    return executed, cash_available


# ── Phase: IDLE ───────────────────────────────────────────────────────────────

def _phase_idle(state: dict, allocations: dict, capital_to_deploy: float,
                monthly_dca: float, broker: AlpacaBroker, dry_run: bool,
                force: bool, bull_days: int, bear_days: int,
                drift_cfg: dict) -> dict:
    """Check triggers, decide, and route: execute now or defer to market open."""
    now   = datetime.now()
    macro = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))
    rebalance_interval = bear_days if macro["regime"] == "BEAR" else bull_days

    # ── Time-based skip + drift check ─────────────────────────────────────────
    last_rebalance = state.get("last_rebalance_date")
    if last_rebalance and not force:
        last_dt = datetime.fromisoformat(last_rebalance)
        trading_days_since = (now - last_dt).days * 5 / 7
        if trading_days_since < rebalance_interval:
            # Lightweight drift check using position market values (no price fetches)
            positions     = broker.get_positions()
            pos_map       = {p["symbol"]: p for p in positions}
            invested      = sum(float(p["market_value"]) for p in positions)
            port_val      = invested if invested >= 10 else capital_to_deploy
            thresholds    = _get_thresholds(state, allocations, drift_cfg)
            actual_weights = state.get("actual_weights")
            regime_factor  = (drift_cfg.get("bull_factor", 1.5)
                              if macro["regime"] == "BULL"
                              else drift_cfg.get("bear_factor", 0.7))

            violators = []
            for symbol in [s for s in allocations if s != "CASH"]:
                pos_sym    = position_symbol(symbol)
                mkt_val    = float(pos_map[pos_sym]["market_value"]) if pos_sym in pos_map else 0.0
                current_w  = (mkt_val / port_val * 100) if port_val > 0 else 0
                baseline_w = (actual_weights.get(symbol, allocations[symbol])
                              if actual_weights else allocations[symbol]) * 100
                drift      = abs(current_w - baseline_w)
                threshold  = thresholds.get(symbol, 5.0) * regime_factor
                if drift > threshold:
                    violators.append({
                        "symbol": symbol, "drift": round(drift, 2),
                        "threshold": round(threshold, 2),
                        "current_pct": round(current_w, 2),
                        "baseline_pct": round(baseline_w, 2),
                    })

            if not violators:
                next_in = int(rebalance_interval - trading_days_since)
                return {
                    "action": "SKIP", "phase": PHASE_IDLE, "macro": macro,
                    "reason": (f"Last rebalance {last_dt.strftime('%Y-%m-%d')}. "
                               f"Next in ~{next_in} trading days ({macro['regime']} mode)."),
                }
            force = True  # drift triggered

    # ── Stale order cancellation ───────────────────────────────────────────────
    open_orders     = broker.get_orders("open")
    stale_cancelled = []
    if not dry_run and open_orders:
        for o in open_orders:
            created_str = o["created_at"].split("+")[0].split(".")[0]
            created     = datetime.fromisoformat(created_str)
            if (now - created).days >= STALE_ORDER_DAYS:
                try:
                    broker.cancel_order(o["id"])
                    stale_cancelled.append(o)
                except Exception:
                    pass
        if stale_cancelled:
            open_orders = broker.get_orders("open")

    if open_orders and not force:
        return {
            "action": "SKIP", "phase": PHASE_IDLE, "macro": macro,
            "reason": f"{len(open_orders)} open order(s) pending fills.",
            "stale_cancelled": stale_cancelled,
        }

    # ── Full portfolio snapshot ────────────────────────────────────────────────
    snap             = _snapshot_portfolio(broker, allocations, capital_to_deploy)
    portfolio_value  = snap["portfolio_value"]
    current_holdings = snap["current_holdings"]
    is_initial       = snap["is_initial"]
    acct             = snap["acct"]

    month_key   = f"{now.year}-{now.month:02d}"
    dca_applied = False
    if month_key != state.get("last_dca_month", "") and not is_initial:
        portfolio_value += monthly_dca
        dca_applied = True

    # ── DECIDE ────────────────────────────────────────────────────────────────
    orders  = _compute_orders(allocations, portfolio_value, current_holdings)
    action  = "INITIAL_BUY" if is_initial else "REBALANCE"
    trigger = "DRIFT" if (force and last_rebalance) else "TIME"

    # ── RECORD (before any execution) ─────────────────────────────────────────
    if not dry_run:
        state["last_rebalance_date"] = now.isoformat()
        state["last_regime"]         = macro["regime"]
        state["first_run_date"]      = state.get("first_run_date", now.isoformat())
        state.setdefault("skip_count", 0)
        state.setdefault("last_skip_notification", None)
        if dca_applied:
            state["last_dca_month"] = month_key
        # Clear previous actual_weights — new rebalance targets config allocations
        state.pop("actual_weights", None)

    if not orders:
        if not dry_run:
            save_state(state)
        return {
            "action": action, "phase": PHASE_IDLE, "macro": macro,
            "portfolio_value": round(portfolio_value, 2),
            "dca_applied": dca_applied, "orders": [], "errors": [],
            "stale_cancelled": stale_cancelled, "dry_run": dry_run,
            "trigger": trigger,
        }

    # ── EXECUTE: route by market hours ─────────────────────────────────────────
    clock = broker.get_clock()

    if clock["is_open"] or dry_run:
        # Market open — execute everything now
        executed, _ = _execute_batch(orders, broker, dry_run, float(acct["cash"]))
        submitted   = [o for o in executed if o.get("order_id")]
        if not dry_run:
            state["phase"]            = PHASE_EXECUTING
            state["submitted_orders"] = submitted
            state["pending_decision"] = {
                "timestamp": now.isoformat(), "action": action,
                "trigger": trigger, "macro": macro,
                "portfolio_value": portfolio_value, "dca_applied": dca_applied,
            }
            save_state(state)
        errors = [o for o in executed if str(o.get("status", "")).startswith("ERROR")]
        return {
            "action": action, "phase": PHASE_EXECUTING, "macro": macro,
            "portfolio_value": round(portfolio_value, 2),
            "dca_applied": dca_applied, "orders": executed, "errors": errors,
            "stale_cancelled": stale_cancelled, "dry_run": dry_run,
            "trigger": trigger, "clock": clock,
        }
    else:
        # Market closed — sell crypto now, defer everything else
        crypto_sells = [o for o in orders if o["side"] == "SELL" and is_crypto(o["symbol"])]
        deferred     = [o for o in orders if o not in crypto_sells]

        executed_now = []
        if crypto_sells:
            executed_now, _ = _execute_batch(crypto_sells, broker, dry_run, float(acct["cash"]))

        if not dry_run:
            state["phase"] = PHASE_PENDING_OPEN
            state["pending_decision"] = {
                "timestamp": now.isoformat(), "action": action,
                "trigger": trigger, "macro": macro,
                "portfolio_value": portfolio_value, "dca_applied": dca_applied,
                "deferred_orders":    deferred,
                "executed_so_far":    executed_now,
            }
            save_state(state)

        return {
            "action": "DECISION_DEFERRED", "phase": PHASE_PENDING_OPEN, "macro": macro,
            "portfolio_value": round(portfolio_value, 2),
            "dca_applied": dca_applied, "dry_run": dry_run,
            "executed_now": executed_now, "deferred_until_open": deferred,
            "next_open": clock["next_open"], "trigger": trigger, "clock": clock,
        }


# ── Phase: PENDING_MARKET_OPEN ────────────────────────────────────────────────

def _phase_pending_open(state: dict, allocations: dict, capital_to_deploy: float,
                        monthly_dca: float, broker: AlpacaBroker, dry_run: bool) -> dict:
    """Market was closed at decision time. Wait for open, then refresh + execute."""
    clock   = broker.get_clock()
    macro   = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))
    pending = state.get("pending_decision", {})

    if not clock["is_open"] and not dry_run:
        return {
            "action": "WAITING", "phase": PHASE_PENDING_OPEN, "macro": macro,
            "reason": f"Market closed. Next open: {clock['next_open'][:16]}",
            "clock": clock,
        }

    # Market open — fresh snapshot, refresh order sizes (prices moved since decision)
    snap             = _snapshot_portfolio(broker, allocations, capital_to_deploy)
    portfolio_value  = snap["portfolio_value"]
    current_holdings = snap["current_holdings"]
    acct             = snap["acct"]

    if pending.get("dca_applied"):
        portfolio_value += monthly_dca

    # Recompute orders with current prices (crypto sells already reflected in positions)
    orders  = _compute_orders(allocations, portfolio_value, current_holdings)
    action  = pending.get("action", "REBALANCE")
    trigger = pending.get("trigger", "TIME") + "+REFRESHED"

    executed, _ = _execute_batch(orders, broker, dry_run, float(acct["cash"]))
    submitted   = [o for o in executed if o.get("order_id")]

    if not dry_run:
        state["phase"]                              = PHASE_EXECUTING
        state["submitted_orders"]                   = submitted
        state["pending_decision"]["refreshed_at"]  = datetime.now().isoformat()
        save_state(state)

    errors = [o for o in executed if str(o.get("status", "")).startswith("ERROR")]
    return {
        "action": action, "phase": PHASE_EXECUTING, "macro": macro,
        "portfolio_value": round(portfolio_value, 2),
        "dca_applied": pending.get("dca_applied", False),
        "orders": executed, "errors": errors, "dry_run": dry_run,
        "trigger": trigger, "clock": clock,
        "note": "Decision refreshed at market open with updated prices.",
    }


# ── Phase: EXECUTING ──────────────────────────────────────────────────────────

def _phase_executing(state: dict, allocations: dict,
                     broker: AlpacaBroker, dry_run: bool) -> dict:
    """Check fill status. When all settled, snapshot actual weights → IDLE."""
    submitted = state.get("submitted_orders", [])
    macro     = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))

    if not submitted:
        if not dry_run:
            state["phase"] = PHASE_IDLE
            save_state(state)
        return {"action": "CONFIRMED", "orders": [], "phase": PHASE_IDLE, "macro": macro}

    SETTLED       = {"filled", "cancelled", "expired", "replaced", "done_for_day"}
    order_results = []
    all_settled   = True

    for sub in submitted:
        try:
            o      = broker.get_order_by_id(sub["order_id"])
            status = o["status"]
            order_results.append({**sub, **o})
            if status not in SETTLED:
                all_settled = False
        except Exception as e:
            order_results.append({**sub, "status": "unknown", "error": str(e)})
            all_settled = False

    if not all_settled:
        return {
            "action": "AWAITING_FILLS", "phase": PHASE_EXECUTING,
            "orders": order_results, "macro": macro,
        }

    # All settled — snapshot actual achieved weights
    positions   = broker.get_positions()
    total_value = sum(float(p["market_value"]) for p in positions)
    actual_weights = {}
    for p in positions:
        for config_sym in allocations:
            if config_sym != "CASH" and position_symbol(config_sym) == p["symbol"]:
                actual_weights[config_sym] = (
                    float(p["market_value"]) / total_value if total_value > 0 else 0
                )
                break

    if not dry_run:
        state["actual_weights"]         = actual_weights
        state["phase"]                  = PHASE_IDLE
        state["submitted_orders"]       = []
        state["last_confirmed_date"]    = datetime.now().isoformat()
        state["last_rebalance_summary"] = {
            "date":             state.get("last_rebalance_date"),
            "trigger":          state.get("pending_decision", {}).get("trigger", "?"),
            "orders_submitted": len(submitted),
            "orders_filled":    sum(1 for o in order_results if o.get("status") == "filled"),
            "actual_weights":   actual_weights,
        }
        state["pending_decision"] = None
        save_state(state)

    return {
        "action": "CONFIRMED", "phase": PHASE_IDLE,
        "orders": order_results, "actual_weights": actual_weights, "macro": macro,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def advance_rebalance_cycle(allocations: dict, capital_to_deploy: float,
                            monthly_dca: float, broker: AlpacaBroker,
                            dry_run: bool = True, force: bool = False,
                            bull_days: int = 126, bear_days: int = 21,
                            drift_cfg: dict = None) -> dict:
    """Drive the rebalance state machine one step forward. Call every cron run."""
    state     = load_state()
    phase     = state.get("phase", PHASE_IDLE)
    drift_cfg = drift_cfg or {}

    if phase == PHASE_IDLE:
        return _phase_idle(state, allocations, capital_to_deploy, monthly_dca,
                           broker, dry_run, force, bull_days, bear_days, drift_cfg)
    elif phase == PHASE_PENDING_OPEN:
        return _phase_pending_open(state, allocations, capital_to_deploy,
                                   monthly_dca, broker, dry_run)
    elif phase == PHASE_EXECUTING:
        return _phase_executing(state, allocations, broker, dry_run)
    else:
        state["phase"] = PHASE_IDLE
        save_state(state)
        return {"action": "PHASE_RESET", "phase": PHASE_IDLE,
                "note": f"Unknown phase '{phase}' — reset to IDLE."}


# Backwards compat for run.py
run_live_rebalance = advance_rebalance_cycle


# ── Daily digest ──────────────────────────────────────────────────────────────

def build_daily_digest(state: dict, broker: AlpacaBroker,
                       allocations: dict, config: dict) -> str:
    """Build the full daily Telegram summary message."""
    now   = datetime.now()
    phase = state.get("phase", PHASE_IDLE)
    macro = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))

    try:
        clock   = broker.get_clock()
        rb_cfg  = config.get("rebalance", {})
        snap    = _snapshot_portfolio(broker, allocations, rb_cfg.get("initial_capital", 1000))
        portfolio_value  = snap["portfolio_value"]
        current_holdings = snap["current_holdings"]
        acct             = snap["acct"]
    except Exception as e:
        return (f"*Trading Bot Daily* — {now.strftime('%Y-%m-%d')}\n"
                f"❌ Snapshot failed: {e}")

    # Uptime
    first_run  = state.get("first_run_date")
    uptime_str = (f"{(now - datetime.fromisoformat(first_run)).days}d "
                  f"(since {datetime.fromisoformat(first_run).strftime('%b %d')})"
                  if first_run else "unknown")

    # Positions table
    actual_weights = state.get("actual_weights", {})
    pos_lines = ""
    for symbol in [s for s in allocations if s != "CASH"]:
        h           = current_holdings.get(symbol, {"value": 0})
        current_pct = h["value"] / portfolio_value * 100 if portfolio_value > 0 else 0
        target_pct  = allocations[symbol] * 100
        baseline_w  = actual_weights.get(symbol, allocations[symbol])
        drift       = abs(current_pct - baseline_w * 100)
        icon        = "✅" if drift < 2 else "⚠️" if drift < 5 else "🔴"
        pos_lines  += (f"\n  {icon} {symbol:<10} {current_pct:.1f}% "
                       f"(target {target_pct:.0f}%, drift {drift:+.1f}pp)")

    # Phase / schedule description
    rb_cfg      = config.get("rebalance", {})
    bear_days   = rb_cfg.get("bear_days", 21)
    bull_days   = rb_cfg.get("bull_days", 126)
    interval    = bear_days if macro["regime"] == "BEAR" else bull_days
    last_reb    = state.get("last_rebalance_date")
    phase_lines = ""

    if last_reb:
        days_ago    = (now - datetime.fromisoformat(last_reb)).days
        td_since    = (now - datetime.fromisoformat(last_reb)).days * 5 / 7
        next_in     = max(0, int(interval - td_since))
        phase_lines += (f"\n  Last rebalance: {datetime.fromisoformat(last_reb).strftime('%Y-%m-%d')}"
                        f" ({days_ago}d ago)")
        phase_lines += f"\n  Next in: ~{next_in} trading days ({macro['regime']} mode)"

    if phase == PHASE_PENDING_OPEN:
        pending     = state.get("pending_decision", {})
        dec_time    = pending.get("timestamp", "?")[:16]
        deferred    = pending.get("deferred_orders", [])
        phase_lines += f"\n  ⏳ Decision at {dec_time} — waiting for market open"
        phase_lines += f"\n  Opens: {clock['next_open'][:16]} | Queued: {len(deferred)} order(s)"
        if pending.get("executed_so_far"):
            phase_lines += f"\n  Crypto sells already executed: {len(pending['executed_so_far'])}"
    elif phase == PHASE_EXECUTING:
        submitted   = state.get("submitted_orders", [])
        phase_lines += f"\n  ⏳ Awaiting fill confirmation on {len(submitted)} order(s)"

    # Last rebalance summary
    summary_lines = ""
    summary = state.get("last_rebalance_summary")
    if summary and summary.get("date"):
        filled  = summary.get("orders_filled", 0)
        total   = summary.get("orders_submitted", 0)
        trigger = summary.get("trigger", "?")
        date_str = datetime.fromisoformat(summary["date"]).strftime("%b %d")
        summary_lines = (f"\n\n*Last rebalance* ({date_str}) — {trigger}\n"
                         f"  {filled}/{total} orders filled")
        aw = summary.get("actual_weights", {})
        if aw:
            for sym, w in aw.items():
                t = allocations.get(sym, 0)
                summary_lines += f"\n  {sym}: {w*100:.1f}% (target {t*100:.0f}%)"

    regime_icon    = "📈" if macro["regime"] == "BULL" else "📉"
    market_status  = "🟢 OPEN" if clock["is_open"] else "🔴 CLOSED"
    fallback_note  = " ⚠️ _regime estimate_" if macro.get("fallback") else ""

    return (
        f"*Trading Bot Daily* — {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"⏱ Uptime: {uptime_str}\n\n"
        f"{regime_icon} *Market:* {macro['regime']}{fallback_note} | {market_status}\n"
        f"SPY ${macro['spy_price']} / SMA200 ${macro['sma_200']} ({macro['gap_pct']:+.1f}%)\n\n"
        f"💼 *Portfolio:* ${portfolio_value:,.2f} | Cash: ${float(acct['cash']):,.2f}\n"
        f"*Positions:*{pos_lines}\n\n"
        f"*Status:* {phase}{phase_lines}"
        f"{summary_lines}"
    )


def should_send_digest(state: dict) -> bool:
    """True if digest hasn't been sent today and it's past 05:00 UTC (09:00 Dubai)."""
    from datetime import timezone
    now_utc = datetime.now(timezone.utc)
    today   = now_utc.date().isoformat()
    return state.get("last_digest_date") != today and now_utc.hour >= 5


# ── CLI pretty-print (backwards compat) ──────────────────────────────────────

def print_rebalance_result(result: dict):
    print(f"\n{'='*60}")
    action = result.get("action", "?")
    phase  = result.get("phase", "?")

    if action == "SKIP":
        print(f"  SKIPPED — {result.get('reason', '')}")
    elif action == "WAITING":
        print(f"  WAITING — {result.get('reason', '')}")
    elif action == "DECISION_DEFERRED":
        print(f"  DEFERRED (market closed)")
        print(f"  Crypto sells executed: {len(result.get('executed_now', []))}")
        print(f"  Orders queued for open: {len(result.get('deferred_until_open', []))}")
        print(f"  Next open: {result.get('next_open', '?')[:16]}")
    elif action in ("REBALANCE", "INITIAL_BUY", "CONFIRMED", "AWAITING_FILLS"):
        mode = "DRY RUN" if result.get("dry_run") else "LIVE"
        print(f"  {action} — {mode} | Phase: {phase}")
        if result.get("note"):
            print(f"  Note: {result['note']}")
        macro = result.get("macro", {})
        print(f"  Market: {macro.get('regime')} (SPY ${macro.get('spy_price')} "
              f"vs SMA200 ${macro.get('sma_200')}, {macro.get('gap_pct', 0):+.1f}%)")
        pv = result.get("portfolio_value")
        if pv:
            print(f"  Portfolio: ${pv:,.2f}")
        orders = result.get("orders", [])
        if orders:
            print(f"\n  {'Symbol':<12} {'Side':<6} {'Amount':>10} {'Status'}")
            print(f"  {'-'*55}")
            for o in orders:
                amt = f"${o.get('notional', 0):.2f}"
                print(f"  {o['symbol']:<12} {o['side']:<6} {amt:>10}  {o.get('status','?')}")
        else:
            print("  Portfolio balanced — no trades needed.")
    else:
        print(f"  {action} | {result}")

    print(f"{'='*60}\n")
