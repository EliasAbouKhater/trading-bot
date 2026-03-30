"""Flask routes for the trading bot dashboard."""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Blueprint, render_template, request, jsonify
from dotenv import load_dotenv
import yaml

from core.data import fetch_data, fetch_pair
from core.engine import BacktestEngine
from core.risk import RiskManager
from core.broker import AlpacaBroker
from strategies import REGISTRY

bp = Blueprint("dashboard", __name__, template_folder="templates", static_folder="static")


def get_config():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(project_root, "config.yaml")) as f:
        return yaml.safe_load(f)


def run_single_backtest(name, config):
    strat_config = config["strategies"].get(name, {})
    strategy = REGISTRY[name](strat_config)
    risk = RiskManager(config["risk"])
    data_cfg = config["data"]
    period = data_cfg.get("default_period", "6mo")
    interval = data_cfg.get("interval", "1h")
    cache_dir = data_cfg.get("cache_dir", "data/")
    cache_max_hours = data_cfg.get("cache_max_hours", 1)

    if name == "pairs":
        df = fetch_pair(strat_config["symbol_a"], strat_config["symbol_b"],
                        period, interval, cache_dir, cache_max_hours)
    else:
        df = fetch_data(strat_config.get("symbol", "XRP/USD"), period, interval,
                        cache_dir, cache_max_hours)

    budget = 250.0
    for asset in config.get("assets", []):
        if asset.get("symbol") == strat_config.get("symbol", "XRP/USD"):
            budget = asset.get("budget", asset.get("budget_monthly", 250.0))
            break

    engine = BacktestEngine(strategy, risk, initial_capital=budget)
    return engine.run(df)


@bp.route("/")
def index():
    config = get_config()
    return render_template("index.html", strategies=list(REGISTRY.keys()),
                           active=config["strategies"]["active"])


@bp.route("/api/backtest", methods=["POST"])
def api_backtest():
    config = get_config()
    name = request.json.get("strategy", "ma_crossover")
    if name not in REGISTRY:
        return jsonify({"error": f"Unknown strategy: {name}"}), 400
    results = run_single_backtest(name, config)
    # Convert equity curve for charting
    eq = results["equity_curve"]
    equity_data = []
    if not eq.empty:
        equity_data = [{"date": str(r["date"])[:10], "equity": round(r["equity"], 2)}
                       for _, r in eq.iterrows()]
    return jsonify({
        "strategy": results["strategy"],
        "initial_capital": results["initial_capital"],
        "final_equity": results["final_equity"],
        "total_return_pct": results["total_return_pct"],
        "total_trades": results["total_trades"],
        "winning_trades": results["winning_trades"],
        "losing_trades": results["losing_trades"],
        "win_rate_pct": results["win_rate_pct"],
        "max_drawdown_pct": results["max_drawdown_pct"],
        "sharpe_ratio": results["sharpe_ratio"],
        "equity_curve": equity_data,
        "trades": [{"date": str(t["date"])[:10], "action": t["action"],
                     "price": round(t["price"], 2), "shares": round(t["shares"], 4),
                     "pnl": round(t["pnl"], 2)} for t in results["trades"]],
    })


@bp.route("/api/compare", methods=["POST"])
def api_compare():
    config = get_config()
    all_results = []
    for name in REGISTRY:
        r = run_single_backtest(name, config)
        eq = r["equity_curve"]
        equity_data = []
        if not eq.empty:
            equity_data = [{"date": str(row["date"])[:10], "equity": round(row["equity"], 2)}
                           for _, row in eq.iterrows()]
        all_results.append({
            "strategy": r["strategy"],
            "total_return_pct": r["total_return_pct"],
            "total_trades": r["total_trades"],
            "win_rate_pct": r["win_rate_pct"],
            "max_drawdown_pct": r["max_drawdown_pct"],
            "sharpe_ratio": r["sharpe_ratio"],
            "final_equity": r["final_equity"],
            "equity_curve": equity_data,
        })
    return jsonify(all_results)


@bp.route("/api/account")
def api_account():
    try:
        broker = AlpacaBroker(paper=True)
        acct = broker.get_account()
        positions = broker.get_positions()
        orders = broker.get_orders("open")
        return jsonify({"account": acct, "positions": positions, "orders": orders})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/paper-signal", methods=["POST"])
def api_paper_signal():
    """Check current signal for a strategy without executing."""
    config = get_config()
    name = request.json.get("strategy", config["strategies"]["active"])
    if name not in REGISTRY:
        return jsonify({"error": f"Unknown strategy: {name}"}), 400

    strat_config = config["strategies"].get(name, {})
    strategy = REGISTRY[name](strat_config)
    data_cfg = config["data"]
    symbol = strat_config.get("symbol", "XRP/USD")
    interval = data_cfg.get("interval", "1h")
    cache_dir = data_cfg.get("cache_dir", "data/")
    cache_max_hours = data_cfg.get("cache_max_hours", 1)

    if name == "pairs":
        df = fetch_pair(strat_config["symbol_a"], strat_config["symbol_b"],
                        period="6mo", interval=interval,
                        cache_dir=cache_dir, cache_max_hours=cache_max_hours)
    else:
        df = fetch_data(symbol, period="6mo", interval=interval,
                        cache_dir=cache_dir, cache_max_hours=cache_max_hours)

    signals = strategy.generate_signals(df)
    latest = signals.iloc[-1]
    sig = int(latest["signal"])

    return jsonify({
        "strategy": name,
        "symbol": symbol,
        "price": round(float(latest["Close"]), 2),
        "signal": "BUY" if sig == 1 else "SELL" if sig == -1 else "HOLD",
        "date": str(signals.index[-1])[:10],
    })
