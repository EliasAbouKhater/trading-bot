#!/usr/bin/env python3
"""Trading Bot Dashboard — Flask web UI."""

import os
import sys
import yaml
from dotenv import load_dotenv
from flask import Flask

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from dashboard.routes import bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


if __name__ == "__main__":
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    dash = config.get("dashboard", {})
    host = dash.get("host", "0.0.0.0")
    port = dash.get("port", 5050)

    app = create_app()
    print(f"Dashboard running at http://localhost:{port}")
    app.run(host=host, port=port, debug=True)
