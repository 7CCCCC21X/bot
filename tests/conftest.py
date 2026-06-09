"""Test bootstrap.

Set the env vars the module reads at import time (it only *requires* a token
in main(), but we set them so nothing trips) and make the repo root importable.
"""
import os
import sys
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("PREDICT_API_KEY", "test-key")
os.environ.setdefault("HEALTH_PORT", "0")  # don't bind a port during tests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
