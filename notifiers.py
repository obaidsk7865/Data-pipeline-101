# notifiers.py
"""
Simple, robust Slack notifier.
It will silently return if SLACK_WEBHOOK_URL is not configured.
Designed to never raise during ETL (fail-safe).
"""

import os
import requests
import traceback

def slack_notify(text: str, blocks: dict | None = None, username: str = "ETL Bot", icon_emoji: str = ":gear:"):
    """
    Send a message to Slack via Incoming Webhook.
    - text: plain text fallback shown in clients
    - blocks: optional Slack Blocks JSON (dict or list); if provided it will be posted as 'blocks'
    """
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return False

    payload = {"text": text, "username": username, "icon_emoji": icon_emoji}
    if blocks is not None:
        payload["blocks"] = blocks

    try:
        # small timeout so notifier doesn't hang ETL
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        return True
    except Exception as e:
        # Avoid raising in ETL; log to stderr if available
        try:
            print("slack_notify error:", e)
        except Exception:
            pass
        return False
