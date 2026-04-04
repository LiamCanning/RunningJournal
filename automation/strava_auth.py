#!/usr/bin/env python3
"""
Strava OAuth2 authorization flow.

Usage:
    python3 strava_auth.py

Opens a browser to Strava's authorization page, captures the callback
on a local HTTP server, exchanges the code for tokens, and stores them.
"""

import json
import os
import sys
import stat
import time
import webbrowser
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add parent so we can import config
sys.path.insert(0, os.path.dirname(__file__))
import config

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package is required. Install with: pip3 install requests")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(config.LOG_DIR, "strava_auth.log")),
    ],
)
log = logging.getLogger(__name__)

# ── Token persistence ──

def save_token(token_data):
    """Save token JSON to disk with restricted permissions."""
    os.makedirs(os.path.dirname(config.STRAVA_TOKEN_FILE), exist_ok=True)
    with open(config.STRAVA_TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)
    os.chmod(config.STRAVA_TOKEN_FILE, stat.S_IRUSR | stat.S_IWUSR)  # 600
    log.info("Token saved to %s", config.STRAVA_TOKEN_FILE)


def load_token():
    """Load stored token, or return None if missing."""
    if not os.path.exists(config.STRAVA_TOKEN_FILE):
        return None
    with open(config.STRAVA_TOKEN_FILE, "r") as f:
        return json.load(f)


def refresh_token_if_needed(token_data=None):
    """
    Check token expiry and refresh if needed.
    Returns an up-to-date token dict, or None on failure.
    """
    if token_data is None:
        token_data = load_token()
    if token_data is None:
        log.error("No token file found. Run strava_auth.py to authorize.")
        return None

    expires_at = token_data.get("expires_at", 0)
    # Refresh 5 minutes before actual expiry
    if time.time() < (expires_at - 300):
        return token_data

    log.info("Token expired or expiring soon, refreshing...")
    try:
        resp = requests.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": config.STRAVA_CLIENT_ID,
                "client_secret": config.STRAVA_CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": token_data["refresh_token"],
            },
            timeout=30,
        )
        resp.raise_for_status()
        new_token = resp.json()
        # Preserve any extra fields from original token
        token_data.update({
            "access_token": new_token["access_token"],
            "refresh_token": new_token["refresh_token"],
            "expires_at": new_token["expires_at"],
            "expires_in": new_token["expires_in"],
            "token_type": new_token.get("token_type", "Bearer"),
        })
        save_token(token_data)
        log.info("Token refreshed successfully, new expiry: %s", token_data["expires_at"])
        return token_data
    except Exception as e:
        log.error("Failed to refresh token: %s", e)
        return None


# ── OAuth callback handler ──

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler that captures the OAuth callback code."""

    authorization_code = None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/callback":
            params = parse_qs(parsed.query)
            if "code" in params:
                OAuthCallbackHandler.authorization_code = params["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization successful!</h2>"
                    b"<p>You can close this window and return to the terminal.</p>"
                    b"</body></html>"
                )
                log.info("Received authorization code.")
            elif "error" in params:
                error = params["error"][0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    f"<html><body><h2>Authorization failed: {error}</h2></body></html>".encode()
                )
                log.error("Authorization error: %s", error)
            else:
                self.send_response(400)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass


def exchange_code_for_token(code):
    """Exchange authorization code for access + refresh tokens."""
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": config.STRAVA_CLIENT_ID,
            "client_secret": config.STRAVA_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def run_oauth_flow():
    """Run the full OAuth flow: open browser, capture callback, exchange code."""
    auth_url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={config.STRAVA_CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={config.STRAVA_REDIRECT_URI}"
        f"&scope=activity:read_all"
        f"&approval_prompt=auto"
    )

    log.info("Starting OAuth flow...")
    log.info("If browser does not open, visit: %s", auth_url)
    webbrowser.open(auth_url)

    server = HTTPServer(("localhost", 5000), OAuthCallbackHandler)
    log.info("Waiting for OAuth callback on http://localhost:5000/callback ...")

    while OAuthCallbackHandler.authorization_code is None:
        server.handle_request()

    server.server_close()
    code = OAuthCallbackHandler.authorization_code

    log.info("Exchanging authorization code for tokens...")
    token_data = exchange_code_for_token(code)
    save_token(token_data)

    log.info("Authorization complete. Athlete ID: %s", token_data.get("athlete", {}).get("id"))
    return token_data


if __name__ == "__main__":
    run_oauth_flow()
