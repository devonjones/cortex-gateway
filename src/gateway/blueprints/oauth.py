"""OAuth token management endpoints for Gmail API authentication.

Provides web-based OAuth flow for token refresh/renewal without requiring
SSH tunneling or headless OAuth flows.
"""

import json
from pathlib import Path

import structlog
from flask import Blueprint, redirect, request
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

logger = structlog.get_logger(__name__)

oauth_bp = Blueprint("oauth", __name__, url_prefix="/oauth")

# OAuth configuration
TOKEN_PATH = Path("/home/devon/cortex-secrets/gmail-token.json")
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://mail.google.com/",
]


def _load_client_config() -> dict[str, dict[str, str | list[str]]]:
    """Load OAuth client configuration from existing token file."""
    if not TOKEN_PATH.exists():
        raise FileNotFoundError(f"Token file not found: {TOKEN_PATH}")

    with open(TOKEN_PATH) as f:
        data = json.load(f)

    return {
        "web": {
            "client_id": data["client_id"],
            "client_secret": data["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [f"http://{request.host}/oauth/callback"],
        }
    }


def _save_token(creds: Credentials) -> None:
    """Save refreshed credentials to token file."""
    # Load existing data to preserve client credentials
    with open(TOKEN_PATH) as f:
        data = json.load(f)

    # Update with new token
    data["token"] = creds.token
    data["refresh_token"] = creds.refresh_token
    if creds.expiry:
        data["expiry"] = creds.expiry.isoformat()

    with open(TOKEN_PATH, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Token saved", path=str(TOKEN_PATH))


@oauth_bp.route("/status")
def status():
    """Show current token status."""
    try:
        if not TOKEN_PATH.exists():
            return {
                "status": "no_token",
                "message": f"Token file not found: {TOKEN_PATH}",
            }, 404

        with open(TOKEN_PATH) as f:
            data = json.load(f)

        has_refresh = bool(data.get("refresh_token"))
        expiry = data.get("expiry", "unknown")

        return {
            "status": "ok",
            "token_path": str(TOKEN_PATH),
            "has_refresh_token": has_refresh,
            "expiry": expiry,
            "scopes": data.get("scopes", []),
            "actions": {
                "refresh": "/oauth/refresh",
                "new": "/oauth/start",
            },
        }
    except Exception as e:
        logger.error("Failed to read token status", error=str(e))
        return {"status": "error", "error": str(e)}, 500


@oauth_bp.route("/refresh", methods=["POST"])
def refresh():
    """Attempt to refresh the existing token (non-interactive)."""
    try:
        if not TOKEN_PATH.exists():
            return {
                "status": "error",
                "message": "No token file found. Use /oauth/start to create one.",
            }, 404

        with open(TOKEN_PATH) as f:
            data = json.load(f)

        if not data.get("refresh_token"):
            return {
                "status": "error",
                "message": "No refresh token available. Use /oauth/start for new token.",
            }, 400

        # Create credentials and refresh
        creds = Credentials(
            token=data.get("token"),
            refresh_token=data["refresh_token"],
            token_uri=data["token_uri"],
            client_id=data["client_id"],
            client_secret=data["client_secret"],
            scopes=data.get("scopes", SCOPES),
        )

        creds.refresh(Request())
        _save_token(creds)

        logger.info("Token refreshed successfully")
        return {
            "status": "success",
            "message": "Token refreshed successfully",
            "expiry": creds.expiry.isoformat() if creds.expiry else None,
        }

    except Exception as e:
        logger.error("Token refresh failed", error=str(e))
        return {
            "status": "error",
            "error": str(e),
            "suggestion": "Try /oauth/start for new token",
        }, 400


@oauth_bp.route("/start")
def start():
    """Start OAuth flow - redirects to Google consent screen."""
    try:
        client_config = _load_client_config()

        # Create flow with dynamic redirect URI
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=f"http://{request.host}/oauth/callback",
        )

        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",  # Force consent to get refresh token
        )

        # Store state in session for CSRF protection
        # In production, use Flask session with secret key
        # For now, we'll validate the callback URL matches our host
        logger.info("Starting OAuth flow", redirect_uri=flow.redirect_uri)

        return redirect(authorization_url)

    except Exception as e:
        logger.error("Failed to start OAuth flow", error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }, 500


@oauth_bp.route("/callback")
def callback():
    """Handle OAuth callback from Google."""
    try:
        client_config = _load_client_config()

        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=f"http://{request.host}/oauth/callback",
        )

        # Exchange authorization code for token
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        _save_token(creds)

        logger.info("OAuth flow completed successfully")

        return f"""
        <html>
        <head><title>OAuth Success</title></head>
        <body style="font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px;">
            <h1 style="color: green;">✅ Authorization Successful!</h1>
            <p>Gmail token has been refreshed and saved.</p>
            <p><strong>Token expires:</strong> {
                creds.expiry.isoformat() if creds.expiry else 'Unknown'
            }</p>
            <p>You can close this window and restart the gmail-sync service.</p>
            <hr>
            <p><a href="/oauth/status">Check token status</a></p>
        </body>
        </html>
        """

    except Exception as e:
        logger.error("OAuth callback failed", error=str(e))
        return (
            f"""
        <html>
        <head><title>OAuth Error</title></head>
        <body style="font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px;">
            <h1 style="color: red;">❌ Authorization Failed</h1>
            <p><strong>Error:</strong> {str(e)}</p>
            <p><a href="/oauth/start">Try again</a></p>
        </body>
        </html>
        """,
            500,
        )
