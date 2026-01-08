"""OAuth token management endpoints for Gmail API authentication.

Provides web-based OAuth flow for token refresh/renewal without requiring
SSH tunneling or headless OAuth flows.
"""

import json
from pathlib import Path

import structlog
from flask import Blueprint, redirect, render_template_string, request, session, url_for
from google.auth import exceptions as google_auth_exceptions
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

from gateway.config import config

logger = structlog.get_logger(__name__)

oauth_bp = Blueprint("oauth", __name__, url_prefix="/oauth")

# OAuth configuration
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://mail.google.com/",
]


def _get_token_path() -> Path:
    """Get token path from configuration."""
    return Path(config.oauth_token_path)


def _load_client_config() -> dict[str, dict[str, str | list[str]]]:
    """Load OAuth client configuration from existing token file."""
    token_path = _get_token_path()
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")

    with open(token_path) as f:
        data = json.load(f)

    return {
        "web": {
            "client_id": data["client_id"],
            "client_secret": data["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }


def _render_oauth_page(
    *, title: str, heading: str, message: str, status_code: int, expiry: str | None = None
) -> tuple[str, int]:
    """Render HTML page for OAuth flow results."""
    is_success = 200 <= status_code < 300
    color = "green" if is_success else "red"
    emoji = "✅" if is_success else "❌"

    # Build additional content based on success/error
    extra_content = ""
    if is_success and expiry:
        extra_content = f"<p><strong>Token expires:</strong> {expiry}</p>"
        extra_content += "<p>You can close this window and restart the gmail-sync service.</p>"
        extra_content += '<hr><p><a href="/oauth/status">Check token status</a></p>'
    else:
        extra_content = '<p><a href="/oauth/start">Try again</a></p>'

    return (
        render_template_string(
            """
        <html>
        <head><title>{{ title }}</title></head>
        <body style="font-family: sans-serif; max-width: 600px; margin: 50px auto; padding: 20px;">
            <h1 style="color: {{ color }};">{{ emoji }} {{ heading }}</h1>
            <p>{{ message }}</p>
            {{ extra_content | safe }}
        </body>
        </html>
        """,
            title=title,
            color=color,
            emoji=emoji,
            heading=heading,
            message=message,
            extra_content=extra_content,
        ),
        status_code,
    )


def _save_token(creds: Credentials) -> None:
    """Save refreshed credentials to token file atomically."""
    token_path = _get_token_path()
    # Load existing data to preserve client credentials
    with open(token_path) as f:
        data = json.load(f)

    # Update with new token
    data["token"] = creds.token
    data["refresh_token"] = creds.refresh_token
    if creds.expiry:
        data["expiry"] = creds.expiry.isoformat()

    # Write atomically via temp file + rename
    temp_path = token_path.with_suffix(f"{token_path.suffix}.tmp")
    with open(temp_path, "w") as f:
        json.dump(data, f, indent=2)
    temp_path.rename(token_path)

    logger.info("Token saved", path=str(token_path))


@oauth_bp.route("/status")
def status():
    """Show current token status."""
    try:
        token_path = _get_token_path()
        if not token_path.exists():
            return {
                "status": "no_token",
                "message": f"Token file not found: {token_path}",
            }, 404

        with open(token_path) as f:
            data = json.load(f)

        has_refresh = bool(data.get("refresh_token"))
        expiry = data.get("expiry", "unknown")

        return {
            "status": "ok",
            "token_path": str(token_path),
            "has_refresh_token": has_refresh,
            "expiry": expiry,
            "scopes": data.get("scopes", []),
            "actions": {
                "refresh": "/oauth/refresh",
                "new": "/oauth/start",
            },
        }
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to parse token file", path=str(token_path), error=str(e))
        return {"status": "error", "error": "Token file is corrupted or invalid."}, 500
    except Exception:
        logger.exception("Failed to read token status")
        return {
            "status": "error",
            "error": "Failed to read token status. See logs for details.",
        }, 500


@oauth_bp.route("/refresh", methods=["POST"])
def refresh():
    """Attempt to refresh the existing token (non-interactive)."""
    try:
        token_path = _get_token_path()
        if not token_path.exists():
            return {
                "status": "error",
                "message": "No token file found. Use /oauth/start to create one.",
            }, 404

        with open(token_path) as f:
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

        try:
            creds.refresh(Request())
        except google_auth_exceptions.RefreshError as e:
            logger.warning("Token refresh failed", error=str(e))
            return {
                "status": "error",
                "error": "Token refresh failed. The refresh token may be invalid or revoked.",
                "suggestion": "Try /oauth/start for a new token.",
            }, 400

        _save_token(creds)

        logger.info("Token refreshed successfully")
        return {
            "status": "success",
            "message": "Token refreshed successfully",
            "expiry": creds.expiry.isoformat() if creds.expiry else None,
        }

    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to parse token file", path=str(token_path), error=str(e))
        return {
            "status": "error",
            "error": "Token file is corrupted or invalid.",
            "suggestion": "Try /oauth/start for new token",
        }, 500
    except Exception:
        logger.exception("Token refresh failed")
        return {
            "status": "error",
            "error": "An internal error occurred while refreshing the token. See logs for details.",
            "suggestion": "Try /oauth/start for new token",
        }, 500


@oauth_bp.route("/start")
def start():
    """Start OAuth flow - redirects to Google consent screen."""
    try:
        # Ensure secret key is configured for session
        if not config.oauth_secret_key:
            logger.error("OAuth secret key not configured")
            return {
                "status": "error",
                "error": "OAuth not properly configured. Contact administrator.",
            }, 500

        client_config = _load_client_config()

        # Create flow with redirect URI from url_for (respects proxy headers)
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=url_for("oauth.callback", _external=True),
        )

        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",  # Force consent to get refresh token
        )

        # Store state in session for CSRF protection
        session["oauth_state"] = state
        logger.info("Starting OAuth flow", redirect_uri=flow.redirect_uri, state=state)

        return redirect(authorization_url)

    except FileNotFoundError as e:
        logger.error("OAuth token file not found", error=str(e))
        return {
            "status": "error",
            "error": (
                "OAuth token file not found. "
                "A token file with client_id and client_secret must exist to start the flow."
            ),
        }, 500
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to load OAuth configuration", error=str(e))
        return {
            "status": "error",
            "error": "OAuth configuration is invalid or corrupted.",
        }, 500
    except Exception:
        logger.exception("Failed to start OAuth flow")
        return {
            "status": "error",
            "error": "Failed to start OAuth flow. See logs for details.",
        }, 500


@oauth_bp.route("/callback")
def callback():
    """Handle OAuth callback from Google."""
    try:
        # Validate state parameter for CSRF protection
        callback_state = request.args.get("state")
        stored_state = session.get("oauth_state")

        if not callback_state or not stored_state or callback_state != stored_state:
            logger.error(
                "OAuth state mismatch",
                callback_state=callback_state,
                stored_state=stored_state,
            )
            return _render_oauth_page(
                title="OAuth Error",
                heading="Authorization Failed",
                message="Invalid state parameter. This may be a CSRF attack.",
                status_code=400,
            )

        # Clear state from session
        session.pop("oauth_state", None)

        client_config = _load_client_config()

        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=url_for("oauth.callback", _external=True),
            state=stored_state,
        )

        # Exchange authorization code for token
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        _save_token(creds)

        logger.info("OAuth flow completed successfully")

        expiry_str = creds.expiry.isoformat() if creds.expiry else "Unknown"
        return _render_oauth_page(
            title="OAuth Success",
            heading="Authorization Successful!",
            message="Gmail token has been refreshed and saved.",
            status_code=200,
            expiry=expiry_str,
        )

    except FileNotFoundError as e:
        logger.error("OAuth token file not found during callback", error=str(e))
        return _render_oauth_page(
            title="OAuth Error",
            heading="Authorization Failed",
            message=(
                "OAuth token file not found. "
                "A token file with client_id and client_secret must exist."
            ),
            status_code=500,
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to load OAuth configuration during callback", error=str(e))
        return _render_oauth_page(
            title="OAuth Error",
            heading="Authorization Failed",
            message="OAuth configuration is invalid or corrupted.",
            status_code=500,
        )
    except Exception:
        logger.exception("OAuth callback failed")
        return _render_oauth_page(
            title="OAuth Error",
            heading="Authorization Failed",
            message="An unexpected error occurred. Please check the server logs for details.",
            status_code=500,
        )
