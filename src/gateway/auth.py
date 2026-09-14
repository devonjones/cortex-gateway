"""Token auth for requests that arrive from outside the Docker network.

TRUST IS DECIDED BY THE PORT, THEN CONFIRMED BY THE SOURCE.

The gateway binds two ports. The internal one carries peer traffic and is
never published to the host; the external one is the only thing Docker
publishes, and always requires a token. Which socket a request arrived on is
therefore a structural fact enforced by port publishing, not an inference we
make from an address -- and it cannot drift the way a subnet list can when a
network is added.

The address check below is kept as a second lock, so that publishing the
internal port by mistake does not silently hand the API to the LAN.

Peers on the container network (triage-worker, gmail-sync, the ofelia jobs)
are trusted and send no credential. Anything else must present
``Authorization: Bearer <CORTEX_API_TOKEN>`` or ``X-Cortex-Token``.

WHY "INSIDE DOCKER" IS NOT JUST A SUBNET CHECK

The gateway is attached to three networks: cortex_default (172.26/16),
metrics (172.29/16) and traefik-public (172.24/16). A request proxied by
Traefik arrives with Traefik's *container* address as its source, which is a
private Docker address like any peer's. Trusting "any Docker subnet" would
therefore hand the whole API to anyone who can reach the Traefik route --
failing open in exactly the case the token exists for.

So two conditions must BOTH hold for a request to be treated as internal:

  1. The source address is in a subnet where peer services actually live
     (cortex_default, metrics). traefik-public is deliberately excluded.
  2. No proxy headers are present. Traefik sets X-Forwarded-For; its presence
     means the request was relayed on behalf of someone else, whoever that is.

Spoofing the header cannot grant access -- it only ever removes trust. And a
LAN client cannot spoof condition 1: verified on this deployment that a LAN
request to a published port arrives with the client's own address intact,
not masqueraded to the bridge gateway, so LAN traffic is externally
addressed and needs the token. (Had Docker rewritten it, address checks
would have been useless and the port lock would carry the whole boundary.)

Exempt paths are health and metrics only, so monitoring keeps working without
distributing a credential to it.
"""

from __future__ import annotations

import hmac
import ipaddress
from typing import Any

from cortex_utils.logging import get_logger
from flask import Flask, Response, jsonify, request

logger = get_logger()

# Port carrying peer traffic. Never publish it to the host: everything below
# assumes reaching it already required being inside the container network.
DEFAULT_INTERNAL_PORT = 8080

# No default. Which subnets carry peer traffic is deployment topology, and
# this repo is public -- baking in one deployment's Docker CIDRs both leaks
# it and silently misconfigures every other deployment. Required whenever a
# token is set; see init_auth.
#
# Critically, this list must EXCLUDE whatever subnet the reverse proxy sits
# on. A request relayed from outside arrives with the proxy's container
# address, which is private and otherwise indistinguishable from a peer's.
TRUSTED_SUBNETS_ENV = "CORTEX_TRUSTED_SUBNETS"

# Headers that mean "this was relayed for someone else".
PROXY_HEADERS = ("X-Forwarded-For", "X-Real-IP", "Forwarded")

# Exact paths, or a path plus a "/" segment boundary. NEVER a bare prefix
# match: `startswith("/health")` also admits `/healthz-admin`, so adding any
# future route whose name merely begins with an exempt one would silently
# punch a hole in the gate.
#
# /oauth/start and /oauth/callback are the two browser-redirect legs of the
# Gmail OAuth grant. Google redirects the USER'S BROWSER to /oauth/callback,
# and a browser redirect cannot carry an Authorization header -- gating them
# breaks the flow outright rather than merely inconveniencing it. /callback
# has its own CSRF protection (a state parameter tied to the Flask session),
# and /start only redirects to Google's consent screen; neither grants
# anything on its own.
#
# /oauth/refresh and /oauth/status are deliberately NOT exempt: they are
# programmatic, callable with a token, and /refresh mutates stored
# credentials.
EXEMPT_PATHS = ("/health", "/metrics", "/oauth/start", "/oauth/callback")


def _is_exempt(path: str) -> bool:
    """Exact match, or an exempt path followed by a `/` segment boundary."""
    return any(path == e or path.startswith(e + "/") for e in EXEMPT_PATHS)


def _parse_subnets(raw: str) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    nets = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            nets.append(ipaddress.ip_network(chunk, strict=False))
        except ValueError:
            logger.error("trusted_subnet_invalid", subnet=chunk)
    return nets


def is_internal(
    remote_addr: str | None,
    headers: Any,
    trusted: list[ipaddress.IPv4Network | ipaddress.IPv6Network],
    server_port: int | str | None = None,
    internal_port: int | None = None,
) -> bool:
    """True only for an unrelayed peer-subnet request on the internal port.

    Both locks must hold. The port says "this socket is not published
    externally"; the address says "and you really are a peer, not the LAN
    reaching a port that got published by accident".
    """
    if internal_port is not None:
        # Fail closed. Previously a missing SERVER_PORT skipped this check
        # entirely and fell back to address-only trust, quietly contradicting
        # the "both locks must hold" design in the worst direction: an
        # unknown port became an allowed one.
        if server_port is None:
            return False
        try:
            if int(server_port) != internal_port:
                return False
        except (TypeError, ValueError):
            return False
    if not remote_addr:
        return False
    if any(headers.get(h) for h in PROXY_HEADERS):
        return False
    try:
        ip = ipaddress.ip_address(remote_addr)
    except ValueError:
        return False
    return any(ip in net for net in trusted)


def init_auth(
    app: Flask,
    token: str,
    trusted_subnets: str = "",
    internal_port: int | None = DEFAULT_INTERNAL_PORT,
) -> None:
    """Install the before_request gate.

    With no token configured the gateway stays open, but says so loudly at
    startup -- silently unauthenticated is how it got to production the first
    time.

    With a token configured, trusted_subnets is REQUIRED: an empty list would
    mean no request is ever internal, so every peer call would start failing
    at once. Refusing to start is a better failure than that.
    """
    trusted = _parse_subnets(trusted_subnets)

    if token and not trusted:
        raise ValueError(
            f"{TRUSTED_SUBNETS_ENV} must be set when CORTEX_API_TOKEN is set. "
            "List the subnets peer services call from, e.g. the compose "
            "network's CIDR plus 127.0.0.1/32. Exclude the reverse-proxy "
            "network: a relayed request arrives with the proxy's own private "
            "address and would otherwise be trusted as a peer."
        )

    if not token:
        logger.warning(
            "api_token_not_set",
            message=(
                "CORTEX_API_TOKEN is unset: every caller is accepted, including "
                "from outside the Docker network. Set it to require a token."
            ),
        )
    else:
        logger.info(
            "api_token_enabled",
            trusted_subnets=[str(n) for n in trusted],
            internal_port=internal_port,
            exempt=list(EXEMPT_PATHS),
        )

    @app.before_request
    def _require_token() -> Response | tuple[Response, int] | None:
        if not token:
            return None
        path = request.path or "/"
        if _is_exempt(path):
            return None
        if is_internal(
            request.remote_addr,
            request.headers,
            trusted,
            request.environ.get("SERVER_PORT"),
            internal_port,
        ):
            return None

        presented = _presented_token(request.headers)
        if presented and hmac.compare_digest(presented, token):
            return None

        # Log the attempt, never the token. remote_addr is attacker-influenced,
        # so keep it a structured field rather than interpolating it.
        logger.warning(
            "api_auth_rejected",
            remote_addr=request.remote_addr,
            path=path,
            method=request.method,
            server_port=request.environ.get("SERVER_PORT"),
            had_credential=bool(presented),
        )
        return (
            jsonify(
                {
                    "error": "authentication required",
                    "detail": (
                        "requests from outside the Docker network must present "
                        "Authorization: Bearer <token> or X-Cortex-Token"
                    ),
                }
            ),
            401,
        )


def _presented_token(headers: Any) -> str:
    """Pull a bearer token out of the request, or return ''."""
    auth: str = headers.get("Authorization", "") or ""
    scheme, _, value = auth.partition(" ")
    if scheme.lower() == "bearer" and value.strip():
        return value.strip()
    direct: str = headers.get("X-Cortex-Token", "") or ""
    return direct.strip()


__all__ = ["init_auth", "is_internal"]
