"""The trust boundary is "unrelayed request from a peer subnet", not "a private IP".

The gateway sits on three Docker networks. Traefik is one of them, so a request
relayed from the internet arrives with a private 172.24/16 source address that
looks exactly like a peer service. A naive "is it RFC1918?" check therefore
hands the whole API to anyone who can reach the Traefik route -- failing open
in precisely the case the token exists for.

These pin both halves of the rule: the subnet must be one where peers actually
live, AND no proxy header may be present.
"""

from __future__ import annotations

import ipaddress

import pytest

from gateway.auth import _parse_subnets, _presented_token, is_internal

# A fixture topology, not this deployment's. The production values live in
# CORTEX_TRUSTED_SUBNETS, which is now required rather than defaulted.
_TRUSTED_FIXTURE = "172.26.0.0/16,172.29.0.0/16,127.0.0.1/32"
TRUSTED = _parse_subnets(_TRUSTED_FIXTURE)


class Headers(dict):
    """Minimal stand-in for werkzeug's case-insensitive headers."""

    def get(self, key, default=None):  # type: ignore[override]
        for k, v in self.items():
            if k.lower() == key.lower():
                return v
        return default


def test_peer_service_is_internal() -> None:
    assert is_internal("172.26.0.13", Headers(), TRUSTED)


def test_metrics_network_is_internal() -> None:
    assert is_internal("172.29.0.11", Headers(), TRUSTED)


def test_lan_client_is_external() -> None:
    """A private address outside the peer subnets is still external.

    Verified on the deployment that a LAN request keeps its own source
    address rather than being masqueraded to the bridge gateway, so this is
    the shape real LAN traffic arrives in. The address here is a stand-in --
    the repo is public, so it must not name a real host.
    """
    assert not is_internal("10.0.0.99", Headers(), TRUSTED)


def test_traefik_subnet_is_not_trusted() -> None:
    """The whole point. 172.24/16 is a Docker subnet but it is the front door."""
    assert not is_internal("172.24.0.5", Headers(), TRUSTED)


@pytest.mark.parametrize("header", ["X-Forwarded-For", "X-Real-IP", "Forwarded"])
def test_proxy_header_removes_trust_even_from_a_peer_subnet(header: str) -> None:
    """A relayed request is external no matter where it was relayed from.

    Spoofing this header can only ever LOSE access, never gain it, so an
    attacker has no incentive and a proxy has no way to be mistaken for a peer.
    """
    assert is_internal("172.26.0.13", Headers(), TRUSTED)
    assert not is_internal("172.26.0.13", Headers({header: "203.0.113.7"}), TRUSTED)


def test_unknown_or_missing_source_is_external() -> None:
    assert not is_internal(None, Headers(), TRUSTED)
    assert not is_internal("", Headers(), TRUSTED)
    assert not is_internal("not-an-ip", Headers(), TRUSTED)


def test_public_address_is_external() -> None:
    assert not is_internal("203.0.113.7", Headers(), TRUSTED)


def test_trusted_subnets_exclude_traefik() -> None:
    """Guard the constant itself: adding 172.24/16 here would fail open."""
    traefik = ipaddress.ip_address("172.24.0.5")
    assert not any(traefik in net for net in TRUSTED)


def test_bearer_and_direct_header_are_both_accepted() -> None:
    assert _presented_token(Headers({"Authorization": "Bearer abc123"})) == "abc123"
    assert _presented_token(Headers({"authorization": "bearer abc123"})) == "abc123"
    assert _presented_token(Headers({"X-Cortex-Token": "abc123"})) == "abc123"


def test_malformed_authorization_yields_no_token() -> None:
    assert _presented_token(Headers({"Authorization": "abc123"})) == ""
    assert _presented_token(Headers({"Authorization": "Basic abc123"})) == ""
    assert _presented_token(Headers({"Authorization": "Bearer "})) == ""
    assert _presented_token(Headers()) == ""


# --- end to end through a real Flask app -------------------------------------
#
# The predicate being right is not the same as the gate being installed. These
# drive a real app through init_auth and assert on status codes.


def _app(token: str):
    from flask import Flask, jsonify

    from gateway.auth import init_auth

    app = Flask(__name__)
    init_auth(app, token, _TRUSTED_FIXTURE)

    @app.route("/health")
    def health():
        return jsonify(ok=True)

    @app.route("/config", methods=["GET", "PUT"])
    def cfg():
        return jsonify(ok=True)

    return app


def _get(app, path="/config", addr="10.0.0.99", port=8080, **headers):
    """Werkzeug derives SERVER_PORT from the host, so set it via base_url.

    Passing SERVER_PORT in environ_base is silently overwritten -- which is how
    the first version of these tests ended up asserting against port 80.
    """
    return app.test_client().get(
        path,
        base_url=f"http://localhost:{port}",
        environ_base={"REMOTE_ADDR": addr},
        headers=headers,
    )


def test_external_request_without_token_is_rejected() -> None:
    r = _get(_app("s3cret"))
    assert r.status_code == 401
    assert r.get_json()["error"] == "authentication required"


def test_external_request_with_token_is_allowed() -> None:
    app = _app("s3cret")
    assert _get(app, Authorization="Bearer s3cret").status_code == 200
    assert _get(app, **{"X-Cortex-Token": "s3cret"}).status_code == 200


def test_external_request_with_wrong_token_is_rejected() -> None:
    assert _get(_app("s3cret"), Authorization="Bearer wrong").status_code == 401


def test_internal_request_needs_no_token() -> None:
    assert _get(_app("s3cret"), addr="172.26.0.13").status_code == 200


def test_traefik_relayed_request_needs_a_token() -> None:
    """The fail-open case: private source IP, but relayed from outside."""
    app = _app("s3cret")
    relayed = {"X-Forwarded-For": "203.0.113.7"}
    assert _get(app, addr="172.24.0.5", **relayed).status_code == 401
    assert _get(app, addr="172.26.0.13", **relayed).status_code == 401
    with_token = _get(app, addr="172.26.0.13", Authorization="Bearer s3cret", **relayed)
    assert with_token.status_code == 200


def test_health_is_exempt_so_monitoring_needs_no_credential() -> None:
    assert _get(_app("s3cret"), path="/health").status_code == 200


def test_writes_are_gated_too() -> None:
    app = _app("s3cret")
    unauth = app.test_client().put(
        "/config", base_url="http://localhost:8080", environ_base={"REMOTE_ADDR": "10.0.0.99"}
    )
    assert unauth.status_code == 401, "PUT /config from the LAN must require a token"


def test_no_token_configured_leaves_the_gateway_open() -> None:
    """Current production behaviour, preserved deliberately until the token is deployed."""
    assert _get(_app("")).status_code == 200


# --- port-based trust --------------------------------------------------------
#
# Which socket a request arrived on is enforced by Docker port publishing, so
# it cannot drift the way a subnet list can. The address check stays as a
# second lock for the case where the internal port gets published by mistake.


def test_peer_on_the_external_port_still_needs_a_token() -> None:
    """Arriving on the published port is external, whatever the source IP."""
    assert is_internal("172.26.0.13", Headers(), TRUSTED, 8080, 8080)
    assert not is_internal("172.26.0.13", Headers(), TRUSTED, 8098, 8080)


def test_lan_on_the_internal_port_still_needs_a_token() -> None:
    """Second lock: publishing the internal port by accident must not fail open."""
    assert not is_internal("10.0.0.99", Headers(), TRUSTED, 8080, 8080)


def test_unparseable_port_is_external() -> None:
    assert not is_internal("172.26.0.13", Headers(), TRUSTED, "not-a-port", 8080)


def test_port_check_is_skipped_when_not_configured() -> None:
    """Backward compatible with a single-port deployment."""
    assert is_internal("172.26.0.13", Headers(), TRUSTED, 9999, None)


def test_gate_end_to_end_rejects_peer_on_external_port() -> None:
    from flask import Flask, jsonify

    from gateway.auth import init_auth

    app = Flask(__name__)
    init_auth(app, "s3cret", _TRUSTED_FIXTURE, internal_port=8080)

    @app.route("/config")
    def cfg():
        return jsonify(ok=True)

    peer_internal = app.test_client().get(
        "/config", base_url="http://localhost:8080", environ_base={"REMOTE_ADDR": "172.26.0.13"}
    )
    peer_external = app.test_client().get(
        "/config", base_url="http://localhost:8098", environ_base={"REMOTE_ADDR": "172.26.0.13"}
    )
    assert peer_internal.status_code == 200
    assert peer_external.status_code == 401


# --- exempt-path boundary: the P1 two reviewers found independently ---------


def test_exempt_paths_do_not_match_by_bare_prefix() -> None:
    """`/healthz-admin` must NOT inherit `/health`'s exemption.

    A bare startswith() means any future route whose name merely begins with
    an exempt one is silently unauthenticated. That is how an auth gate
    develops a hole nobody edited.
    """
    from gateway.auth import _is_exempt

    assert _is_exempt("/health")
    assert _is_exempt("/health/")
    assert _is_exempt("/health/deep")
    assert not _is_exempt("/healthz-admin")
    assert not _is_exempt("/health-admin")
    assert not _is_exempt("/healthcheck")
    assert not _is_exempt("/metricsx")


def test_only_the_oauth_callback_is_exempt() -> None:
    """/oauth/start must NOT be exempt -- that was an account-takeover hole.

    With both legs open, any caller reaching the external port could drive
    the whole grant itself: /start, consent as its own Google account, land
    on /callback, and overwrite the production token -- silently repointing
    gmail-sync at an attacker's mailbox. The state parameter is no defence
    when the attacker drives both legs.
    """
    from gateway.auth import _is_exempt

    assert _is_exempt("/oauth/callback")
    assert not _is_exempt("/oauth/start"), "gating /start is what closes the takeover"
    assert not _is_exempt("/oauth/refresh")
    assert not _is_exempt("/oauth/status")
    assert not _is_exempt("/oauth")


def test_gate_allows_the_oauth_callback_without_a_token() -> None:
    from flask import Flask, jsonify

    from gateway.auth import init_auth

    app = Flask(__name__)
    init_auth(app, "s3cret", _TRUSTED_FIXTURE, internal_port=8080)

    @app.route("/oauth/callback")
    def cb():
        return jsonify(ok=True)

    @app.route("/oauth/refresh", methods=["POST"])
    def rf():
        return jsonify(ok=True)

    @app.route("/oauth/start")
    def st():
        return jsonify(ok=True)

    ext = {"REMOTE_ADDR": "10.0.0.99"}
    assert (
        app.test_client()
        .get("/oauth/callback", base_url="http://localhost:8098", environ_base=ext)
        .status_code
        == 200
    )
    assert (
        app.test_client()
        .post("/oauth/refresh", base_url="http://localhost:8098", environ_base=ext)
        .status_code
        == 401
    )
    # The takeover leg. /oauth/start must be refused unauthenticated from
    # outside: with it exempt, any caller reaching the external port could
    # drive the whole grant and overwrite the stored token.
    assert (
        app.test_client()
        .get("/oauth/start", base_url="http://localhost:8098", environ_base=ext)
        .status_code
        == 401
    )
    # ...while an operator inside the container network can still drive it,
    # via _require_token()'s internal bypass rather than an exemption.
    assert (
        app.test_client()
        .get(
            "/oauth/start",
            base_url="http://localhost:8080",
            environ_base={"REMOTE_ADDR": "172.26.0.13"},
        )
        .status_code
        == 200
    )


# --- port lock fails closed -------------------------------------------------


def test_missing_server_port_is_not_trusted() -> None:
    """An unknown port must not become an allowed one.

    Previously a missing SERVER_PORT skipped the port check and fell back to
    address-only trust, contradicting "both locks must hold" in the worst
    direction.
    """
    assert not is_internal("172.26.0.13", Headers(), TRUSTED, None, 8080)


# --- IPv6 -------------------------------------------------------------------


def test_ipv6_sources() -> None:
    trusted6 = _parse_subnets("fd00::/8,::1/128")
    assert is_internal("fd00::1", Headers(), trusted6)
    assert not is_internal("2001:db8::1", Headers(), trusted6)
    # An IPv6 source against an IPv4-only trust list is external, not a crash.
    assert not is_internal("fd00::1", Headers(), TRUSTED)
    # IPv4-mapped IPv6 must not sneak past an IPv4 trust list.
    assert not is_internal("::ffff:172.26.0.13", Headers(), TRUSTED)


# --- subnet parsing ---------------------------------------------------------


def test_parse_subnets_drops_malformed_entries_without_crashing() -> None:
    nets = _parse_subnets("172.26.0.0/16, not-a-subnet, ,999.0.0.1/8,127.0.0.1/32")
    assert len(nets) == 2, "valid entries survive, invalid are dropped"
    assert is_internal("172.26.0.5", Headers(), nets, 8080, 8080)


def test_empty_trust_list_trusts_nobody() -> None:
    """Fail closed: an empty list must not mean 'allow all'."""
    assert not is_internal("172.26.0.13", Headers(), [], 8080, 8080)


def test_init_auth_refuses_a_token_without_trusted_subnets() -> None:
    """Refusing to start beats every peer call failing at once."""
    import pytest
    from flask import Flask

    from gateway.auth import init_auth

    with pytest.raises(ValueError, match="CORTEX_TRUSTED_SUBNETS"):
        init_auth(Flask(__name__), "s3cret", "")


def test_no_token_still_starts_without_trusted_subnets() -> None:
    """The open-gateway path is unchanged, so this can deploy before the token."""
    from flask import Flask

    from gateway.auth import init_auth

    init_auth(Flask(__name__), "", "")  # must not raise


# --- round 2 findings --------------------------------------------------------


def test_non_ascii_token_is_rejected_not_a_500() -> None:
    """hmac.compare_digest raises TypeError on non-ASCII str.

    Before this was fixed, `X-Cortex-Token: café` escaped _require_token as an
    unhandled exception. Flask turned it into a 500, which still denied the
    view but discarded the 401 body and the api_auth_rejected audit line, and
    handed any external caller a one-request way to spray tracebacks.
    """
    app = _app("s3cret")
    for bad in ("café", "tökén", "日本語", "\udcff"):
        r = _get(app, **{"X-Cortex-Token": bad})
        assert r.status_code == 401, f"{bad!r} must be a clean 401, not a 500"
        assert r.get_json()["error"] == "authentication required"


def test_a_non_ascii_token_can_still_authenticate() -> None:
    """Encoding both sides must not break a legitimately non-ASCII token."""
    app = _app("pàsswörd")
    assert _get(app, **{"X-Cortex-Token": "pàsswörd"}).status_code == 200
    assert _get(app, **{"X-Cortex-Token": "pàssword"}).status_code == 401


def test_unknown_paths_are_denied_by_default_end_to_end() -> None:
    """The deny-by-default property, through a real app rather than _is_exempt.

    A refactor that stopped calling _is_exempt() from _require_token would
    pass the unit tests while reopening the prefix hole. This drives the gate.
    """
    from flask import Flask, jsonify

    from gateway.auth import init_auth

    app = Flask(__name__)
    init_auth(app, "s3cret", _TRUSTED_FIXTURE, internal_port=8080)

    @app.route("/healthz-admin")
    def healthz_admin():
        return jsonify(ok=True)

    @app.route("/anything")
    def anything():
        return jsonify(ok=True)

    ext = {"REMOTE_ADDR": "10.0.0.99"}
    for path in ("/healthz-admin", "/anything"):
        assert (
            app.test_client()
            .get(path, base_url="http://localhost:8098", environ_base=ext)
            .status_code
            == 401
        ), f"{path} must be gated"
        assert (
            app.test_client()
            .get(
                path,
                base_url="http://localhost:8098",
                environ_base=ext,
                headers={"Authorization": "Bearer s3cret"},
            )
            .status_code
            == 200
        )


def test_create_app_refuses_to_boot_without_trusted_subnets(monkeypatch) -> None:
    """The real startup chain: env var -> config -> create_app -> raise.

    This PR changed config.py's CORTEX_TRUSTED_SUBNETS default to "", which is
    exactly the input that trips the raise, so the chain is worth exercising
    rather than only the bare-Flask path.
    """
    import importlib

    import pytest

    monkeypatch.setenv("CORTEX_API_TOKEN", "s3cret")
    monkeypatch.setenv("CORTEX_TRUSTED_SUBNETS", "")
    monkeypatch.setenv("POSTGRES_PASSWORD", "x")
    monkeypatch.setenv("OAUTH_SECRET_KEY", "x")
    monkeypatch.setenv("OAUTH_TOKEN_PATH", "/tmp/t.json")

    import gateway.config

    importlib.reload(gateway.config)
    import gateway.app

    importlib.reload(gateway.app)
    with pytest.raises(ValueError, match="CORTEX_TRUSTED_SUBNETS"):
        gateway.app.create_app()
