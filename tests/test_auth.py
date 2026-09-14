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

from gateway.auth import DEFAULT_TRUSTED_SUBNETS, _parse_subnets, _presented_token, is_internal

TRUSTED = _parse_subnets(DEFAULT_TRUSTED_SUBNETS)


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
    """Verified on the deployment: a LAN request keeps its real source IP."""
    assert not is_internal("10.5.2.12", Headers(), TRUSTED)


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
    init_auth(app, token)

    @app.route("/health")
    def health():
        return jsonify(ok=True)

    @app.route("/config", methods=["GET", "PUT"])
    def cfg():
        return jsonify(ok=True)

    return app


def _get(app, path="/config", addr="10.5.2.12", port=8080, **headers):
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
        "/config", base_url="http://localhost:8080", environ_base={"REMOTE_ADDR": "10.5.2.12"}
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
    assert not is_internal("10.5.2.12", Headers(), TRUSTED, 8080, 8080)


def test_unparseable_port_is_external() -> None:
    assert not is_internal("172.26.0.13", Headers(), TRUSTED, "not-a-port", 8080)


def test_port_check_is_skipped_when_not_configured() -> None:
    """Backward compatible with a single-port deployment."""
    assert is_internal("172.26.0.13", Headers(), TRUSTED, 9999, None)


def test_gate_end_to_end_rejects_peer_on_external_port() -> None:
    from flask import Flask, jsonify

    from gateway.auth import init_auth

    app = Flask(__name__)
    init_auth(app, "s3cret", internal_port=8080)

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
