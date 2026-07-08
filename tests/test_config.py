"""Tests for config API endpoints."""

import pytest
from flask import Flask

from gateway.blueprints.config import config_bp

# Minimal valid config with a chain that has rules. Exercises the real
# normalize_chains -> ChainConfig path so the rule-count stat is computed
# against ChainConfig objects (regression: cortex-fzkj).
VALID_YAML = """
default_label: Uncategorized
label_prefix: Cortex
chains:
  main:
    auto_run: true
    rules:
      - match:
          from_contains: example.com
        action:
          label: Test
      - match:
          from_contains: other.com
        action:
          label: Other
"""


@pytest.fixture
def client():
    app = Flask("test")
    app.register_blueprint(config_bp, url_prefix="/config")
    return app.test_client()


def test_validate_returns_stats_for_chainconfig(client):
    """/config/validate must count rules on ChainConfig.rules, not len(chain).

    Regression for cortex-fzkj: chains are ChainConfig objects, and
    sum(len(chain) ...) raised 'object of type ChainConfig has no len()',
    500-ing every validation.
    """
    response = client.post("/config/validate", data=VALID_YAML, content_type="text/plain")

    assert response.status_code == 200
    data = response.get_json()
    assert data["valid"] is True
    assert data["stats"]["chains"] == 1
    assert data["stats"]["rules"] == 2


def test_validate_rejects_invalid_yaml(client):
    """Malformed YAML is a 400, not a 500."""
    response = client.post("/config/validate", data="{not: valid: yaml:", content_type="text/plain")
    assert response.status_code == 400
