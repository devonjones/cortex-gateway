"""Triage config management API endpoints."""

import difflib

import structlog
from cortex_utils.triage_config import (
    export_config_to_yaml,
    import_yaml_to_db,
    load_rules_from_string,
    validate_rules,
)
from flask import Blueprint, Response, jsonify, request
from yaml import YAMLError  # type: ignore[import]

from gateway.services.postgres import ConnectionContext, execute_query

logger = structlog.get_logger()

config_bp = Blueprint("config", __name__)


@config_bp.route("", methods=["GET"])
def get_active_config() -> Response | tuple[Response, int]:
    """Get the active config as YAML.

    Returns:
        200: YAML content (text/plain)
        404: No active config found
        500: Error loading config
    """
    try:
        with ConnectionContext() as conn:
            # Export active version (None = active)
            yaml_content = export_config_to_yaml(conn, version=None)

        return Response(
            yaml_content,
            mimetype="text/plain",
            headers={"Content-Disposition": "attachment; filename=config.yaml"},
        )

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception:
        logger.error("Failed to export active config", exc_info=True)
        return jsonify({"error": "Failed to export active config"}), 500


@config_bp.route("/versions", methods=["GET"])
def list_versions() -> Response | tuple[Response, int]:
    """List all config versions.

    Query params:
        limit: Max results (default: 20, max: 100)
        offset: Pagination offset (default: 0)

    Returns:
        200: List of versions with metadata
    """
    limit_param = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    # Validate parameters
    if limit_param <= 0 or offset < 0:
        return jsonify({"error": "limit must be positive and offset must be non-negative"}), 400

    limit = min(limit_param, 100)

    query = """
        SELECT
            version,
            config_hash,
            label_prefix,
            created_at,
            created_by,
            notes,
            is_active,
            rolled_back_from,
            COUNT(*) OVER() as total
        FROM triage_config_versions
        ORDER BY version DESC
        LIMIT %s OFFSET %s
    """

    rows = execute_query(query, (limit, offset))

    # Extract total and remove it from version records
    total = rows[0]["total"] if rows else 0
    versions = [{k: v for k, v in row.items() if k != "total"} for row in rows]

    return jsonify(
        {
            "versions": versions,
            "limit": limit,
            "offset": offset,
            "total": total,
        }
    )


@config_bp.route("/versions/<int:version>", methods=["GET"])
def get_version(version: int) -> Response | tuple[Response, int]:
    """Get a specific config version as YAML.

    Args:
        version: Config version number

    Returns:
        200: YAML content (text/plain)
        404: Version not found
        500: Error loading config
    """
    try:
        with ConnectionContext() as conn:
            yaml_content = export_config_to_yaml(conn, version=version)

        return Response(
            yaml_content,
            mimetype="text/plain",
            headers={"Content-Disposition": f"attachment; filename=config-v{version}.yaml"},
        )

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception:
        logger.error("Failed to export config version", version=version, exc_info=True)
        return jsonify({"error": "Failed to export config version"}), 500


@config_bp.route("", methods=["PUT", "POST"])
def update_config() -> Response | tuple[Response, int]:
    """Create new config version from YAML.

    Request body: YAML content (text/plain or application/yaml)
    Request headers:
        X-Created-By: Username/email creating the config (required)
        X-Notes: Optional description of changes

    Returns:
        201: New version created
        400: Invalid YAML or validation errors
        500: Import failed
    """
    # Get YAML content from request body
    yaml_content = request.get_data(as_text=True)
    if not yaml_content:
        return jsonify({"error": "Empty request body"}), 400

    # Get metadata from headers
    created_by = request.headers.get("X-Created-By")
    if not created_by:
        return jsonify({"error": "Missing X-Created-By header"}), 400

    notes = request.headers.get("X-Notes")

    try:
        # Validate before opening DB connection
        config = load_rules_from_string(yaml_content)
        errors = validate_rules(config)
        if errors:
            return jsonify({"error": "Validation failed", "errors": errors}), 400

        # If validation passes, import to database
        with ConnectionContext() as conn:
            version = import_yaml_to_db(conn, yaml_content, created_by, notes)
            conn.commit()

        return (
            jsonify(
                {
                    "message": "Config created successfully",
                    "version": version,
                    "created_by": created_by,
                    "notes": notes,
                }
            ),
            201,
        )

    except YAMLError as e:
        logger.warning("Invalid YAML in request to /config", error=str(e))
        return jsonify({"error": "Invalid YAML format"}), 400
    except ValueError as e:
        logger.error(
            "Failed to import config after validation",
            error=str(e),
            created_by=created_by,
            notes=notes,
            exc_info=True,
        )
        return jsonify({"error": "Import failed"}), 500
    except Exception as e:
        logger.error(
            "Failed to import config",
            error=str(e),
            created_by=created_by,
            notes=notes,
            exc_info=True,
        )
        return jsonify({"error": "Import failed"}), 500


@config_bp.route("/validate", methods=["POST"])
def validate_config() -> Response | tuple[Response, int]:
    """Validate YAML config without saving.

    Request body: YAML content (text/plain or application/yaml)

    Returns:
        200: Valid config with stats
        400: Invalid YAML or validation errors
    """
    yaml_content = request.get_data(as_text=True)
    if not yaml_content:
        return jsonify({"error": "Empty request body"}), 400

    try:
        # Parse YAML
        config = load_rules_from_string(yaml_content)

        # Validate
        errors = validate_rules(config)
        if errors:
            return (
                jsonify(
                    {
                        "valid": False,
                        "errors": errors,
                    }
                ),
                400,
            )

        # Count components
        chain_count = len(config.chains)
        rule_count = sum(len(rules) for rules in config.chains.values())
        priority_mappings = len(config.priority_email_mappings)
        fallback_mappings = len(config.fallback_email_mappings)

        return jsonify(
            {
                "valid": True,
                "stats": {
                    "chains": chain_count,
                    "rules": rule_count,
                    "priority_mappings": priority_mappings,
                    "fallback_mappings": fallback_mappings,
                },
            }
        )

    except YAMLError as e:
        logger.warning("Invalid YAML in request to /validate", error=str(e))
        return jsonify({"error": "Invalid YAML format"}), 400
    except ValueError as e:
        logger.warning("Config validation failed in /validate", error=str(e))
        return jsonify({"error": "Validation failed"}), 400
    except Exception as e:
        logger.error("Config validation failed", error=str(e), exc_info=True)
        return jsonify({"error": "An unexpected error occurred during validation"}), 500


@config_bp.route("/rollback/<int:version>", methods=["POST"])
def rollback_to_version(version: int) -> Response | tuple[Response, int]:
    """Rollback to a previous config version.

    Creates a new version with the content of the specified version.

    Args:
        version: Version number to rollback to

    Request headers:
        X-Created-By: Username/email performing rollback (required)
        X-Notes: Optional reason for rollback

    Returns:
        201: Rollback successful, new version created
        404: Version not found
        400: Missing required headers
        500: Rollback failed
    """
    created_by = request.headers.get("X-Created-By")
    if not created_by:
        return jsonify({"error": "Missing X-Created-By header"}), 400

    notes = request.headers.get("X-Notes") or f"Rollback to version {version}"

    try:
        with ConnectionContext() as conn:
            # Export the target version
            try:
                yaml_content = export_config_to_yaml(conn, version=version)
            except ValueError as e:
                logger.error("Version not found", version=version, error=str(e), exc_info=True)
                return jsonify({"error": f"Version {version} not found"}), 404

            # Import as new version with rollback marker
            try:
                new_version = import_yaml_to_db(conn, yaml_content, created_by, notes)
            except ValueError as e:
                conn.rollback()
                logger.error(
                    "Rollback failed: config from version is invalid",
                    version=version,
                    created_by=created_by,
                    notes=notes,
                    error=str(e),
                    exc_info=True,
                )
                return (
                    jsonify(
                        {
                            "error": (
                                f"Failed to import config from version {version} "
                                "as it is no longer valid."
                            )
                        }
                    ),
                    500,
                )

            # Mark as rollback in database
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE triage_config_versions
                    SET rolled_back_from = %s
                    WHERE version = %s
                    """,
                    (version, new_version),
                )
            conn.commit()

        return (
            jsonify(
                {
                    "message": f"Rolled back to version {version}",
                    "new_version": new_version,
                    "rolled_back_from": version,
                    "created_by": created_by,
                    "notes": notes,
                }
            ),
            201,
        )

    except Exception as e:
        logger.error(
            "Rollback failed",
            error=str(e),
            version=version,
            created_by=created_by,
            notes=notes,
            exc_info=True,
        )
        return jsonify({"error": "Rollback failed"}), 500


@config_bp.route("/diff/<int:v1>/<int:v2>", methods=["GET"])
def diff_versions(v1: int, v2: int) -> Response | tuple[Response, int]:
    """Get a diff between two config versions.

    Args:
        v1: First version number
        v2: Second version number

    Returns:
        200: Diff information
        404: One or both versions not found
        500: Diff failed
    """
    try:
        with ConnectionContext() as conn:
            yaml1 = export_config_to_yaml(conn, version=v1)
            yaml2 = export_config_to_yaml(conn, version=v2)

        # Use difflib for accurate line-by-line comparison
        lines1 = yaml1.splitlines()
        lines2 = yaml2.splitlines()

        # Generate proper diff using difflib.ndiff
        diff = list(difflib.ndiff(lines1, lines2))
        added_lines = [line[2:] for line in diff if line.startswith("+ ")]
        removed_lines = [line[2:] for line in diff if line.startswith("- ")]

        return jsonify(
            {
                "v1": v1,
                "v2": v2,
                "stats": {
                    "lines_added": len(added_lines),
                    "lines_removed": len(removed_lines),
                    "total_lines_v1": len(lines1),
                    "total_lines_v2": len(lines2),
                },
                "added": added_lines[:100],  # Limit to first 100 for API response
                "removed": removed_lines[:100],
                "note": (
                    "Use GET /config/versions/{v} to download full YAML " "for detailed comparison"
                ),
            }
        )

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception:
        logger.error("Diff failed", v1=v1, v2=v2, exc_info=True)
        return jsonify({"error": "Diff failed"}), 500
