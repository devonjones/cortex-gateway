# Cortex Gateway

Unified REST API gateway for Cortex email automation services.

## Features

- **Email queries**: List, search, and retrieve email details and bodies
- **Queue management**: View queue stats, manage dead letters, retry failed jobs
- **Backfill control**: Trigger and monitor historical email processing
- **Triage operations**: Re-run classification, view statistics

## Quick Start

```bash
# Install dependencies
uv sync

# Run development server
uv run python -m gateway.app

# Or with gunicorn (production)
uv run gunicorn -w 4 -b 0.0.0.0:8080 'gateway.app:application'
```

## API Documentation

See [CLAUDE.md](CLAUDE.md) for full API endpoint documentation.

## Configuration

Set environment variables:

```bash
POSTGRES_HOST=localhost
POSTGRES_DB=cortex
POSTGRES_USER=cortex
POSTGRES_PASSWORD=secret
DUCKDB_API_URL=http://localhost:8081
```

## License

MIT
