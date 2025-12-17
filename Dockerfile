FROM python:3.11-slim

WORKDIR /app

# Install git (needed for git dependencies like cortex-utils)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY src/ ./src/

# Install dependencies
RUN uv sync --frozen --no-dev

# Create non-root user
RUN useradd -r -s /bin/false appuser && chown -R appuser:appuser /app
USER appuser

# Expose ports
EXPOSE 8080 8001

# Run with gunicorn
CMD ["uv", "run", "gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "gateway.app:create_app()"]
