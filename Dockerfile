# Use a Python base image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv

# Set the working directory in the container
WORKDIR /app

# Enable bytecode compilation for better performance
ENV UV_COMPILE_BYTECODE=1

# Use copy mode for mounting cache to avoid linking issues
ENV UV_LINK_MODE=copy

# Install project dependencies using uv
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-install-project --no-dev --no-editable

# Copy the rest of the application code
ADD . /app

# Version for setuptools-scm (hatch-vcs) when .git is not available
# This is set at build time by Docker's build system
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# Install the application
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Create a minimal runtime image
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Copy the installed dependencies and the virtual environment
COPY --from=uv --chown=appuser:appuser /app/.venv /app/.venv

# Set the PATH to include the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Configure logging to show output
ENV PYTHONUNBUFFERED=1

# Default to stdio transport for MCP
ENV MCP_TRANSPORT=stdio

# Switch to non-root user
USER appuser

# Set the default entrypoint
ENTRYPOINT ["dbt-mcp"]
