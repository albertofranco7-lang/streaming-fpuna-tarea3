# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml README.md ./
RUN uv sync --frozen --no-dev

COPY notebook.py ./
COPY data ./data
COPY tests ./tests

RUN useradd --create-home --uid 10001 student \
    && chown -R student:student /app
USER student

EXPOSE 2718

CMD ["/app/.venv/bin/marimo", "edit", "notebook.py", "--headless", "--no-token", "--host", "0.0.0.0", "--port", "2718"]
