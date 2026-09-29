FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    LAKEHOUSE_DIR=/lakehouse \
    HOME=/tmp

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra dashboard --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --extra dashboard
COPY dashboard ./dashboard

USER nobody
WORKDIR /app/dashboard
EXPOSE 8501
HEALTHCHECK --interval=15s --timeout=5s --retries=10 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
