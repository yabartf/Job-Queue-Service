FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

# Dependencies are installed from the project metadata alone so this layer is
# cached until the dependency set itself changes, not on every source edit.
COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && pip install -e ".[dev]"

COPY alembic.ini ./
COPY app ./app

# Non-root: a worker executes job payloads, so it should not run as root even
# though the handlers here are simulated.
RUN useradd --create-home --uid 1000 appuser && chown -R appuser:appuser /srv
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
