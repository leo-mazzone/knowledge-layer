FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable --no-cache

FROM python:3.13-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/.venv /app/.venv
RUN useradd --uid 10001 --create-home knowledge
USER knowledge
ENV PATH="/app/.venv/bin:$PATH" BIND_HOST=0.0.0.0 PORT=8080 PYTHONDONTWRITEBYTECODE=1
EXPOSE 8080
CMD ["knowledge", "serve"]
