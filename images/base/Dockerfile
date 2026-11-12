# leasehold base images. Build from the repository root:
#
#   docker build -f images/base/Dockerfile --target runtime     -t leasehold-base .
#   docker build -f images/base/Dockerfile --target control-api -t leasehold-control-api .
#
# runtime:     Python 3.12 + leasehold with its locked dependencies. Every
#              component image builds FROM it, so a deploy pulls only the
#              component's own layers.
# control-api: runtime + the server extra; runs `leasehold api serve`.
#
# Dependencies come from uv.lock (exact versions), not from a fresh resolve.

ARG PYTHON=python:3.12-slim-bookworm

FROM ghcr.io/astral-sh/uv:0.12.9 AS uv

FROM ${PYTHON} AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/leasehold \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /src
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
 && cp -a /opt/leasehold /opt/leasehold-runtime \
 && uv sync --frozen --no-dev --no-editable --extra server

FROM ${PYTHON} AS common
# No apt: the slim image already has CA certificates, and tini isn't needed (see
# the ENTRYPOINT note below), so the image builds from Python and uv.lock alone.
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin app \
 && mkdir -p /app && chown app:app /app
ENV PATH=/opt/leasehold/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LEASEHOLD_SERVICE_FILE=/app/service.yml
WORKDIR /app
LABEL org.opencontainers.image.source="https://github.com/Thewildweb/leasehold" \
      org.opencontainers.image.licenses="TBD"
# Python runs as PID 1. PID 1 ignores signals it has no handler for, but the
# leasehold runtime installs handlers for SIGTERM and SIGINT, so a Nomad stop
# still triggers the graceful drain. Components that start child processes can
# set `init = true` in the Nomad docker config to get a reaper.

FROM common AS runtime
COPY --from=build /opt/leasehold-runtime /opt/leasehold
LABEL org.opencontainers.image.description="leasehold runtime base image"
USER app
CMD ["python", "-c", "import leasehold; print('leasehold', leasehold.__version__)"]

FROM common AS control-api
COPY --from=build /opt/leasehold /opt/leasehold
LABEL org.opencontainers.image.description="leasehold control API"
USER app
EXPOSE 8400
CMD ["leasehold", "api", "serve", "--host", "0.0.0.0", "--port", "8400"]
