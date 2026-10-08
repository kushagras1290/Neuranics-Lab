FROM python:3.13-slim-bookworm@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.lock ./
COPY src ./src

RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock \
    && groupadd --gid 10001 morphx \
    && useradd --uid 10001 --gid morphx --no-create-home --home-dir /nonexistent morphx \
    && mkdir -p /data \
    && chown morphx:morphx /data

USER morphx:morphx
VOLUME ["/data"]
EXPOSE 8000

CMD ["python", "-m", "morphx.server", "--db-path", "/data/server.db", "--host", "0.0.0.0"]
