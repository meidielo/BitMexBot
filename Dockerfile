# syntax=docker/dockerfile:1.7
FROM python:3.12.13-slim-bookworm@sha256:8a7e7cc04fd3e2bd787f7f24e22d5d119aa590d429b50c95dfe12b3abe52f48b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 bitmexbot \
    && useradd --system --uid 10001 --gid bitmexbot --home-dir /nonexistent \
       --shell /usr/sbin/nologin bitmexbot \
    && install -d -o bitmexbot -g bitmexbot -m 0750 \
       /app/data /app/logs /snapshot

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --requirement /app/requirements.txt

COPY --chown=bitmexbot:bitmexbot *.py /app/

USER 10001:10001

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main.py"]
