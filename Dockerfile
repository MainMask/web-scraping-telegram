# syntax=docker/dockerfile:1
FROM python:3.11-slim

# unbuffered so `docker logs` shows scrape progress live; no .pyc clutter
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /src
COPY pyproject.toml ./
COPY telegram_scraper ./telegram_scraper
RUN --mount=type=cache,target=/root/.cache/pip pip install .

# session file + output/ land here; mount a host dir over it
WORKDIR /data

ENTRYPOINT ["telegram-scraper"]
CMD ["menu"]
