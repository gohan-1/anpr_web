FROM python:3.12-slim

# Python settings for containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# A system library OpenCV needs on a minimal Linux
RUN apt-get update \
    && apt-get install -y --no-install-recommends libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Libraries first, code second (see "why" below)
COPY requirements.txt .
RUN pip install -r requirements.txt

# A normal user, so the code doesn't run as root
RUN useradd --create-home appuser \
    && mkdir -p /app/state && chown appuser /app/state   # the producer remembers sent folders here (volume)
COPY --chown=appuser . .
USER appuser

# Default command: a quick self-check. Each container will override this.
CMD ["python", "web/manage.py", "check"]

