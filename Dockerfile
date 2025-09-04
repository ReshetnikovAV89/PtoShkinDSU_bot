# Multi-stage is overkill here; keep it simple and fast for Koyeb.
FROM python:3.11-slim

# Recommended Python flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# OS deps (kept minimal). Add build-base if your deps require compilation.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
# If you have requirements.txt, it will be used. Otherwise we install a safe default set.
COPY requirements*.txt ./
RUN set -eux; \
    if [ -f requirements.txt ]; then \
        pip install --upgrade pip && pip install -r requirements.txt; \
    else \
        pip install --upgrade pip && pip install \
            python-telegram-bot[ext]==20.7 \
            python-dotenv \
            pandas \
            openpyxl \
            rapidfuzz \
            python-dateutil; \
    fi

# Copy project files
COPY . /app

# Non-root user (optional but recommended)
# RUN useradd -m bot && chown -R bot:bot /app
# USER bot

# Koyeb provides $PORT; our app will bind to it in webhook mode.
# EXPOSE is optional on Koyeb, but doesn't hurt for local runs.
EXPOSE 8080

# Start the bot.
# The Botv2.py contains logic:
# - If BASE_URL is set -> WEBHOOK mode (listens on 0.0.0.0:$PORT)
# - Else -> POLLING mode (local dev)
CMD ["python", "Botv2.py"]
