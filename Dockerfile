# ---- Runtime ----
FROM python:3.11-slim

# Базовые настройки и ускорение pip
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080

# Системные пакеты: tzdata (часовой пояс), curl (на всякий случай для отладочных healthchecks)
# Без build-essential — колёсики numpy/pandas ставятся из бинарных wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ставим зависимости отдельно для лучшего кеширования
COPY requirements.txt .
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

# Копируем исходники и данные
COPY . .

# Non-root пользователь (Koyeb best practice)
RUN useradd -m -u 10001 appuser && chown -R appuser /app
USER appuser

# Koyeb будет пробрасывать $PORT; Botv2.py должен слушать 0.0.0.0:$PORT при WEBHOOK-режиме
EXPOSE 8080

# Запуск бота. Если задан BASE_URL — запустится webhook-сервер, иначе polling.
CMD ["python", "Botv2.py"]