FROM python:3.11-slim

# (ускоряет сборку pandas)
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# код
COPY . .

# порт нужен только если включишь вебхуки
ENV PORT=8000

# запускаем бота в polling-режиме
CMD ["python", "Botv2.py"]