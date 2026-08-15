FROM python:3.12-slim

# Run as non-root
RUN useradd --create-home --uid 1000 bot
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

USER bot

# No ports to expose — outbound only (REST + WebSocket to clawroyale.ai)
CMD ["python", "-u", "bot.py"]
