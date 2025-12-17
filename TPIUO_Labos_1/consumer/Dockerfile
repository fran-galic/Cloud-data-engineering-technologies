FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv
RUN uv sync --frozen

COPY consumer.py .

# Cloud Run expects the app to listen on $PORT
ENV PORT=8080

# Disable stdout buffering so prints show up immediately
ENV PYTHONUNBUFFERED=1

CMD ["uv", "run", "consumer.py"]
