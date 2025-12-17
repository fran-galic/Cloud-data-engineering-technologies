FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv
RUN uv sync --frozen

COPY TPIUO_Labos_2/Loader/load_to_bq.py ./load_to_bq.py

ENV PYTHONUNBUFFERED=1

CMD ["uv", "run", "python3", "load_to_bq.py"]
