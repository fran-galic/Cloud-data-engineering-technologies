# Use lightweight Python image
FROM python:3.11-slim

# Set working dir inside container
WORKDIR /app

# Copy project metadata first (for dependency caching)
COPY pyproject.toml uv.lock ./

# Install uv package manager
RUN pip install uv

# Sync dependencies inside container
RUN uv sync --frozen

# Copy the actual application code
COPY TPIUO_Labos_1/producer/producer.py .

# Default command when container starts
CMD ["uv", "run", "producer.py"]
