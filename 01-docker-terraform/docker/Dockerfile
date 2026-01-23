FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy uv installer script and install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml .
COPY ingest_data.py .

# Install dependencies using uv
RUN uv pip install --system -r pyproject.toml

# Set the entrypoint to run the ingestion script
ENTRYPOINT ["python", "ingest_data.py"]
