# Use a base image with Python 3.10
FROM python:3.10-slim

# Install system dependencies for DBT and Airflow
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    libpq-dev \
    libsasl2-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# # Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.8.2

#Set environnement for dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt_projects

# Set up environment variables
ENV PATH="${HOME}/.local/bin:${PATH}"

# Set working directory (This should match where the project will be copied)
WORKDIR /workspace

# #Copy only the dependency files first (for caching efficiency)
# COPY pyproject.toml poetry.lock /workspace/

# Copy the project files from your local machine into the container
COPY . /workspace/

# Ensure Poetry installs dependencies
# RUN poetry install --no-root

# Set default command
CMD ["bash", "entrypoint.sh"]
