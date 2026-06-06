FROM python:3.13-slim

# Install Java (required for PySpark) and build tools (required for pandas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline
COPY src/main.py .

# Data and output directories
VOLUME ["/data", "/app/output"]

ENV DATA_DIR=/data
ENV OUTPUT_DIR=/app/output

CMD ["python", "main.py"]
