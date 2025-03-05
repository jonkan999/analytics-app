# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy Python source code
COPY python/ ./python/

# Create an empty keys directory (for local development compatibility)
RUN mkdir -p ./python/keys

# Set the entry point
CMD python python/trending_races.py && python python/analytics_processor.py