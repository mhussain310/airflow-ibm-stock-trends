FROM apache/airflow:3.0.0

# Add /opt to Python module search path
ENV PYTHONPATH=/opt:${PYTHONPATH}

# Set workdir to /opt to copy general project files
WORKDIR /opt

COPY config ./config
COPY etl ./etl
COPY utils ./utils
COPY README.md .
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Switch to airflow working directory
WORKDIR /opt/airflow