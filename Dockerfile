# Start from the official Airflow image
FROM apache/airflow:3.0.0-python3.9

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Create airflow user and group
RUN groupadd -r airflow && useradd -r -g airflow -u 1000 airflow

# Copy requirements file and install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dags and utils folders
COPY dags $AIRFLOW_HOME/dags
COPY utils $AIRFLOW_HOME/utils

# Set correct permissions for AIRFLOW_HOME
RUN chown -R airflow:airflow $AIRFLOW_HOME

# Switch to airflow user
USER airflow

# Expose port for Airflow webserver
EXPOSE 8080
