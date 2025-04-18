# First-time build can take upto 10 mins.

FROM apache/airflow:2.3.0

ENV AIRFLOW_HOME=/opt/airflow

# Install required packages
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Include PostgreSQL dependencies
RUN pip install --no-cache-dir 'apache-airflow[postgres]' psycopg2-binary

# Copy files into the image
WORKDIR $AIRFLOW_HOME

USER root
COPY scripts scripts
COPY data data
RUN chmod -R +x scripts data

USER $AIRFLOW_UID