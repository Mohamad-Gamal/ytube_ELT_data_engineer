# ---------- Build Arguments ----------
ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.10

# ---------- Base Image ----------
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare args so they're available during build
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# ---------- Environment Setup ----------
ENV AIRFLOW_HOME=/opt/airflow

# ---------- Copy Requirements ----------
COPY requirements.txt /

# ---------- Install Dependencies ----------
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# ---------- Working Directory ----------
WORKDIR ${AIRFLOW_HOME}

# ---------- Expose Airflow Webserver Port ----------
EXPOSE 8080

# ---------- Default Command ----------
CMD ["airflow", "version"]
