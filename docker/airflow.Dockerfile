ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
WORKDIR /opt/airflow/project
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src
# Pinning apache-airflow keeps pip from up/downgrading the base installation.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" ".[spark]" "pytest>=8"

# Resolve the Delta Lake jars at build time so tasks never depend on Maven at runtime.
RUN python -c "from claims_pipeline.spark import build_spark; build_spark('warmup').stop()"

ENV LAKEHOUSE_DIR=/opt/airflow/lakehouse
RUN mkdir -p "$LAKEHOUSE_DIR"
COPY --chown=airflow:root dags /opt/airflow/dags
WORKDIR /opt/airflow
