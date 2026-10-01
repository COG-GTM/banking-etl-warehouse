# syntax=docker/dockerfile:1.6
# Spark application image for EKS (Spark Operator / spark-submit on Kubernetes).
# Build from the repository root so the sample landing files can be baked in:
#   docker buildx build --platform linux/amd64 -f pyspark/Dockerfile -t <ecr-repo>/banking-etl:<tag> .
ARG SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM --platform=linux/amd64 ${SPARK_IMAGE}

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG DELTA_VERSION=3.2.1
ARG MSSQL_JDBC_VERSION=12.8.1.jre11
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

USER root

# Delta Lake, SQL Server JDBC and S3A (hadoop-aws matches the bundled Hadoop 3.3.4 client).
RUN set -eux; \
    cd /opt/spark/jars; \
    for artifact in \
        "io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
        "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
        "com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar" \
        "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    do \
        curl -fsSL --retry 5 --retry-delay 3 -O "${MAVEN_REPO}/${artifact}"; \
    done; \
    chmod 644 /opt/spark/jars/*.jar

ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /opt/banking-etl

COPY pyspark/requirements.txt ./requirements.txt
# Ubuntu's distro setuptools ignores PEP 621 metadata; install current build tooling first.
RUN pip3 install --no-cache-dir pip==24.2 setuptools==75.1.0 wheel==0.44.0 \
    && pip3 install --no-cache-dir -r requirements.txt

COPY pyspark/pyproject.toml pyspark/README.md ./
COPY pyspark/src ./src
RUN pip3 install --no-cache-dir --no-deps --no-build-isolation . \
    && rm -rf build src/*.egg-info \
    && python3 -c "import banking_etl.jobs.load_fact_transaction"

COPY pyspark/config ./config
COPY data_sources/transaction_excel.xlsx data_sources/transaction_csv.csv ./data_sources/

ENV PYSPARK_PYTHON=python3 \
    PYARROW_IGNORE_TIMEZONE=1 \
    BANKING_ETL_CONFIG=/opt/banking-etl/config/config.yaml \
    BANKING_ETL_RESOLVE_PACKAGES=false \
    BANKING_ETL_JOBS=/opt/banking-etl/jobs \
    TRANSACTION_EXCEL_PATH=/opt/banking-etl/data_sources/transaction_excel.xlsx \
    TRANSACTION_CSV_PATH=/opt/banking-etl/data_sources/transaction_csv.csv \
    DWH_BASE_PATH=/opt/banking-etl/warehouse/dwh

# Thin spark-submit entry scripts: local:///opt/banking-etl/jobs/<job>.py
RUN set -eux; mkdir -p jobs warehouse; \
    for job in load_dim_branch load_dim_account load_dim_customer load_fact_transaction \
               daily_transaction balance_per_customer; do \
        printf 'from banking_etl.jobs.%s import main\n\nif __name__ == "__main__":\n    main()\n' "$job" > "jobs/${job}.py"; \
    done; \
    chown -R spark:spark /opt/banking-etl

USER spark
# Inherit /opt/entrypoint.sh from the Spark image (driver/executor modes for Spark on K8s).
