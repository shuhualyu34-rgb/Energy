FROM apache/spark:4.2.0-python3@sha256:fc64959c04bd87b0ac686be9aaa9008b69cdb1afc695400528279c8b01f43d89 AS spark_runtime
FROM python:3.11-slim-bookworm@sha256:a83ba18aef0f865b1a4a611c2cf460fe446f2795673bbb9fe43be570439e3b39 AS app_dependencies

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CDP_DATA_DIR=/data

WORKDIR /app
COPY requirements.lock .
RUN --mount=type=secret,id=build_ca \
    if [ -s /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi \
    && pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock \
    && groupadd --gid 10001 cdp \
    && useradd --uid 10001 --gid 10001 --no-create-home cdp \
    && install -d -o cdp -g cdp /data

FROM app_dependencies AS api
COPY cdp ./cdp
COPY samples ./samples
RUN chmod -R a+rX /app/cdp /app/samples
USER cdp
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "cdp.api:app", "--host", "0.0.0.0", "--port", "8000"]

FROM app_dependencies AS spark_base
COPY docker/spark-kafka-jars.lock docker/fetch-spark-jars.py /tmp/
RUN --mount=type=bind,from=spark_runtime,target=/mnt/spark-runtime,ro \
    --mount=type=secret,id=build_ca \
    mkdir -p /opt/java \
    && cp -a /mnt/spark-runtime/opt/java/openjdk /opt/java/openjdk \
    && cp -a /mnt/spark-runtime/opt/spark /opt/spark \
    && python /tmp/fetch-spark-jars.py \
    && rm /tmp/spark-kafka-jars.lock /tmp/fetch-spark-jars.py

FROM spark_base AS spark
COPY --from=api /app /app
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/local/bin/python PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PYTHONPATH=/app SPARK_LOCAL_IP=127.0.0.1
USER cdp
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/app/cdp/spark_job.py"]
CMD ["--once"]

FROM api AS default
