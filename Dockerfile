FROM apache/spark:3.5.9

USER root
COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    uv python install 3.12 && \
    ln -s "$(uv python find 3.12)" /usr/local/bin/python3.12 && \
    /usr/local/bin/python3.12 -c "import ctypes, ssl" && \
    curl -fL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar

ENV PYSPARK_PYTHON=/usr/local/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

USER 185
