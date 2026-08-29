FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN SPARK_JARS="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / "jars")')" && \
    curl -fL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o "${SPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar" && \
    curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o "${SPARK_JARS}/hadoop-aws-3.3.4.jar"
