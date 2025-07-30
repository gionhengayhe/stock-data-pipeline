FROM bitnami/spark:3.5.6

USER root
RUN install_packages curl

USER 1001

RUN rm -rf /opt/bitnami/spark/jars && \
    curl -L https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz | \
    tar -xz --strip=1 -C /opt/bitnami/spark spark-3.5.6-bin-hadoop3/jars
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
    -o /opt/bitnami/spark/jars/hadoop-common-3.3.4.jar
