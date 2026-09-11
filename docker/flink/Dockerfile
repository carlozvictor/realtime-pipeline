# Stage 1: compila o job de bootstrap (Application Mode) que submete o
# pipeline SQL versionado em processing/sql/.
FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /build
COPY processing/jobs/pipeline-job/pom.xml processing/jobs/pipeline-job/pom.xml
COPY processing/jobs/pipeline-job/src processing/jobs/pipeline-job/src
COPY processing/sql processing/sql

WORKDIR /build/processing/jobs/pipeline-job
RUN mvn -q -B package -DskipTests

# Stage 2: runtime do Flink com os conectores necessarios e o job pronto
# em usrlib/, para rodar em Application Mode (standalone-job).
FROM flink:1.20

RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar

COPY --from=build /build/processing/jobs/pipeline-job/target/pipeline-job.jar /opt/flink/usrlib/pipeline-job.jar
