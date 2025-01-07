FROM eclipse-temurin:11-jdk-noble
LABEL authors="ahmedhamdy"
WORKDIR /
COPY target/flink-rpc-handson-2.0-SNAPSHOT.jar /app.jar

RUN apt-get update -y &&\
    apt-get install -y iputils-ping

ENTRYPOINT ["java", "-jar", "/app.jar"]
