FROM eclipse-temurin:11-jdk-noble
#RUN apt-get update -y &&\
#    apt-get install -y iputils-ping


ENV DEBUG_PORT 5005

WORKDIR /
COPY target/flink-resourcemanager-handson-2.0-SNAPSHOT.jar app.jar

#ENTRYPOINT ["sh","-c", "java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:$DEBUG_PORT -jar /app.jar"]

ENTRYPOINT ["java","-jar","/app.jar"]
