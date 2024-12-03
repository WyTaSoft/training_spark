# Use a base image with Java
FROM openjdk:11-slim

# Set environment variables for Spark, Hadoop, and Scala versions
ENV SCALA_VERSION=2.12.15 \
    SPARK_VERSION=3.3.1 \
    HADOOP_VERSION=3.3.4

# Install required dependencies (Scala, Spark, Maven, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl bash procps maven && \
    wget https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz && \
    tar -xzf scala-${SCALA_VERSION}.tgz -C /usr/local && \
    ln -s /usr/local/scala-${SCALA_VERSION} /usr/local/scala && \
    ln -s /usr/local/scala/bin/* /usr/local/bin/ && \
    rm scala-${SCALA_VERSION}.tgz && \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local && \
    ln -s /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /usr/local/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables for Spark and Hadoop
ENV SPARK_HOME=/usr/local/spark \
    PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH \
    JAVA_HOME=/usr/local/openjdk-11

# Create application directory
WORKDIR /app

# Copy the Maven project (POM and source code)
COPY ./pom.xml /app/pom.xml
COPY ./src /app/src

# Build the application using Maven
RUN mvn clean package -DskipTests

# Copy the built JAR file into the container
RUN cp /app/target/*.jar /app/app.jar

# Define the entry point for the Spark application
ENTRYPOINT ["spark-submit", "--class", "com.wts.kayan.app.job.MainDriver", "--master", "local[*]", "/app/app.jar"]
