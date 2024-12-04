# Use a base image with Java (Spark requires Java)
FROM openjdk:11-jre-slim

WORKDIR /wytasoft_training_academy

RUN mkdir -p /tmp/spark-events

# Install Spark dependencies
RUN apt-get update && apt-get install -y wget tar

# Download and install Apache Spark
ENV SPARK_VERSION=3.4.1
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy the Spark application JAR file to the container
COPY ./target/wtskayansparkall-1.0.0.jar /wytasoft_training_academy/my-spark-app.jar

EXPOSE 4040 8080
# Set the default command to run the Spark application
CMD ["spark-submit", "--class", "com.wts.kayan.app.job.MainDriver", "--master", "local[*]", "/wytasoft_training_academy/my-spark-app.jar", "prd"]
