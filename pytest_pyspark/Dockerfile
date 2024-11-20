# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean;

# Install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar xvz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

# Install Hadoop and winutils
RUN wget -qO- https://github.com/cdarlint/winutils/archive/refs/heads/master.zip | unzip -d /opt/ - && \
    mv /opt/winutils-master/hadoop-3.3.5 /opt/hadoop && \
    mkdir -p /opt/hadoop/bin && \
    cp /opt/hadoop/bin/winutils.exe /opt/hadoop/bin/

# Copy the current directory contents into the container at /app
WORKDIR /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Run pytest when the container launches
CMD ["pytest"]