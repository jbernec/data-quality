### Collection of pyspark data-quality unit tests using pytest in a windows local environment.

#### 0. Ensure that python is installed. Preferebly not the latest version. Make sure the python path is set.
#### 1. Install pyspark and pytest dependencies defined in the requirements.txt file, using the pip installer.
#### 2. Download and install Java from the https://www.oracle.com/java/technologies/downloads/#jdk21-windows url. Preferably the version before the latest.
#### 3. Clone the winutils repo from https://github.com/cdarlint/winutils.
#### 4. Set the spark home environment variable - [System.Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark\spark-3.5.0-bin-hadoop3", [System.EnvironmentVariableTarget]::Machine). Verify using the following command - echo $env:SPARK_HOME
#### 5. Set the java home environment variable - [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-21", [System.EnvironmentVariableTarget]::Machine)
#### 6. Mkdir C:\Hadoop-3.3.5 .
#### 7. Copy the bin folder containing the winutils.exe file from the cloned hadoop-3.3.5 or latest version to the C:\Hadoop-3.3.5 folder.
#### 8. Set hadoop environment variable - Set the hadoop home environment variable - [System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop-3.3.5", [System.EnvironmentVariableTarget]::Machine) .
#### 9. Set the PYTHONPATH env variable to "%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.9.7-src.zip;%PYTHONPATH%" .
#### 10. Update the Path variable with the following: "%HADOOP_HOME%\bin", "%SPARK_HOME%\bin", "%JAVA_HOME%\bin" .


It might be necessary to deploy these unit tests and package dependencies in a docker container. A Dockerfile is included the root of the pytest_pyspark project. Instructions on how to deploy are included below:


This Dockerfile does the following:
1. Uses the official Python 3.8 slim image.
2. Sets the necessary environment variables.
3. Installs Java.
4. Downloads and installs Spark.
5. Downloads and installs Hadoop and winutils.
6. Copies the current directory into the container.
7. Installs the Python packages listed in `requirements.txt`.
8. Exposes port 80.
9. Runs `pytest` when the container launches.

To build and run the Docker container, use the following commands:

```sh
docker build -t pytest_pyspark .
docker run -it pytest_pyspark
```


