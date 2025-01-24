FROM spark:3.5.4-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip sudo; \
    apt install -y git; \
    rm -rf /var/lib/apt/lists/*

#USER spark

# Update and install dependencies required for Homebrew
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    file \
    unzip \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Azure CLI and Homebrew
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

RUN bash -c "$(curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh)" || true 

# Add databricks to PATH
ENV PATH="/usr/local/bin:$PATH"

# Install pytest
RUN pip3 install --no-cache-dir pytest pyspark

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PATH=$PATH:/opt/spark/bin

# OS Patching
RUN apt update  && \
    apt upgrade -y && \
 	rm -rf /var/lib/apt/lists/*


WORKDIR /apps/unittests
RUN mkdir -p results

ENV TEST_OUTPUT_DIR=/apps/unittests/results

COPY . /apps/unittests

# Expose ports if needed
EXPOSE 8080 4040

# Run a specific pytest test function when the container starts
CMD ["pytest", "-v", "--junitxml=${TEST_OUTPUT_DIR}/TEST-ingestion-junit-results.xml"]
