FROM spark:3.5.4-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

USER spark

RUN brew tap databricks/tap; \
    brew install databricks; \
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Copy the current directory contents into the container at /app
WORKDIR /app
COPY . /app

# Install pytest
RUN pip3 install -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Run a specific pytest test function when the container starts
CMD [f'pytest test_dq.py -v --junitxml={self.test_results_dir}/{self.junit_report}']
