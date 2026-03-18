FROM apache/airflow:2.8.1

# Switch to root to install system-level packages (Java)
USER root

# Install Java 17 and clean up the cache to keep the image small
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user to install Python packages
USER airflow

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0