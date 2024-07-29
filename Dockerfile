# Use an official Python runtime as a parent image
FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    && tar -xvf spark-3.3.2-bin-hadoop3.tgz -C /opt/ \
    && rm spark-3.3.2-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark-3.3.2-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY process_reviews.py .
COPY data /app/data
COPY schemas /app/schemas

# Define environment variable
ENV NAME=World

# Set the entry point for the container
ENTRYPOINT ["python3", "process_reviews.py"]