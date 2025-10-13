# Use an official Python image with OpenJDK 17
FROM python:3.12-slim-bullseye

# Set the working directory
WORKDIR /app

# Install OpenJDK 17, procps, curl, and other utilities
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . /app/

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Expose the necessary port
EXPOSE 8080

# Command to run the application
CMD ["python", "app.py"]
