FROM python:3.12-slim-bullseye

# Set working directory
WORKDIR /app

# Install Java 17 + dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Detect and set JAVA_HOME dynamically during build
RUN JAVA_PATH=$(readlink -f /usr/bin/java) && \
    export DETECTED_JAVA_HOME=$(dirname $(dirname $JAVA_PATH)) && \
    echo "Detected JAVA_HOME: $DETECTED_JAVA_HOME" && \
    echo "JAVA_HOME=$DETECTED_JAVA_HOME" > /etc/java_home.env

# Source the java_home during runtime
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Verify Java installation
RUN java -version && echo "Java installed at: $(readlink -f /usr/bin/java)"

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of app
COPY . .

# Expose app port
EXPOSE 8080

# Default command
CMD ["python", "app.py"]
