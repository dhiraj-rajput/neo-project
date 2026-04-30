FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set working directory
WORKDIR /app

# Install Java 17 (LTS) + dependencies for Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-java

# Set JAVA_HOME via standard Debian path
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Verify installations during build
RUN echo "Build-time verification:" && \
    java -version && \
    python --version && \
    uv --version

# Copy requirements and install Python dependencies using uv
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy rest of application
COPY . .

# Default command (overridden by docker-compose)
CMD ["tail", "-f", "/dev/null"]
