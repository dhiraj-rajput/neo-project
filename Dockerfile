FROM python:3.12-slim-bullseye

# Set working directory
WORKDIR /app

# Install Java 17 + dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Dynamically detect and set JAVA_HOME at runtime
# Create a dynamic environment setup script
RUN echo '#!/bin/bash' > /usr/local/bin/dynamic-entrypoint.sh && \
    echo '' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo '# Dynamically detect JAVA_HOME' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo '' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo '# Print environment info for debugging' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "========================================="' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "Container Environment Information"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "========================================="' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "Architecture: $(uname -m)"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "OS: $(uname -s)"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "JAVA_HOME: $JAVA_HOME"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "Java Version: $(java -version 2>&1 | head -n 1)"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "Python Version: $(python --version)"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'echo "========================================="' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo '' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo '# Execute the command passed to the container' >> /usr/local/bin/dynamic-entrypoint.sh && \
    echo 'exec "$@"' >> /usr/local/bin/dynamic-entrypoint.sh && \
    chmod +x /usr/local/bin/dynamic-entrypoint.sh

# Verify installations during build
RUN echo "Build-time verification:" && \
    echo "Architecture: $(uname -m)" && \
    echo "OS: $(uname -s)" && \
    java -version && \
    python --version

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy rest of application
COPY . .

# Use dynamic entrypoint that sets JAVA_HOME at runtime
ENTRYPOINT ["/usr/local/bin/dynamic-entrypoint.sh"]

# Default command
CMD ["python", "app.py"]