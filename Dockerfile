FROM python:3.12-slim-bullseye

WORKDIR /app

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

EXPOSE 8080

CMD ["python", "app.py"]
