# High-Level System Architecture Suggestions

This document provides suggestions for high-level system architectures for the NEO data analysis project. Each suggestion includes a diagram, a description of the components, and a discussion of the pros and cons.

## 1. Current Architecture (Lambda Architecture)

The current architecture is a good example of a Lambda architecture. It has separate paths for batch and real-time processing.

**Diagram:**

```
+-----------------+      +-----------------+      +-----------------+
|   NASA NeoWs    |----->|   fetch_raw.py  |----->|      Kafka      |
+-----------------+      +-----------------+      +-----------------+
                                                     |
                                                     | (real-time)
                                                     v
+-----------------+      +-----------------+      +-----------------+
| spark_processor.py |----->|   TimescaleDB   |<-----| calculate_metrics_data.py |
+-----------------+      +-----------------+      +-----------------+
       |                                              ^ (batch)
       |                                              |
       +----------------------------------------------+
```

**Components:**

*   **Data Ingestion:** `fetch_raw.py` fetches data from the NASA NeoWs API and sends it to Kafka.
*   **Real-time Path:** `spark_processor.py` consumes data from Kafka in real time, processes it, and stores it in TimescaleDB.
*   **Batch Path:** `calculate_metrics_data.py` periodically runs a batch job to calculate aggregated metrics from the data in TimescaleDB.
*   **Data Storage:** TimescaleDB stores both the raw and processed data.
*   **Visualization:** Grafana and Streamlit are used to visualize the data.

**Pros:**

*   **Robust and Fault-Tolerant:** The Lambda architecture is very robust and fault-tolerant. If the real-time path fails, the batch path can still process the data.
*   **Flexibility:** The separate paths for real-time and batch processing provide a lot of flexibility.

**Cons:**

*   **Complexity:** The Lambda architecture can be complex to set up and maintain.
*   **Code Duplication:** The logic for processing the data may need to be duplicated in both the real-time and batch paths.

## 2. Kappa Architecture

The Kappa architecture is a simplification of the Lambda architecture. It uses a single stream processing engine for both real-time and batch processing.

**Diagram:**

```
+-----------------+      +-----------------+      +-----------------+
|   NASA NeoWs    |----->|   fetch_raw.py  |----->|      Kafka      |
+-----------------+      +-----------------+      +-----------------+
                                                     |
                                                     v
+-----------------+      +-----------------+
| spark_processor.py |----->|   TimescaleDB   |
+-----------------+      +-----------------+
```

**Components:**

*   **Data Ingestion:** `fetch_raw.py` fetches data from the NASA NeoWs API and sends it to Kafka.
*   **Stream Processing:** `spark_processor.py` consumes data from Kafka, processes it, and stores it in TimescaleDB. All processing, including the calculation of aggregated metrics, is done in the stream processor.
*   **Data Storage:** TimescaleDB stores the processed data.
*   **Visualization:** Grafana and Streamlit are used to visualize the data.

**Pros:**

*   **Simplicity:** The Kappa architecture is much simpler than the Lambda architecture.
*   **No Code Duplication:** The processing logic is all in one place.

**Cons:**

*   **Less Mature:** The Kappa architecture is a newer and less mature architecture than the Lambda architecture.
*   **Limited Tooling:** There is limited tooling available for the Kappa architecture.

## 3. Microservices Architecture

A microservices architecture is an architectural style that structures an application as a collection of loosely coupled services.

**Diagram:**

```
+-----------------+      +-----------------+
|   NASA NeoWs    |----->| Ingestion Service |
+-----------------+      +-----------------+
                             |
                             v
+-----------------+
|  Processing Service |
+-----------------+
                             |
                             v
+-----------------+
|   Query Service   |
+-----------------+
```

**Components:**

*   **Ingestion Service:** This service is responsible for fetching data from the NASA NeoWs API and publishing it to a message queue.
*   **Processing Service:** This service consumes data from the message queue, processes it, and stores it in a database.
*   **Query Service:** This service provides an API for querying the processed data.
*   **Visualization:** A separate front-end application would consume the data from the Query Service and visualize it.

**Pros:**

*   **Scalability:** Each service can be scaled independently.
*   **Flexibility:** Each service can be developed and deployed independently.
*   **Technology Diversity:** Each service can be written in a different programming language.

**Cons:**

*   **Complexity:** A microservices architecture can be complex to set up and maintain.
*   **Distributed System Challenges:** A microservices architecture introduces a number of distributed system challenges, such as service discovery, load balancing, and fault tolerance.

## 4. Serverless Architecture

A serverless architecture is a cloud computing execution model in which the cloud provider runs the server, and dynamically manages the allocation of machine resources.

**Diagram:**

```
+-----------------+      +-----------------+
|   NASA NeoWs    |----->|   AWS Lambda    |
+-----------------+      +-----------------+
                             |
                             v
+-----------------+
|   AWS Lambda    |
+-----------------+
                             |
                             v
+-----------------+
| Amazon Timestream |
+-----------------+
```

**Components:**

*   **Data Ingestion:** An AWS Lambda function is triggered on a schedule to fetch data from the NASA NeoWs API and publish it to a message queue (e.g., Amazon SQS).
*   **Processing:** Another AWS Lambda function is triggered by the message queue to process the data and store it in a database.
*   **Data Storage:** Amazon Timestream is a serverless time-series database.
*   **Visualization:** A separate front-end application would consume the data from the database and visualize it.

**Pros:**

*   **Cost-Effective:** You only pay for the resources you use.
*   **Scalability:** The architecture can automatically scale to handle any amount of traffic.
*   **No Server Management:** The cloud provider manages the servers for you.

**Cons:**

*   **Vendor Lock-in:** A serverless architecture can lead to vendor lock-in.
*   **Cold Starts:** There can be a delay when a Lambda function is invoked for the first time.
*   **Limited Execution Time:** Lambda functions have a limited execution time.
