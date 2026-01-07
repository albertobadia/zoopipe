# RFC: SchemaFlow (The Memory-Aware Micro-ETL)

**Status:** Final Proposal / Implementation  
**Author:** Alberto Daniel Badia  
**Niche:** Data Infrastructure / Data Reliability Engineering

---

## 1. Abstract

**SchemaFlow** is a high-performance **Micro-ETL** engine for Python that solves the fragility of traditional data pipelines. Its core innovation is **proactive byte-based backpressure**, which guarantees RAM stability even when dealing with massive data volumes or unpredictable data sources (corrupted Excel files, legacy databases).

## 2. The Problem: "The Fragile Script Syndrome"

Currently, Python developers face a technical chasm:

1. **Pandas Scripts/Simple Scripts:** Consume RAM linearly until the system throws an *Out of Memory* (OOM) error. They lack atomic error handling (if one row fails, the entire process typically fails).
2. **Big Data Frameworks (Spark/Ray):** Oversized for 90% of enterprise tasks, requiring complex infrastructure (JVM, Clusters) and high operational costs.

## 3. Proposed Architecture

### 3.1 Data Flow

SchemaFlow uses a decoupled architecture based on four components:

* **InputAdapter:** Source-agnostic reading (CSV, SQL, Parquet, API).
* **Orchestrator (Core):** The "brain" that measures the size of data in transit and manages the memory semaphore.
* **Executor/Worker:** Parallel computation that validates with **Pydantic**.
* **OutputAdapter / ErrorAdapter:** Differentiated persistence for successes and errors (DLQ - Dead Letter Queue).

### 3.2 Transport Stack (Optimized IPC)

To cross the process boundary in the `MultiprocessingExecutor`, SchemaFlow replaces Python's slow `Pickle` with a binary tunnel:

1. **Serialization:** Msgpack (lightweight, typed, fast).
2. **Compression:** LZ4 (low latency, high decompression speed).

### 3.3 Byte-Based Backpressure (Core Innovation)

Unlike other systems that count "messages", SchemaFlow measures `len(msgpack_payload)`.

* **Threshold:** The user defines a limit (e.g., 500MB).
* **Action:** If `bytes_in_flight` > `threshold`, the `InputAdapter` blocks. This prevents data from accumulating in the communication bus and saturating the RAM.

---

## 4. Ideal Use Cases (The "Sweet Spot")

* **Legacy Migrations:** Moving data between heterogeneous databases where the network is unstable and integrity is critical.
* **Third-Party Data Ingestion:** Processing manually generated files (Excel/CSV) that contain formatting errors or inconsistent types.
* **Industrial Sanitization:** Acting as a quality filter before loading data into a Data Lake or training an ML model.

---

## 5. Technical Specifications

| Component | Technology |
| --- | --- |
| **Schema Validation** | Pydantic V2 |
| **Internal Serialization** | Msgpack |
| **Compression Algorithm** | LZ4 |
| **Parallelism** | Multiprocessing (Worker Pool) |
| **Supported Formats** | Parquet, CSV, SQL, JSON (via Adapters) |

---

## 6. Strategic Comparison

* **Vs Celery:** SchemaFlow is *data-aware*. It knows how much data weighs and what's inside it. It doesn't require an external Broker (Redis/RabbitMQ).
* **Vs Spark:** SchemaFlow is *Python-native* and *lightweight*. Ideal for processing on a single high-capacity server.
* **Vs Pandas:** SchemaFlow is *resilient*. It doesn't need to load the entire dataset into memory and doesn't crash on a corrupted row.

---

## 7. Conclusion

**SchemaFlow** is not just a library; it's a **Defensive Data Programming** methodology. By moving validation and memory control to the pipeline infrastructure, we allow developers to focus on business logic, knowing that the transport system is indestructible.

---

### What's Next for the Project?

Would you like me to write a **"Quick Start Guide"** (Quickstart) for the GitHub repository based on this RFC? It would be the example code that any developer would copy and paste to see SchemaFlow's magic working in 5 minutes.
