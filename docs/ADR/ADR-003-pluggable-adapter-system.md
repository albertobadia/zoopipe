# ADR-003: Pluggable Adapter System

## Status
Accepted

## Context
ZooPipe needs to support a wide variety of data sources and destinations (CSV, JSON, SQL, S3, Kafka, etc.). Hardcoding these into the core pipeline would make the project difficult to maintain and extend.

## Decision
We define a pluggable adapter system based on standard protocols.

### Design
-   **InputAdapter**: Must implement a method to fetch chunks of data and keep track of the current cursor/offset.
-   **OutputAdapter**: Must implement a method to write chunks of data.
-   **Protocol Definition**: In Python, we use `Protocols` (duck typing) or base classes to define the expected interface.

### Native vs. Python Adapters
-   **Native Adapters (Rust)**: High-performance adapters for common formats (CSV, JSONL, SQL) implemented in Rust for speed.
-   **Python Adapters**: Flexible adapters written in pure Python for less performance-critical or highly custom integrations (e.g., custom API clients).

## Consequences
-   **Benefit**: Easy to add support for new data formats without modifying the core engine.
-   **Benefit**: Users can implement their own custom adapters in Python.
-   **Benefit**: Decouples the storage format from the processing logic.
-   **Drawback**: Requires a stable and well-defined protocol to ensure compatibility between Python and Rust components.
