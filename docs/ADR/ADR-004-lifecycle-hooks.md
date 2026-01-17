# ADR-004: Lifecycle Hooks for Data Enrichment

## Status
Accepted

## Context
Standard ETL often requires more than just simple mapping or validation. Users need to enrich data from external sources (APIs, Caches), perform complex transformations, or manage state across chunks.

## Decision
We implement a lifecycle-based hook system that allows Python code to execute at specific stages of the pipeline.

### Stages
1.  **`pre_validation_hooks`**: Execute before Pydantic validation. Useful for sanitizing raw data or early filtering.
2.  **`execute` (Main Hook)**: The primary transformation logic.
3.  **`post_validation_hooks`**: Execute after validation. Useful for enrichment that depends on valid data structures.
4.  **`setup`/`teardown`**: Framework-level hooks for initializing and cleaning up resources (e.g., database connections, API clients).

### Batch Processing
Hooks receive batches of data, allowing for efficient operations like bulk API calls or vectorized transformations within the hook itself.

## Consequences
-   **Benefit**: High flexibility for complex "Chaotic ETL" workloads.
-   **Benefit**: Separation of concerns between data validation (Schema) and business logic (Hooks).
-   **Benefit**: Batching within hooks allows for performance optimizations.
-   **Drawback**: Heavy logic in hooks will slow down the pipeline as it runs in the Python interpreter.
