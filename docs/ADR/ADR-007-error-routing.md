# ADR-007: Automated Error Routing

## Status
Accepted

## Context
In large datasets, "poison pills" (malformed records) are inevitable. Stopping the entire pipeline for a single invalid record is often unacceptable for production workloads.

## Decision
ZooPipe provides native support for **Automated Error Routing**.

### Implementation
-   The `Pipe` object can be configured with an `error_output_adapter`.
-   If a record fails Pydantic validation or throws an exception during a Hook, it is tagged as an error.
-   Successful records proceed to the standard `output_adapter`.
-   Failed records (along with error metadata, if possible) are routed to the `error_output_adapter`.
-   The main pipeline continues processing the remaining records in the batch.

## Consequences
-   **Benefit**: Ensures high pipeline availability and resilience.
-   **Benefit**: Simplifies error debugging by isolating failed records in a dedicated location (e.g., an `errors.csv`).
-   **Benefit**: No manual try-except blocks required for standard validation failures.
-   **Drawback**: Requires configuring a separate output stream for errors.
-   **Drawback**: May hide underlying issues if errors are not monitored and addressed.
