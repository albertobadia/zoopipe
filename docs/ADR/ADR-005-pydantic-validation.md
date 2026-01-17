# ADR-005: Pydantic-based Declarative Validation

## Status
Accepted

## Context
Ensuring data integrity is a core requirement for any data engineering tool. Manual validation is error-prone and tedious.

## Decision
We adopt **Pydantic** as the standard for declarative data validation and schema definition in ZooPipe.

### Strategy
-   Users define a `BaseModel` representing the expected record structure.
-   The `Pipe` automatically validates each record in the batch against this model.
-   `model_config = ConfigDict(extra="ignore")` is typically encouraged to allow for lean schemas while handling broad inputs.
-   Validated data is coerced into specific types, ensuring downstream consumers receive clean data.

## Consequences
-   **Benefit**: Leverages a industry-standard, well-documented, and high-performance validation library.
-   **Benefit**: Clear, readable schemas that serve as self-documentation.
-   **Benefit**: Automatic type coercion and error reporting.
-   **Drawback**: Pydantic validation adds overhead compared to raw dictionary access.
-   **Drawback**: Dependency on Pydantic's versioning and API changes.
