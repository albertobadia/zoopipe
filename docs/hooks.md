# ZooPipe Hooks Guide

> **Why Hooks? (vs `df.apply`)**
> In vectorized libraries like Pandas/Polars, using `.apply()` forces serialization overhead per row. **ZooPipe Hooks** execute inside a Rust-managed parallel thread pool, allowing complex Python logic (API calls, DB queries) to run in efficient parallel streams without the "DataFrame tax".

Hooks are a powerful feature in ZooPipe that allow you to inject custom logic into the pipeline's lifecycle. They enable you to transform data, manage resources, and perform complex enrichments without modifying the core adapter logic.

## The BaseHook Class

All hooks must inherit from `BaseHook`.

```python
from zoopipe import BaseHook, EntryTypedDict, HookStore

class MyHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        pass

    def execute(self, entries: list[EntryTypedDict], store: HookStore) -> list[EntryTypedDict]:
        return entries

    def teardown(self, store: HookStore) -> None:
        pass
```

## Lifecycle Phases

### 1. Setup (`setup`)
- **When**: Called exactly **ONCE** (globally) before the pipeline begins processing any data.
- **Context**: Even with `MultiThreadExecutor`, `setup` runs on the main thread.
- **Purpose**: Initialize **global** resources or configuration.
- **Store**: The `store` passed here is the **global** pipeline store. It is NOT passed to `execute`.

### 2. Execute (`execute`)
- **When**: Called for every batch of data that passes through the pipeline.
- **Purpose**: Transform, validate, or enrich the data.
- **Store**: The `store` passed here is **fresh and empty** for each batch. It is isolated from other batches and threads. Use it for temporary state within the hook chain for that specific batch.
- **Thread Safety**: Since `store` is local to the batch/thread, it is safe to write to. However, do NOT rely on `setup` store values here.

### 3. Teardown (`teardown`)
- **When**: Called once after all data has been processed (or if the pipeline crashes).
- **Purpose**: Clean up **global** resources.
- **Store**: Receives the **global** pipeline store (same as `setup`).

---

## Example 1: Simple Enrichment Hook

This hook adds a processing timestamp to every record.

```python
import time
from datetime import datetime
from zoopipe import BaseHook, EntryTypedDict, HookStore

class TimestampHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        # Store the pipeline start time
        store["start_time"] = time.time()
        print(f"Pipeline started at: {datetime.now()}")

    def execute(self, entries: list[EntryTypedDict], store: HookStore) -> list[EntryTypedDict]:
        current_time = datetime.now().isoformat()
        
        for entry in entries:
            # Modify the raw data dictionary in-place
            entry["raw_data"]["processed_at"] = current_time
            
        return entries

    def teardown(self, store: HookStore) -> None:
        duration = time.time() - store["start_time"]
        print(f"Pipeline finished in {duration:.2f} seconds")
```

---

## Example 2: Advanced SQL Expansion (Pagination Pattern)

This example demonstrates how the `SQLExpansionHook` works conceptually. It is used in cursor pagination to "hydrate" full records from a batch of IDs.

**Scenario**: You have a list of user IDs, and you need to fetch their full profiles from a database.

```python
import sqlite3
from zoopipe import BaseHook, EntryTypedDict, HookStore

class UserHydrationHook(BaseHook):
    def __init__(self, db_path: str):
        super().__init__()
        self.db_path = db_path

    def setup(self, store: HookStore) -> None:
        # In this pattern, setup doesn't open connections because we want
        # each batch to be self-contained and thread-safe.
        pass

    def execute(self, entries: list[EntryTypedDict], store: HookStore) -> list[EntryTypedDict]:
        # 2. Open connection locally per batch (or use a thread-safe pool)
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            
            # 3. Extract IDs from the incoming batch
            user_ids = [entry["raw_data"]["id"] for entry in entries]
            
            if not user_ids:
                return entries

            # 4. Perform a bulk fetch
            placeholders = ",".join("?" * len(user_ids))
            query = f"SELECT id, email, status FROM users WHERE id IN ({placeholders})"
            
            cursor.execute(query, user_ids)
            results = {row[0]: row for row in cursor.fetchall()}

            # 5. Merge data back into the entries
            for entry in entries:
                uid = entry["raw_data"]["id"]
                if uid in results:
                    row = results[uid]
                    entry["raw_data"]["email"] = row[1]
                    entry["raw_data"]["status"] = row[2]
                else:
                    entry["raw_data"]["error"] = "User not found"
            
            cursor.close()
        finally:
            # 6. Ensure connection is closed even if errors occur
            conn.close()

        return entries

    def teardown(self, store: HookStore) -> None:
        pass
```

---

## Best Practices

1.  **Thread Safety**: The `store` passed to `execute` is unique per batch and safe to modify. However, if you access **global** resources initialized in `setup` (like a shared API client), you MUST ensure they are thread-safe or use locks.
2.  **Performance vs Safety**: Creating resources (like DB connections) inside `execute` ensures safety but adds overhead. Use connection pooling (initialized in `setup`) for the best of both worlds.
3.  **Error Handling**: If an exception is raised in `execute`, that batch may fail. Handle expected errors (like missing keys) gracefully if you want the pipeline to continue.
4.  **Priorities**: You can set `priority` in `__init__` to control the order in which hooks run (lower numbers run earlier).

```python
class FirstHook(BaseHook):
    def __init__(self):
        super().__init__(priority=0)

class LastHook(BaseHook):
    def __init__(self):
        super().__init__(priority=100)
```
