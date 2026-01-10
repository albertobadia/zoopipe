import os
import random

import duckdb


def generate_duckdb_data(path: str, count: int = 100000):
    if os.path.exists(path):
        print(f"File {path} already exists. Skipping generation.")
        return

    print(f"Generating {count} rows in {path}...")
    conn = duckdb.connect(path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER,
            balance DOUBLE
        )
    """)

    names = [
        "Alice",
        "Bob",
        "Charlie",
        "Dave",
        "Eve",
        "Frank",
        "Grace",
        "Heidi",
        "Ivan",
        "Judy",
    ]

    # We can use executemany for efficiency
    rows = []
    for i in range(1, count + 1):
        name = random.choice(names)
        rows.append(
            (
                i,
                f"{name}_{i}",
                f"{name.lower()}.{i}@example.com",
                random.randint(18, 80),
                round(random.uniform(0, 10000), 2),
            )
        )

        if len(rows) >= 10000:
            conn.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", rows)
            rows = []

    if rows:
        conn.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", rows)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    db_dir = "examples/data"
    os.makedirs(db_dir, exist_ok=True)
    generate_duckdb_data(os.path.join(db_dir, "sample_data.duckdb"))
