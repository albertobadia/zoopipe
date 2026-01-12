import csv
import os
import time
from typing import Any, Callable, Dict, List

from faker import Faker


def generate_csv_with_faker(
    csv_path: str,
    data_generator_callback: Callable[[Faker], Dict[str, Any]],
    batch_size: int = 1000,
    target_size_gb: float = 10.0,
    append: bool = False,
    progress_interval_seconds: int = 10,
):
    fake = Faker()
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024

    mode = "a" if append and os.path.exists(csv_path) else "w"
    current_size = os.path.getsize(csv_path) if os.path.exists(csv_path) else 0

    first_batch = True
    total_rows = 0
    last_print_time = time.time()

    with open(csv_path, mode, newline="", encoding="utf-8") as csvfile:
        writer = None

        while current_size < target_size_bytes:
            batch_data: List[Dict[str, Any]] = []

            for _ in range(batch_size):
                row_data = data_generator_callback(fake)
                batch_data.append(row_data)

            if writer is None:
                fieldnames = list(batch_data[0].keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if first_batch and mode == "w":
                    writer.writeheader()

            writer.writerows(batch_data)
            csvfile.flush()

            total_rows += batch_size
            current_size = os.path.getsize(csv_path)

            current_time = time.time()
            if current_time - last_print_time >= progress_interval_seconds:
                current_size_mb = current_size / (1024 * 1024)
                target_size_mb = target_size_gb * 1024
                progress_percent = (current_size / target_size_bytes) * 100
                print(
                    f"Generated {total_rows} rows | File size: {current_size_mb:.2f} "
                    f"MB / {target_size_mb:.2f} MB ({progress_percent:.1f}%)"
                )
                last_print_time = current_time

            first_batch = False

    final_size_mb = current_size / (1024 * 1024)
    print("\nGeneration complete!")
    print(f"Total rows: {total_rows}")
    print(f"Final file size: {final_size_mb:.2f} MB")


def rich_user_generator(fake: Faker) -> Dict[str, Any]:
    return {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "email": fake.email(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "phone_number": fake.phone_number(),
        "address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state(),
        "country": fake.country(),
        "postal_code": fake.postcode(),
        "company": fake.company(),
        "job_title": fake.job(),
        "salary": fake.random_int(min=30000, max=200000),
        "credit_card_number": fake.credit_card_number(),
        "credit_card_provider": fake.credit_card_provider(),
        "iban": fake.iban(),
        "registration_date": fake.date_time_between(
            start_date="-5y", end_date="now"
        ).isoformat(),
        "last_login": fake.date_time_between(
            start_date="-30d", end_date="now"
        ).isoformat(),
        "is_active": fake.boolean(chance_of_getting_true=85),
        "profile_picture_url": fake.image_url(),
        "bio": fake.text(max_nb_chars=200),
        "website": fake.url(),
        "timezone": fake.timezone(),
        "language": fake.language_code(),
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
    }


if __name__ == "__main__":
    generate_csv_with_faker(
        csv_path="users_data.csv",
        data_generator_callback=rich_user_generator,
        batch_size=1000,
        target_size_gb=10,
    )
