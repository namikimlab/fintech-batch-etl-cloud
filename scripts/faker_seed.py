"""
This script generates fake transaction data for a specified number of days and saves it to CSV files.
The generated data includes duplicates and late-arriving data to simulate real-world scenarios.
"""

import argparse
import os
import random
import csv
import uuid
from datetime import datetime, timedelta
from faker import Faker
from typing import List, Dict, Any, Optional

def generate_transaction(
    fake: Faker,
    transaction_timestamp: datetime,
    customer_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generates a single transaction record.

    Args:
        fake: An instance of the Faker class.
        transaction_timestamp: The timestamp for the transaction.
        customer_id: The ID of the customer.
        merchant_id: The ID of the merchant.

    Returns:
        A dictionary representing a single transaction.
    """
    if customer_id is None:
        customer_id = str(uuid.uuid4())
    if merchant_id is None:
        merchant_id = str(uuid.uuid4())

    amount = round(random.uniform(1.0, 300.0), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "card_id": str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "txn_ts": transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": f"{amount:.2f}",
        "currency": "KRW",
        "mcc": random.choice(["5411", "5812", "5999", "4121", "7011"]),
        "status": random.choice(["approved", "approved", "approved", "declined"]),
        "is_refund": random.choice(["false", "false", "false", "true"]),
        "channel": random.choice(["online", "pos", "mobile"]),
    }


def get_days_to_generate(for_date: Optional[str], days: int) -> List[datetime.date]:
    """
    Determines the range of dates for which to generate data.

    Args:
        for_date: A specific date in YYYY-MM-DD format.
        days: The number of days to generate data for.

    Returns:
        A list of dates to generate data for.
    """
    if for_date:
        return [datetime.strptime(for_date, "%Y-%m-%d").date()]
    else:
        now = datetime.utcnow()
        return [(now - timedelta(days=(days - 1 - d))).date() for d in range(days)]


def write_transactions_to_csv(
    transactions: List[Dict[str, Any]], output_directory: str, ingest_date: datetime.date
):
    """
    Writes a list of transactions to a CSV file.

    Args:
        transactions: A list of transaction records.
        output_directory: The directory to write the CSV file to.
        ingest_date: The ingestion date for the data.
    """
    day_directory = os.path.join(
        output_directory, f"transactions/ingest_date={ingest_date.isoformat()}"
    )
    os.makedirs(day_directory, exist_ok=True)
    output_file = os.path.join(
        day_directory, f"part-00000-{uuid.uuid4().hex}.csv"
    )

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(transactions[0].keys()))
        writer.writeheader()
        writer.writerows(transactions)

    print(f"Write complete")
    print(f"Number of rows={len(transactions)}, Number of columns={len(transactions[0].keys())}")
    print(f"Output file={output_file}")


def main():
    """
    Main function to generate and save fake transaction data.
    """
    parser = argparse.ArgumentParser(
        description="Generate fake transaction data."
    )
    parser.add_argument(
        "--days", type=int, default=7, help="Number of days to generate data for."
    )
    parser.add_argument(
        "--for-date",
        type=str,
        default=None,
        help="YYYY-MM-DD to seed exactly one day.",
    )
    parser.add_argument(
        "--records-per-day",
        type=int,
        default=5000,
        help="Number of records to generate per day.",
    )
    parser.add_argument(
        "--dup-rate",
        type=float,
        default=0.02,
        help="Rate of duplicate records to generate.",
    )
    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.05,
        help="Rate of late-arriving records to generate.",
    )
    parser.add_argument(
        "--bronze-dir",
        type=str,
        default="/app/data/bronze",
        help="Directory to save the generated data.",
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducibility."
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    fake = Faker()
    os.makedirs(args.bronze_dir, exist_ok=True)

    # Pre-generate a pool of customers and merchants to pick from
    customers = [str(uuid.uuid4()) for _ in range(2000)]
    merchants = [str(uuid.uuid4()) for _ in range(300)]
    recent_transactions = []

    days_to_generate = get_days_to_generate(args.for_date, args.days)

    for ingest_day in days_to_generate:
        # Generate base transactions for the day
        daily_transactions = []
        for _ in range(args.records_per_day):
            transaction_timestamp = datetime(
                ingest_day.year,
                ingest_day.month,
                ingest_day.day,
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            daily_transactions.append(
                generate_transaction(
                    fake,
                    transaction_timestamp,
                    customer_id=random.choice(customers),
                    merchant_id=random.choice(merchants),
                )
            )

        # Add duplicate transactions
        duplicate_count = int(args.dup_rate * len(daily_transactions))
        if duplicate_count > 0:
            daily_transactions.extend(random.sample(daily_transactions, duplicate_count))

        # Add late-arriving transactions from previous days
        late_count = int(args.late_rate * len(recent_transactions))
        if late_count > 0:
            daily_transactions.extend(random.sample(recent_transactions, late_count))

        # Keep a pool of recent transactions to simulate late arrivals in the future
        recent_transactions.extend(daily_transactions)
        recent_transactions = recent_transactions[-min(5000, len(recent_transactions)):]

        # Write the day's transactions to a CSV file
        write_transactions_to_csv(daily_transactions, args.bronze_dir, ingest_day)


if __name__ == "__main__":
    main()