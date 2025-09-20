"""
Generates fake transaction data and saves it to a CSV files.
The data includes duplicates and late-arriving data to simulate real-world scenarios.

Arguments to be passed to the script:
--today (required)
    The date for the generated transactions, in YYYY-MM-DD format.
    Example: 2025-09-20

--num-transactions (optional, default=1000)
    Number of transactions to generate for the given date.
    Example: --num-transactions 5000

--late-rate (optional, default=0.1)
    Fraction of generated transactions to mark as "late arrivals" 
    (i.e., they will have timestamps offset by up to 2 days earlier or later).
    Example: --late-late 0.15  # 15% late arrivals

--dup-rate (optional, default=0.05)
    Fraction of transactions to duplicate (simulating duplicates in raw data).
    Example: --dup-rate 0.1  # 10% duplicates

--output-dir (optional, default="./data/bronze")
    Directory where the output CSV file will be written.
    Example: --output-dir ./bronze_data

"""
import csv
import random
import uuid
from datetime import datetime, timedelta
import argparse
import os

def create_transaction(today: datetime):
    """Creates a single synthetic transaction for a given date"""
    
    # Generate a random timestamp within the 24-hour window of the given date
    seconds_in_day = 24 * 60 * 60  # 86400 seconds
    transaction_timestamp = today + timedelta(seconds=random.randint(0, seconds_in_day - 1))

    # Simulate a small merchant pool for repeatable joins
    merchant_id = f"MER{random.randint(1000, 1020)}"  # 21 merchants
    customer_id = f"CUS{random.randint(1000, 9999)}"  # Large pool for variety

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "card_id": str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "txn_ts": transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": f"{random.uniform(1000.0, 50000.0):.2f}",
        "currency": "KRW",
        "mcc": random.choice(["5411", "5812", "5999", "4121", "7011"]),
        "status": random.choice(["approved", "approved", "approved", "declined"]),
        "is_refund": str(random.choice([False, False, False, True])).lower(),
        "channel": random.choice(["online", "pos", "mobile"]),
    }

def generate_transactions_for_date(num_transactions: int, today: datetime):
    """Generates a list of transactions for a specific date."""
    return [create_transaction(today) for _ in range(num_transactions)]

def read_transactions_from_file(file_path: str):
    """Reads transactions from a CSV file."""
    if not os.path.exists(file_path):
        print(f"Warning: Late arrival file not found: {file_path}")
        return []
    try:
        with open(file_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def inject_late_arrivals(transactions: list, late_rate: float, today: datetime, source_dir: str):
    """Injects late-arriving transactions from D-1 and D-2 by reading from existing files."""
    if late_rate == 0:
        return transactions

    num_late_arrivals = int(len(transactions) * late_rate)
    if num_late_arrivals == 0:
        return transactions

    potential_late_arrivals = []
    for days_ago in [1, 2]:
        past_date = today - timedelta(days=days_ago)
        file_path = os.path.join(source_dir, f"transactions_{past_date.strftime('%Y-%m-%d')}.csv")
        potential_late_arrivals.extend(read_transactions_from_file(file_path))

    if not potential_late_arrivals:
        print("Warning: No historical data found to source late arrivals from.")
        return transactions

    num_to_sample = min(num_late_arrivals, len(potential_late_arrivals))
    
    print(f"Sampling {num_to_sample} late arrivals from a pool of {len(potential_late_arrivals)} historical transactions.")
    late_arrivals = random.sample(potential_late_arrivals, num_to_sample)
    
    return transactions + late_arrivals

def inject_duplicates(transactions: list, dup_rate: float):
    """Injects duplicate transactions."""
    if dup_rate == 0:
        return transactions
        
    num_duplicates = int(len(transactions) * dup_rate)
    if num_duplicates == 0:
        return transactions

    duplicates_to_add = random.sample(transactions, num_duplicates)
    
    # Jitter timestamp for duplicates
    for duplicate in duplicates_to_add:
        try:
            original_ts = datetime.strptime(duplicate["txn_ts"], "%Y-%m-%d %H:%M:%S")
            jittered_ts = original_ts + timedelta(seconds=random.randint(1, 5))
            duplicate["txn_ts"] = jittered_ts.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            # Handle cases where timestamp might be in a different format or not a string
            pass

    return transactions + duplicates_to_add

def write_to_csv(transactions: list, output_path: str):
    """Writes the list of transactions to a CSV file."""
    if not transactions:
        print("No transactions to write.")
        return

    # Ensure all dictionaries have the same keys
    fieldnames = transactions[0].keys()
    for txn in transactions:
        if txn.keys() != fieldnames:
            # A simple way to handle this is to just use the keys from the first transaction
            # A more robust solution would be to find the union of all keys
            pass

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    print(f"Successfully wrote {len(transactions)} records to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data.")
    
    parser.add_argument("--today", required=True, help="The date for the transaction data in YYYY-MM-DD format.")
    
    parser.add_argument("--num-transactions", type=int, default=1000, help="Number of transactions to generate for the date.")
    
    parser.add_argument("--late-rate", type=float, default=0.1, help="Ratio of late-arriving transactions to inject.")
    
    parser.add_argument("--dup-rate", type=float, default=0.05, help="Ratio of duplicate transactions to inject.")
    
    parser.add_argument("--output-dir", default="./app/data/bronze", help="Directory to save the output CSV file.")
    
    args = parser.parse_args()

    try:
        today = datetime.strptime(args.today, "%Y-%m-%d")
    except ValueError:
        print("Error: Please provide the date in YYYY-MM-DD format.")
        return

    # 1. Generate Today's Data
    print(f"Generating {args.num_transactions} transactions for {args.today}...")
    daily_transactions = generate_transactions_for_date(args.num_transactions, today)

    # 2. Inject Late Arrivals
    print(f"Injecting {args.late_rate * 100}% late arrivals...")
    transactions_with_late_arrivals = inject_late_arrivals(daily_transactions, args.late_rate, today, args.output_dir)

    # 3. Inject Duplicates
    print(f"Injecting {args.dup_rate * 100}% duplicates...")
    final_transactions = inject_duplicates(transactions_with_late_arrivals, args.dup_rate)
    
    # 4. Write Daily CSV
    output_filename = f"transactions_{args.today}.csv"
    output_path = os.path.join(args.output_dir, output_filename)
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    write_to_csv(final_transactions, output_path)

if __name__ == "__main__":
    main()