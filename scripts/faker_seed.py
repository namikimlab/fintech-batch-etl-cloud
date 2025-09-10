# Generate bronze CSV per ingest_date=YYYY-MM-DD, with duplicates & late arrivals.
import argparse, os, random, csv, uuid
from datetime import datetime, timedelta
from faker import Faker

def gen_txn(fk, txn_ts, customer_id=None, merchant_id=None):
    if customer_id is None: customer_id = str(uuid.uuid4())
    if merchant_id is None: merchant_id = str(uuid.uuid4())
    amount = round(random.uniform(1.0, 300.0), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "card_id": str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "txn_ts": txn_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": f"{amount:.2f}",
        "currency": "KRW",
        "mcc": random.choice(["5411","5812","5999","4121","7011"]),
        "status": random.choice(["approved","approved","approved","declined"]),
        "is_refund": random.choice(["false","false","false","true"]),
        "channel": random.choice(["online","pos","mobile"]),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--records-per-day", type=int, default=5000)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--bronze-dir", type=str, default="/app/data/bronze")
    args = ap.parse_args()

    fk = Faker()
    now = datetime.utcnow()
    os.makedirs(args.bronze_dir, exist_ok=True)

    customers = [str(uuid.uuid4()) for _ in range(2000)]
    merchants = [str(uuid.uuid4()) for _ in range(300)]
    recent_rows = []

    for d in range(args.days):
        ingest_day = (now - timedelta(days=(args.days-1-d))).date()
        day_dir = os.path.join(args.bronze_dir, f"transactions/ingest_date={ingest_day.isoformat()}")
        os.makedirs(day_dir, exist_ok=True)
        out_fp = os.path.join(day_dir, f"part-00000-{uuid.uuid4().hex}.csv")

        rows = []
        for _ in range(args.records_per_day):
            txn_ts = datetime(ingest_day.year, ingest_day.month, ingest_day.day,
                              random.randint(0,23), random.randint(0,59), random.randint(0,59))
            rows.append(gen_txn(fk, txn_ts,
                                customer_id=random.choice(customers),
                                merchant_id=random.choice(merchants)))

        # Inject duplicates (exact copies)
        dup_count = int(args.dup_rate * len(rows))
        if dup_count > 0:
            rows += random.sample(rows, dup_count)

        # Late arrivals (sample from earlier generated rows)
        late_count = int(args.late_rate * len(recent_rows)) if recent_rows else 0
        if late_count > 0:
            rows += random.sample(recent_rows, late_count)

        # Track recent for future late arrivals
        recent_rows += rows[-min(5000, len(rows)):]  # cap memory

        with open(out_fp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)

        print(f"Wrote {len(rows)} rows to {out_fp}")

if __name__ == "__main__":
    main()
