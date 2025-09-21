# --- continue after you've created `context`, and connected data source, data asset ---
import great_expectations as gx

# Configuration
DATA_SOURCE_NAME = "silver"
DATA_ASSET_NAME = "transactions"
BATCH_DEFINITION_NAME = "silver_daily"
SUITE_NAME = "my_expectation_suite"

COLUMNS = [
    "transaction_id",
    "customer_id",
    "card_id",
    "merchant_id",
    "txn_ts",
    "amount",
    "currency",
    "mcc",
    "status",
    "is_refund",
    "channel",
    "source_file",
    "ingest_date",
    "ingest_ts",
    "txn_date",
]

COLUMN_TYPE_MAP = {
    "txn_ts": "TimestampType",
    "is_refund": "BooleanType",
    "ingest_ts": "TimestampType",
}

ALLOWED_VALUE_MAP = {
    "currency": ["KRW"],
    "mcc": ["5411", "5812", "5999", "4121", "7011"],
    "status": ["approved", "declined"],
    "channel": ["online", "pos", "mobile"],
}

def build_expectations(suite):
    """ Builds and adds expectations to the given suite."""

    # Test column exist 
    for column in COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=column))
    
    # Test column types
    for col_name, col_type in COLUMN_TYPE_MAP.items():
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column=col_name, type_=col_type
            )
        )

    # Test column values to be in a given set.
    for col_name, values in ALLOWED_VALUE_MAP.items():
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=col_name, value_set=values
            )
        )

    # Test if transaction id is unique 
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
    )


def main():
    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations",
    )

    try:
        suite = context.suites.get(name=SUITE_NAME)
    except gx.exceptions.DataContextError:
        suite = gx.ExpectationSuite(name=SUITE_NAME)
        context.suites.add(suite)

    # clear any pre-existing expectation 
    suite.expectations.clear()
    build_expectations(suite)

if __name__ == "__main__":
    main()
