# --- continue after you've created `context`, and connected data source, data asset ---
from datetime import datetime
import great_expectations as gx

data_source_name = "silver"
data_asset_name = "transactions"
batch_definition_name = "silver_daily"
batch_definition_column = "txn_ts"
suite_name = "my_expectation_suite"

def test_column_exist(batch, suite):

    columns = [
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
    
    for column in columns:
        # Define the Expectation to test
        expectation = gx.expectations.ExpectColumnToExist(
            column=column
        )

        # Test the Expectation:
        validation_results = batch.validate(expectation)
        print(validation_results)
    
    # Add the Expectation to the Expectation Suite
    suite.add_expectation(expectation)


def main():
    # Load context, data source, data asset, batch 
    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations",  
    )

    # Retrieve an Expectation Suite from the Data Context
    try: 
        suite = context.suites.get(name=suite_name)
    except: 
        # if none exist, create an Expectation Suite
        suite = gx.ExpectationSuite(name=suite_name)
        # Add the Expectation Suite to the Data Context
        suite = context.suites.add(suite)

    # Retrieve the Batch Definition .
    batch_definition = (
        context.data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name)
    )
    batch = batch_definition.get_batch()

    test_column_exist(batch, suite)

    validation_definition_name = "my_validation_definition"
    # Retrieve the Validation Definition
    try: 
        validation_definition = context.validation_definitions.get(validation_definition_name)
    except: 
        # if none exist, create 
        validation_definition = gx.ValidationDefinition(
            data=batch_definition, suite=suite, name=validation_definition_name
        )
    # optionally, you can save validation definition to data context and retreive later

    print("✅ Running validations")
    # Define a Batch of data to validate
    batch_parameters_monthly = {"year": "2025", "month": "09"}
    validation_results = validation_definition.run(batch_parameters=batch_parameters_monthly)
    print(validation_results)

if __name__ == "__main__":
    main()