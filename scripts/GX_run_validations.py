import sys
import great_expectations as gx

data_source_name = "silver"
data_asset_name = "transactions"
batch_definition_name = "silver_daily"
batch_definition_column = "txn_ts"
suite_name = "my_expectation_suite"

def main():
    # Load context, data source, data asset, batch 
    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations",  
    )

    # Retrieve the Batch Definition.
    batch_definition = (
        context.data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name)
    )
    batch = batch_definition.get_batch()

    # Retrieve the Expectation Suite 
    try:
        suite = context.suites.get(name=suite_name)
    except gx.exceptions.DataContextError:
        sys.exit("❌ Expectations and Expectation suite must be created first")

    validation_definition_name = "my_validation_definition"

    # Retrieve the Validation Definition
    try:
        validation_definition = context.validation_definitions.get(validation_definition_name)
    except gx.exceptions.DataContextError:
        # if none exist, create 
        print(f"Validation definition {validation_definition_name} not found, creating a new one.")
        validation_definition = gx.ValidationDefinition(
            data=batch_definition, suite=suite, name=validation_definition_name
        )

    print("✅ Running validations")
    # Define a Batch of data to validate
    batch_parameters_monthly = {"year": "2025", "month": "09"}

    validation_results = validation_definition.run(batch_parameters=batch_parameters_monthly)
    print(validation_results)

if __name__ == "__main__":
    main()