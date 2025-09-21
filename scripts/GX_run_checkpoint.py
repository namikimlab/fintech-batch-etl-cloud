import sys
import great_expectations as gx

data_source_name = "silver"
data_asset_name = "transactions"
batch_definition_name = "silver_daily"
batch_definition_column = "txn_ts"
suite_name = "my_expectation_suite"
validation_definition_name = "my_validation_definition"

def main():
    # Load context, data source, data asset, batch 
    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations",  
    )

    # Retrieve the Validation Definition
    try:
        validation_definition = context.validation_definitions.get(validation_definition_name)
    except gx.exceptions.DataContextError:
        sys.exit("❌ Validation must be created first")

    # Create the Checkpoint
    checkpoint_name = "my_checkpoint"
    checkpoint = gx.Checkpoint(
        name=checkpoint_name
    )

    batch_parameters = {"year": "2025", "month": "09"}

    # Run Checkpoint
    validation_results = checkpoint.run(batch_parameters=batch_parameters)

    
if __name__ == "__main__":
    main()