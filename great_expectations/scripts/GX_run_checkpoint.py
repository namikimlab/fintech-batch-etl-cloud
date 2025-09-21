import sys
import great_expectations as gx
from great_expectations.checkpoint import (
    SlackNotificationAction,
    UpdateDataDocsAction,
)

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

    # Create a list of one or more Validation Definitions for the Checkpoint to run
    validation_definitions = [
        context.validation_definitions.get(validation_definition_name)
    ]

    # Create a list of Actions for the Checkpoint to perform
    action_list = [
        # This Action updates the Data Docs static website with the Validation
        #   Results after the Checkpoint is run.
        UpdateDataDocsAction(
            name="update_all_data_docs",
        ),
    ]

    checkpoint_name = "my_checkpoint"
    
    # if a checkpoint exist, delete 
    try: 
        checkpoint = context.checkpoints.get(checkpoint_name)
        context.checkpoints.delete(checkpoint_name)
    except: 
        pass

    # Create a checkpoint 
    checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=validation_definitions,
        actions=action_list,
        # result_format keys: "BOOLEAN_ONLY", "BASIC" "SUMMARY", "COMPLETE"
        result_format={"result_format": "BOOLEAN_ONLY"},
    )

    batch_parameters = {"year": "2025", "month": "09"}

    # Run Checkpoint
    validation_results = checkpoint.run(batch_parameters=batch_parameters)

    # returns bool value of validation 
    if not validation_results.success:
        print("❌ Validation failure")
        sys.exit(1)
    print("✅ Validation success")    
    sys.exit(0)
    
if __name__ == "__main__":
    main()