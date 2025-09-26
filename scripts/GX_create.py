# Import great_expectations and request a Data Context.
import great_expectations as gx

# Request a File Data Context from a specific folder.
context = gx.get_context(mode="file", project_root_dir="/opt/airflow/great_expectations")

# Review the configuration of the returned File Data Context.
print(context)