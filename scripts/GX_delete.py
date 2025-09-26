import great_expectations as gx

context = gx.get_context(project_root_dir="/opt/airflow/great_expectations")

# Access the datasource
ds = context.data_sources.get("silver")

# Delete the asset by its name
ds.delete_asset("transactions_dir")
print(f"Asset deleted.")

