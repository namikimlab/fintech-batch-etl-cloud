import great_expectations as gx

    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations",  
    )
