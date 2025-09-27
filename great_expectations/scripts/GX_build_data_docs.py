import great_expectations as gx

context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)

# Build Data Docs
context.build_data_docs()   # regenerates the HTML locally

# Print out where the HTML actually is
for site in context.get_docs_sites_urls():
    print(f"[GX] Site name: {site.get('site_name')}")
    print(f"[GX] Local path/URL: {site.get('site_url')}")
    # If this is a local file site, also print the index.html path explicitly
    if site.get("site_name") == "local_site":
        print("[GX] Index file:", "/opt/airflow/great_expectations/uncommitted/data_docs/local_site/index.html")
