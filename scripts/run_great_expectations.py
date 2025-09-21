import subprocess
import os
import sys
from pathlib import Path

# Change working directory to where the scripts are located
scripts_dir = Path("/opt/airflow/great_expectations/scripts")
os.chdir(scripts_dir)

# Define the scripts in the order you want to run
scripts = [
    "GX_create.py",
    "GX_connect_data.py",
    "GX_build_expectations.py",
    "GX_run_validations.py",
    "GX_run_checkpoint.py",
    "GX_build_data_docs.py",
]

for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run(["python", script])
    if result.returncode != 0:
        print(f"❌ {script} failed. Stopping execution.")
        break
else:
    print("✅ All scripts completed successfully!")