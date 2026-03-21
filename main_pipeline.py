import sys
#from pathlib import Path

#Ensure src is on your sys.path for import
#sys.path.append(str(Path(".."))) # If your notebook is in 'notebooks/' folder

import sys, pathlib
#print("cwd:", pathlib.Path.cwd())
#print("sys.path[0]:", sys.path[0])

#Import the revised config loader and jobreq ETL
from src import (
    load_config, 
    run_jobreq_pipeline, 
    run_hire_pipeline,
    run_app_pipeline,
    run_referral_pipeline,
    run_backup, run_check
)

#Check if Backup Folder already exist
run_check()

#Load config (switch YAML files as needed)
config = load_config("default.yaml") # Or 'prod.yaml', 'test.yaml', etc.

#Run jobreq pipeline!
jr_df , refresh_date = run_jobreq_pipeline(config)

#Run hire pipeline!
hire_df = run_hire_pipeline(config)

#Run app pipeline!
app_df = run_app_pipeline(config)

#Run referral pipeline!
referral_df, headcount_df, referral_funnel_df = run_referral_pipeline(config)

#Backup Project
run_backup(refresh_date)

#Show resulting DataFrame shape and preview data
#print(f"Final shape: {jr_df.shape}")
