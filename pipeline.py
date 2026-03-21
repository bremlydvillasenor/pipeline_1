import sys

import sys, pathlib
#print("cwd:", pathlib.Path.cwd())
#print("sys.path[0]:", sys.path[0])

#Import the revised config loader
from src import (
    load_config, 
    run_jobreq_pipeline, 
    run_hire_pipeline,
    run_app_pipeline,
    run_referral_pipeline,
    run_erp_pipeline
    run_backup
)

#Load config (switch YAML files as needed)
config = load_config("config.yaml")

#Run jobreq pipeline!
run_jobreq_pipeline(config)

#Run hire pipeline!
run_hire_pipeline(config)

#Run app pipeline!
run_app_pipeline(config)

#Run referral pipeline!
run_referral_pipeline(config)
run_erp_pipeline(config)

#Backup Project
run_backup()
