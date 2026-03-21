import sys

import sys, pathlib
#print("cwd:", pathlib.Path.cwd())
#print("sys.path[0]:", sys.path[0])

#Import the revised config loader
from src import (
    load_config, 
    run_jobreq, 
    run_hire,
    run_app,
    run_referral,
    run_erp,
    run_backup
)

#Load config (switch YAML files as needed)
config = load_config("config.yaml")

#Run jobreq processing!
run_jobreq(config)

#Run hire processing!
run_hire(config)

#Run app processing!
run_app(config)

#Run referral and erp processing!
run_referral(config)
run_erp(config)

#Backup Project
run_backup()
