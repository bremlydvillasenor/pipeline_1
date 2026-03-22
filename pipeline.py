import sys
import pathlib

from src import (
    load_config,
    run_jobreq,
    run_hire,
    run_app,
    run_referral,
    run_backup,
)
from src.erp import run_erp
from src._utils import setup_logging

# Load config
config = load_config("config.yaml")

# Configure logging (console + daily log file)
setup_logging(config["LOG_DIR"])

# Run pipeline stages
jobreq_df, refresh_date = run_jobreq(config)

run_hire(config)

run_app(config)

referrals_df, unique_referrals_df = run_referral(config)

run_erp(config, unique_referrals_df)

# Backup project
run_backup()
