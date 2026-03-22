# src/config.py

import yaml
from pathlib import Path
import pandas as pd
import warnings

import logging

pd.set_option('future.no_silent_downcasting', True)
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

def load_config(config_name="default.yaml", config_dir=None):
    """
    Loads YAML config and returns a single dict with all paths, patterns, and settings.
    """
    # Step 1: Load base config from YAML
    if config_dir is None:
        project_root = Path(__file__).resolve().parents[1] # up 2 levels
        config_dir = project_root / "config"
    config_path = config_dir / config_name
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Step 2: Compute important paths
    project_root = Path(__file__).resolve().parents[1] # up 2 levels
    data_root = project_root / cfg["data_root"]

    logging.info(f"Project root: {project_root}")

    paths = {
        "PROJECT_ROOT": project_root,

        "DATA_ROOT": data_root,
        "RAW_DATA_ROOT": data_root / cfg["raw_data_root"],
        "HISTORICAL_DATA_ROOT": data_root / cfg["historical_data_root"],
        "PROCESSED_DATA_ROOT": data_root / cfg["processed_data_root"],

        "SCRUM_FILE": data_root / cfg["historical_data_root"] / cfg["scrum_file"],

        "LOG_DIR": project_root / cfg["log_dir"],

        "OUTPUT_JOBREQS_PAR": data_root / cfg["processed_data_root"] / cfg["output_jobreqs_par"],

        "OUTPUT_HIRES_PAR": data_root / cfg["processed_data_root"] / cfg["output_hires_par"],
        "OUTPUT_HIRESDB_PAR": data_root / cfg["historical_data_root"] / cfg["output_hiresdb_par"],

        "OUTPUT_APPS_CSV": data_root / cfg["processed_data_root"] / cfg["output_apps_csv"],
        "OUTPUT_APPS_PAR": data_root / cfg["processed_data_root"] / cfg["output_apps_par"],
        "OUTPUT_APPS_OFFER_ACCEPTS_PAR": data_root / cfg["processed_data_root"] / cfg["output_apps_offer_accepts_par"],
        "OUTPUT_FUNNEL_PAR": data_root / cfg["processed_data_root"] / cfg["output_funnel_par"],

        "OUTPUT_REFERRALS_PARQUET": data_root / cfg["processed_data_root"] / cfg["output_referrals_par"],
        "OUTPUT_REFERRALS_FUNNEL_PARQUET": data_root / cfg["processed_data_root"] / cfg["output_referrals_funnel_par"],
        
        "OUTPUT_MHEADCOUNT_PARQUET": data_root / cfg["processed_data_root"] / cfg["output_mheadcount_par"],

    }

    patterns = {
        "ALL_STATUS_PATTERN": cfg["all_status_pattern"],
        "FULFILLMENT_PATTERN": cfg["fulfillment_pattern"],
        "INTX_HIRES_PATTERN": cfg["intx_hires_pattern"],
        "DAILY_APP_PATTERN": cfg["daily_app_pattern"],
        "PROSPECT_PATTERN": cfg["prospect_pattern"],
        "HEADCOUNT_PATTERN": cfg["headcount_pattern"],
    }

    vars = {
        "REQS_YEARS_SCOPE": cfg["reqs_years_scope"],
    }

    # Step 3: Merge everything into one config dictionary
    config = {**cfg, **paths, **patterns, **vars}
    return config

__all__ = ["load_config"]
