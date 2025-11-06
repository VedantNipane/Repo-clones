#!/usr/bin/env python3
"""
Clone GitHub repositories from a CSV file in batches, with progress tracking and resume support.

Usage:
    python clone_githubs.py 1     # clone 1 repo (for testing)
    python clone_githubs.py 5     # clone 5 repos at a time
    python clone_githubs.py all   # clone all remaining repos

Assumes:
- CSV file has: Sr_no,source,url,request_success,type,processed
- Only rows where type == 'github' are considered.
- Script can be safely resumed after interruption.
"""

import os
import sys
import csv
import subprocess
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from time import sleep

# =======================
# CONFIGURATION
# =======================
INPUT_CSV = "reportv2.csv"  # <-- CSV file in same repo
OUTPUT_DIR = "cloned_repos"
STATUS_CSV = "clone_status.csv"
LOG_FILE = "clone_log.txt"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =======================
# LOGGING SETUP
# =======================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_console(msg, level="info"):
    """Print to console + log file with colors."""
    colors = {
        "info": "\033[94m",
        "success": "\033[92m",
        "warning": "\033[93m",
        "error": "\033[91m",
        "end": "\033[0m",
    }
    print(f"{colors.get(level, '')}{msg}{colors['end']}")
    getattr(logging, level if level in ["info","warning","error"] else "info")(msg)

# =======================
# LOAD CSVs AND MERGE PROGRESS
# =======================
def load_data():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    if not {"url", "type"}.issubset(df.columns):
        raise ValueError("CSV must contain columns: url, type")

    df = df[df["type"].str.lower() == "github"].copy()

    # Load existing status file if available
    if os.path.exists(STATUS_CSV):
        status_df = pd.read_csv(STATUS_CSV)
        df = df.merge(status_df[["url", "status"]], on="url", how="left")
    else:
        df["status"] = None

    # Keep only those not yet cloned
    df = df[df["status"].isna() | (df["status"] != "success")]

    return df.reset_index(drop=True)

# =======================
# CLONE FUNCTION
# =======================
def clone_repo(url, output_dir):
    repo_name = url.rstrip("/").split("/")[-1]
    repo_path = os.path.join(output_dir, repo_name)

    if os.path.exists(repo_path):
        log_console(f"⏭️ Already exists: {repo_name}", "warning")
        return "exists"

    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", url, repo_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300,
        )
        log_console(f"✅ Cloned successfully: {repo_name}", "success")
        return "success"
    except subprocess.TimeoutExpired:
        log_console(f"⚠️ Timeout cloning: {url}", "warning")
        return "timeout"
    except subprocess.CalledProcessError as e:
        log_console(f"❌ Failed to clone: {url}", "error")
        logging.error(f"Error cloning {url}: {e}")
        return "failed"
    except Exception as e:
        log_console(f"❌ Unexpected error: {url} | {e}", "error")
        return "failed"

# =======================
# SAVE PROGRESS SAFELY
# =======================
def update_status(url, status):
    """Append/update the status CSV after each repo."""
    new_entry = {"url": url, "status": status, "last_updated": datetime.now().isoformat()}
    if os.path.exists(STATUS_CSV):
        df = pd.read_csv(STATUS_CSV)
        df = df[df["url"] != url]
        df = pd.concat([df, pd.DataFrame([new_entry])], ignore_index=True)
    else:
        df = pd.DataFrame([new_entry])
    df.to_csv(STATUS_CSV, index=False)

# =======================
# MAIN SCRIPT
# =======================
def main():
    if len(sys.argv) < 2:
        print("Usage: python clone_githubs.py [N|all]")
        sys.exit(1)

    arg = sys.argv[1]
    df = load_data()

    if len(df) == 0:
        log_console("✅ All repositories are already cloned!", "success")
        return

    total = len(df)
    if arg != "all":
        try:
            limit = int(arg)
            df = df.head(limit)
        except ValueError:
            print("Invalid argument. Use a number (e.g. 2) or 'all'.")
            sys.exit(1)

    log_console(f"🧩 Starting cloning for {len(df)} of {total} repos...", "info")
    logging.info(f"Batch started at {datetime.now()}")

    for idx, row in df.iterrows():
        url = str(row["url"]).strip()
        if not url.startswith("https://github.com"):
            log_console(f"[{idx+1}/{total}] Skipping invalid URL: {url}", "warning")
            update_status(url, "invalid")
            continue

        log_console(f"[{idx+1}/{total}] Cloning: {url}", "info")
        status = clone_repo(url, OUTPUT_DIR)
        update_status(url, status)

        # small delay to avoid rate limits
        sleep(2)

    log_console("🎯 Batch complete.", "success")
    logging.info(f"Batch completed at {datetime.now()}")

if __name__ == "__main__":
    main()
