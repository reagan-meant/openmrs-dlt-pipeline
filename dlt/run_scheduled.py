#!/usr/bin/env python3
"""
Run incremental updates - schedule this with cron
"""

from main import run_incremental_pipeline
from datetime import datetime

print(f"🕒 Starting scheduled run at {datetime.now()}")
run_incremental_pipeline()
print(f"✅ Finished at {datetime.now()}")