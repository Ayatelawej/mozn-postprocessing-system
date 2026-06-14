from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

PIPELINE = "scripts/inference/run_pipeline.py"
RUN_AT_MINUTE = 5


def seconds_until_next_run(now=None, minute=RUN_AT_MINUTE):
    now = now or datetime.now(timezone.utc)
    nxt = now.replace(minute=minute, second=0, microsecond=0)
    if nxt <= now:
        nxt = nxt + timedelta(hours=1)
    return (nxt - now).total_seconds()


def main():
    while True:
        print("[scheduler] running pipeline at", datetime.now(timezone.utc).isoformat(), flush=True)
        result = subprocess.run([sys.executable, PIPELINE])
        print("[scheduler] pipeline exit code:", result.returncode, flush=True)
        wait = seconds_until_next_run()
        print(f"[scheduler] sleeping {wait:.0f}s until next run", flush=True)
        time.sleep(wait)


if __name__ == "__main__":
    main()
