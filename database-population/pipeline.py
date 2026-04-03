import db
import datetime as dt
import time
import traceback

from snapshots import bills, hearings, topics
from refresh import views
from utils.slack_bot import send_pipeline_success_alert, send_pipeline_failure_alert


def run_pipeline(force_update=False):
    start_time = time.time()
    stats = {
        "bills_updated": 0,
        "hearings_updated": 0,
        "runtime_seconds": 0
    }
    try:
        with db.get_cursor() as cur:
            # log today's date
            timestamp = dt.datetime.now(dt.timezone.utc)
            print(f"""
                #####################
                STARTING DAILY LEGISLATION DATA UPDATES...
                CURRENT TIMESTAMP: {timestamp.strftime('%Y-%m-%d %I:%M%p')}
                #####################
                """)
            # bills
            last_update = bills.get_last_update_timestamp(cur)
            bill_updates = bills.fetch_updates(last_update)

            if len(bill_updates["bills"].index) > 0 or force_update:
                bills.upsert(cur, bill_updates)
                stats["bills_updated"] = len(bill_updates["bills"].index)

            # topics
            # topics.update(cur)

            hearing_schedule, bill_schedule = hearings.fetch_updates()

            if len(hearing_schedule):
                hearings.update(cur, hearing_schedule, bill_schedule)
                stats["hearings_updated"] = len(hearing_schedule)

            # views
            views.refresh(cur)
        # Success - send alert
        stats["runtime_seconds"] = time.time() - start_time
        send_pipeline_success_alert(stats)
    except Exception as e:
        stats["runtime_seconds"] = time.time() - start_time
        error_details = f"Step that failed: Check logs\nError: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        send_pipeline_failure_alert(
            f"Pipeline failed after {stats['runtime_seconds']:.2f} seconds", 
            error_details
        )
        raise  # Re-raise so cron knows it failed