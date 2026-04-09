import db
import datetime as dt
import time
import traceback
import logging

from snapshots import bills, hearings, topics
from refresh import views
from utils.slack_bot import send_pipeline_success_alert, send_pipeline_failure_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_pipeline(force_update=False):
    start_time = time.time()
    current_step = "initializing"
    stats = {"bills_updated": 0, "hearings_updated": 0, "runtime_seconds": 0}
    try:
        timestamp = dt.datetime.now(dt.timezone.utc)
        log.info(
            f"Starting daily legislation updates | timestamp={timestamp.strftime('%Y-%m-%d %H:%M %Z')}"
        )

        # --- Phase 1: Fetch (no DB connection open) ---
        log.info("Fetching bill updates...")
        current_step = "bills fetch"
        last_update = bills.get_last_update_timestamp()
        bill_updates = bills.fetch_updates(last_update)
        n_bills = len(bill_updates["bills"].index)
        log.info(f"Bill fetch complete | rows={n_bills}")

        log.info("Fetching hearing schedule...")
        current_step = "hearings fetch"
        hearing_schedule, bill_schedule = hearings.fetch_updates()
        log.info(
            f"Hearing fetch complete | hearings={len(hearing_schedule)}, bill_schedule_rows={len(bill_schedule)}"
        )

        log.info("Opening DB transaction...")
        with db.get_cursor() as cur:
            current_step = "bills write"
            if n_bills > 0 or force_update:
                log.info(
                    f"Upserting bills | rows={n_bills}, force_update={force_update}"
                )
                bills.upsert(cur, bill_updates)
                stats["bills_updated"] = n_bills
            else:
                log.info("No bill updates to write, skipping")

            # TODO: add topics
            current_step = "hearings write"
            if len(hearing_schedule):
                log.info(
                    f"Clearing and re-inserting hearings | rows = {len(hearing_schedule)}"
                )
                hearings.update(cur, hearing_schedule, bill_schedule)
                stats["hearings_updated"] = len(hearing_schedule)
            else:
                log.info("No hearing updates to write, skipping")

            current_step = "views refresh"
            log.info("Refreshing materialized views...")
            views.refresh(cur)
            log.info("Views refreshed")

        stats["runtime_seconds"] = time.time() - start_time
        log.info(
            f"Pipeline complete | bills={stats['bills_updated']} hearings={stats['hearings_updated']} runtime={stats['runtime_seconds']:2f}s"
        )
        send_pipeline_success_alert(stats)

    except Exception as e:
        stats["runtime_seconds"] = time.time() - start_time
        log.error(
            f"Pipeline failed | runtime={stats['runtime_seconds']:.2f}s | error={str(e)}",
            exc_info=True,
        )
        error_details = f"Step that failed: {current_step}\nError: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        send_pipeline_failure_alert(
            f"Pipeline failed after {stats['runtime_seconds']:.2f} seconds",
            error_details,
        )
        raise
