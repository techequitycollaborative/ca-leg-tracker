from snapshots import bills, schedule, hearings, topics
from refresh import views
import db

def run_pipeline(force_update=False):
    with db.get_cursor() as cur:
        # bills
        last_update = bills.get_last_update_timestamp(cur)
        bill_updates = bills.fetch_updates(last_update)

        if len(bill_updates["bills"].index) > 0 or force_update:
            bills.upsert(cur, bill_updates)

        # topics
        # topics.update(cur)

        hearing_schedule, bill_schedule = hearings.fetch_updates()

        if len(hearing_schedule):
            hearings.update(cur, hearing_schedule, bill_schedule)

        # views
        views.refresh(cur)