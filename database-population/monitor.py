import db 
from refresh import views 
from utils import slack_bot

def main():
    # define temporary table: snapshot.bill where all app.bills_mv conditions apply
    temporary_table_query = """
        CREATE TEMPORARY TABLE eligible_bills AS (
                SELECT
                    openstates_bill_id,
                    session,
                    chamber,
                    bill_num,
                    last_action_date::date
                FROM snapshot.bill
                WHERE session='20252026'
                AND bill_num NOT LIKE 'ACR%'
                AND bill_num NOT LIKE 'HR%'
                AND bill_num NOT LIKE 'SCR%'
                AND bill_num NOT LIKE 'SR%'
                AND bill_num NOT LIKE 'SJR%'
                AND bill_num NOT LIKE 'AJR%'
                AND (
                    last_action_date >= '2025-12-01'
                    OR openstates_bill_id IN (
                        SELECT DISTINCT openstates_bill_id
                        FROM app.bill_history
                        WHERE LOWER(description) LIKE '%inactive file%'
                    )
                    OR bill_num = 'AB 412'
                    OR bill_num = 'SB 435'
                )
        )
    """

    eligible_bill_count_query = """
        SELECT COUNT(*) FROM eligible_bills
    """
    # check if any of the eligible bills are missing from the mat view
    missing_bills_query = """
        SELECT COUNT(*)
        FROM eligible_bills eb
        WHERE NOT EXISTS (
            SELECT 1
            FROM app.bills_mv bm
            WHERE eb.openstates_bill_id = bm.openstates_bill_id
        )
    """

    app_bill_count_query = """
        SELECT COUNT(*) FROM app.bills_mv
    """

    # Execute
    with db.get_cursor() as cur:
        cur.execute(temporary_table_query)
        cur.execute(eligible_bill_count_query)
        eligible_count = cur.fetchone()[0]

        cur.execute(missing_bills_query)
        missing_count = cur.fetchone()[0]

        if missing_count > 0:
            # Slack alert that a manual refresh is needed again
            slack_bot.send_monitor_failure_alert(missing_count)
            
            # manual refresh
            views.refresh(cur)

            # Check app data rows
            cur.execute(app_bill_count_query)
            successful_count = cur.fetchone()[0]
            still_missing = eligible_count - successful_count
            if still_missing:
                slack_bot.send_monitor_refresh_failure_alert()
            else:
                slack_bot.send_monitor_refresh_success_alert(eligible_count)
        else:
            # Slack alert that snapshot and app data is in-sync
            slack_bot.send_monitor_success_alert()
    return

if __name__ == "__main__":
    main()