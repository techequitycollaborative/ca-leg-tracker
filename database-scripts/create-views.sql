CREATE VIEW ca.bill_latest_actions AS
SELECT b.bill_id,
    bd.dashboard_id,
    h.event_date AS last_date,
    h.event_text AS last_text,
    s.event_date AS next_date,
    s.event_text AS next_text,
    u.user_date,
    u.user_text
FROM ca.bill b
JOIN ca.bill_dashboard bd USING(bill_id)
LEFT JOIN (
    SELECT a.bill_history_id,
        a.bill_id,
        a.event_date,
        a.event_text,
        a."row"
    FROM (
        SELECT bill_history.bill_history_id,
            bill_history.bill_id,
            bill_history.event_date,
            bill_history.event_text,
            row_number() OVER (PARTITION BY bill_history.bill_id ORDER BY bill_history.event_date DESC, bill_history.bill_history_id) AS "row"
        FROM ca.bill_history
    ) a
    WHERE a."row" = 1
) h USING (bill_id)
LEFT JOIN (
    SELECT a.bill_schedule_id,
        a.bill_id,
        a.event_date,
        a.event_text,
        a."row"
    FROM (
        SELECT bill_schedule.bill_schedule_id,
            bill_schedule.bill_id,
            bill_schedule.event_date,
            bill_schedule.event_text,
            row_number() OVER (PARTITION BY bill_schedule.bill_id ORDER BY bill_schedule.event_date DESC, bill_schedule.bill_schedule_id) AS "row"
        FROM ca.bill_schedule
    ) a
    WHERE a."row" = 1
) s USING (bill_id)
LEFT JOIN (
    SELECT b.bill_id,
        b.dashboard_id,
        a.due_date AS user_date,
        a.user_action_type_name AS user_text,
        a."row"
    FROM (
        SELECT ua.bill_dashboard_id,
            ua.due_date,
            uat.user_action_type_name,
            row_number() OVER (PARTITION BY ua.bill_dashboard_id ORDER BY ua.due_date DESC, ua.user_action_id) as "row"
        FROM ca.user_action ua
        JOIN ca.user_action_type uat USING(user_action_type_id)
    ) a
    JOIN ca.bill_dashboard b USING(bill_dashboard_id)
    WHERE a."row" = 1
) u USING (bill_id,dashboard_id)
;
GRANT SELECT ON ca.bill_latest_actions TO [FRONTEND_USER];
