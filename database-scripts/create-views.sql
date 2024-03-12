CREATE VIEW ca.bill_latest_actions AS
SELECT b.bill_id,
    bd.dashboard_id,
    f.first_date,
    h.event_date AS last_date,
    h.event_text AS last_text,
    h.event_chamber AS last_chamber,
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
        a.event_chamber,
        a."row"
    FROM (
        SELECT bh.bill_history_id,
            bh.bill_id,
            bh.event_date,
            bh.event_text,
            c.name as event_chamber,
            row_number() OVER (PARTITION BY bh.bill_id ORDER BY bh.event_date DESC, bh.bill_history_id) AS "row"
        FROM ca.bill_history bh
        LEFT JOIN ca.chamber c USING(chamber_id)
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
LEFT JOIN (
    SELECT bill_id,
        MIN(event_date) AS first_date
    FROM ca.bill_history
    GROUP BY bill_id
) f USING (bill_id)
;
GRANT SELECT ON ca.bill_latest_actions TO [FRONTEND_USER];
