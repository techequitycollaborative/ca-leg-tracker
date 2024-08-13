-- GRANTS

-- Frontend
grant usage on schema [LEGTRACKER_SCHEMA] to [FRONTEND_USER];
grant select on all tables in schema [LEGTRACKER_SCHEMA] to [FRONTEND_USER];

grant usage, select on all sequences in schema [LEGTRACKER_SCHEMA] to [FRONTEND_USER];

grant update, insert, delete on
    [LEGTRACKER_SCHEMA].app_user,
    [LEGTRACKER_SCHEMA].bill_priority,
    [LEGTRACKER_SCHEMA].priority_tier,
    [LEGTRACKER_SCHEMA].bill_dashboard,
    [LEGTRACKER_SCHEMA].bill_details,
    [LEGTRACKER_SCHEMA].bill_issue,
    [LEGTRACKER_SCHEMA].dashboard,
    [LEGTRACKER_SCHEMA].discussion_comment,
    [LEGTRACKER_SCHEMA].issue,
    [LEGTRACKER_SCHEMA].org_position,
    [LEGTRACKER_SCHEMA].user_action,
    [LEGTRACKER_SCHEMA].user_action_type,
    [LEGTRACKER_SCHEMA].user_action_status
to [FRONTEND_USER];

-- Backend
grant usage on schema [LEGTRACKER_SCHEMA] to [BACKEND_USER];

grant select, update, insert, delete, truncate on
    [LEGTRACKER_SCHEMA].bill,
    [LEGTRACKER_SCHEMA].bill_history,
    [LEGTRACKER_SCHEMA].chamber_vote_result,
    [LEGTRACKER_SCHEMA].bill_schedule
to [BACKEND_USER];