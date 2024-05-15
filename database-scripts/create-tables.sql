-- Replace [LEGTRACKER_SCHEMA] with primary schema name
-- Replace [FRONTEND_USER] with webapp db username
-- Replace [BACKEND_USER] with scripting db username

create schema if not exists [LEGTRACKER_SCHEMA];

-- BILL DATA

create table if not exists [LEGTRACKER_SCHEMA].bill (
    bill_id serial primary key,
    openstates_bill_id text unique,
    bill_name text,
    bill_number text,
    full_text text,
    author text,
    coauthors text,
    origin_chamber_id integer,
    committee_id integer,
    status text,
    leginfo_link text,
    leg_session text
);

create table if not exists [LEGTRACKER_SCHEMA].bill_history (
    bill_history_id serial primary key,
    bill_id integer,
    event_date date,
    event_text text,
    chamber_id integer,
    history_order integer
);

create table if not exists [LEGTRACKER_SCHEMA].bill_schedule (
    bill_schedule_id serial primary key,
    bill_id integer,
    event_date date,
    event_text text
);

create table if not exists [LEGTRACKER_SCHEMA].chamber_vote_result (
    chamber_vote_result_id serial primary key,
    vote_date date,
    bill_id integer,
    chamber_id integer,
    vote_text text,
    vote_threshold text,
    vote_result text,
    votes_for integer,
    votes_against integer,
    votes_other integer
);

create table if not exists [LEGTRACKER_SCHEMA].committee_vote_result (
    committee_vote_result_id serial primary key,
    vote_date date,
    bill_id integer,
    committee_id integer,
    votes_for integer,
    votes_against integer
);



-- CHAMBER AND COMMITTEE DATA

create table if not exists [LEGTRACKER_SCHEMA].chamber (
    chamber_id serial primary key,
    name text
);
insert into [LEGTRACKER_SCHEMA].chamber (chamber_id, name)
values
(1, 'Assembly'),
(2, 'Senate');

create table if not exists [LEGTRACKER_SCHEMA].chamber_schedule (
    schedule_id serial primary key,
    chamber_id integer,
    event_date text,
    description text,
    source text
);

create table if not exists [LEGTRACKER_SCHEMA].committee (
    committee_id serial primary key,
    chamber_id integer,
    name text,
    webpage_link text
);

create table if not exists [LEGTRACKER_SCHEMA].committee_assignment (
    committee_assignment_id serial primary key,
    legislator_id integer,
    committee_id integer,
    assignment_type text
);

create table if not exists [LEGTRACKER_SCHEMA].legislator (
    legislator_id serial primary key,
    chamber_id integer,
    name text,
    district integer,
    party text
);



-- USER CUSTOMIZED DATA

create table if not exists [LEGTRACKER_SCHEMA].app_user (
    user_id serial primary key,
    user_name text,
    user_access_level text
);

create table if not exists [LEGTRACKER_SCHEMA].bill_priority (
    bill_priority_id serial primary key,
    bill_details_id integer,
    priority_id integer
);

create table if not exists [LEGTRACKER_SCHEMA].bill_dashboard (
	bill_dashboard_id serial primary key,
	dashboard_id integer,
	bill_id integer,
    hidden boolean default false
);

create table if not exists [LEGTRACKER_SCHEMA].bill_details (
    bill_details_id serial primary key,
    bill_dashboard_id integer,
    alternate_name text,
    assigned_user_id integer,
    org_position_id integer,
    community_sponsor text,
    coalition text,
    policy_notes text,
    political_intel text
);

create table if not exists [LEGTRACKER_SCHEMA].bill_issue (
    bill_issue_id serial primary key,
    issue_id integer,
    bill_details_id integer
);

create table if not exists [LEGTRACKER_SCHEMA].priority_tier (
    priority_id serial primary key,
    priority_description text
);
insert into [LEGTRACKER_SCHEMA].priority_tier (priority_id, priority_description)
values
(1, 'Sponsored'),
(2, 'Priority'),
(3, 'No Priority'),
(4, 'Position');

create table if not exists [LEGTRACKER_SCHEMA].dashboard (
    dashboard_id serial primary key,
    dashboard_name text
);

create table if not exists [LEGTRACKER_SCHEMA].discussion_comment (
    discussion_comment_id serial primary key,
    bill_dashboard_id integer,
    user_id integer,
    comment_datetime timestamp,
    comment_text text
);

create table if not exists [LEGTRACKER_SCHEMA].issue (
    issue_id serial primary key,
    issue_name text
);

create table if not exists [LEGTRACKER_SCHEMA].org_position (
    org_position_id serial primary key,
    org_position_name text
);
insert into [LEGTRACKER_SCHEMA].org_position (org_position_id, org_position_name)
values
(1, 'Needs Decision'),
(2, 'Neutral/No Position'),
(3, 'Support'),
(4, 'Support, if Amended'),
(5, 'Oppose'),
(6, 'Oppose, unless Amended');

create table if not exists [LEGTRACKER_SCHEMA].user_action (
    user_action_id serial primary key,
    bill_dashboard_id integer,
    user_id integer,
    due_date date,
    user_action_type_id integer,
    user_action_status_id integer,
    legislator_id integer,
    committee_id integer,
    link text,
    notes text
);

create table if not exists [LEGTRACKER_SCHEMA].user_action_type (
    user_action_type_id serial primary key,
    user_action_type_name text
);

create table if not exists [LEGTRACKER_SCHEMA].user_action_status (
    user_action_status_id serial primary key,
    user_action_status_name text
);
insert into [LEGTRACKER_SCHEMA].user_action_status (user_action_status_id, user_action_status_name)
values
(1, 'Planned'),
(2, 'In Progress'),
(3, 'Done');



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
    [LEGTRACKER_SCHEMA].chamber_vote_result
to [BACKEND_USER];