-- Replace [LEGTRACKER_SCHEMA] with primary schema name

drop table if exists [LEGTRACKER_SCHEMA].bill;
drop table if exists [LEGTRACKER_SCHEMA].bill_history;
drop table if exists [LEGTRACKER_SCHEMA].bill_schedule;
drop table if exists [LEGTRACKER_SCHEMA].chamber_vote_result;
drop table if exists [LEGTRACKER_SCHEMA].committee_vote_result;
drop table if exists [LEGTRACKER_SCHEMA].chamber;
drop table if exists [LEGTRACKER_SCHEMA].chamber_schedule;
drop table if exists [LEGTRACKER_SCHEMA].committee;
drop table if exists [LEGTRACKER_SCHEMA].committee_assignment;
drop table if exists [LEGTRACKER_SCHEMA].legislator;
drop table if exists [LEGTRACKER_SCHEMA].app_user;
drop table if exists [LEGTRACKER_SCHEMA].bill_priority;
drop table if exists [LEGTRACKER_SCHEMA].priority_tier;
drop table if exists [LEGTRACKER_SCHEMA].bill_dashboard;
drop table if exists [LEGTRACKER_SCHEMA].bill_details;
drop table if exists [LEGTRACKER_SCHEMA].bill_issue;
drop table if exists [LEGTRACKER_SCHEMA].dashboard;
drop table if exists [LEGTRACKER_SCHEMA].discussion_comment;
drop table if exists [LEGTRACKER_SCHEMA].issue;
drop table if exists [LEGTRACKER_SCHEMA].org_position;
drop table if exists [LEGTRACKER_SCHEMA].user_action;
drop table if exists [LEGTRACKER_SCHEMA].user_action_status;
drop table if exists [LEGTRACKER_SCHEMA].user_action_type;