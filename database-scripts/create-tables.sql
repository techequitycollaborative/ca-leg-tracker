create schema if not exists ca;


-- BILL DATA

create table if not exists ca.bill (
	bill_id integer primary key,
	bill_name text,
	bill_number text,
	user_name text,
	full_text text,
	author text,
	origin_chamber_id integer,
	committee_id integer,
	status text
);

create table if not exists ca.bill_history (
	bill_history_id integer primary key,
	bill_id integer,
	entry_date date,
	entry_text text
);

create table if not exists ca.bill_schedule (
	bill_schedule_id integer primary key,
	bill_id integer,
	event_date date,
	event_text text
);

create table if not exists ca.chamber_vote_result (
	chamber_vote_result_id integer primary key,
	vote_date date,
	bill_id integer,
	chamber_id integer,
	votes_for integer,
	votes_against integer,
	votes_other integer
);

create table if not exists ca.committee_vote_result (
	committee_vote_result_id integer primary key,
	vote_date date,
	bill_id integer,
	committee_id integer,
	votes_for integer,
	votes_against integer
);



-- CHAMBER AND COMMITTEE DATA

create table if not exists ca.chamber (
	chamber_id integer primary key,
	name text
);
insert into ca.chamber (chamber_id, name) values (1, 'Assembly'), (2, 'Senate');

create table if not exists ca.chamber_schedule (
    schedule_id integer primary key,
    chamber_id integer,
    event_date text,
    description text,
    source text
);

create table if not exists ca.committee (
	committee_id integer primary key,
	chamber_id integer,
	name text,
	webpage_link text
);

create table if not exists ca.legislator (
    legislator_id integer primary key,
    chamber_id integer,
    name text,
    district integer,
    party text
);
-- USER CUSTOMIZED DATA

create table if not exists ca.bill_issue (
    bill_issue_id integer primary key,
    issue_id integer,
    bill_id integer
);

create table if not exists ca.issue (
    issue_id integer primary key,
    issue_name text
);

create table if not exists ca.legislator_expected_vote (
    legislator_expected_vote_id integer primary key,
    legislator_id integer,
    bill_id integer,
    expected_vote text
);

create table if not exists ca.user_action (
    user_action_id integer primary key,
    bill_id integer,
    date date,
    action_type text
);