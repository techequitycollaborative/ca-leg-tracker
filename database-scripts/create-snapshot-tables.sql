-- Replace [OPENSTATES_SCHEMA] with scripting schema name
-- Replace [BACKEND_USER] with scripting db username

create schema if not exists [OPENSTATES_SCHEMA];

drop table if exists [OPENSTATES_SCHEMA].bill;
drop table if exists [OPENSTATES_SCHEMA].bill_sponsor;
drop table if exists [OPENSTATES_SCHEMA].bill_action;
drop table if exists [OPENSTATES_SCHEMA].bill_vote;

create table [OPENSTATES_SCHEMA].bill (
    openstates_bill_id text primary key,
    session text,
    chamber text,
    bill_num text,
    title text,
    created_at text,
    updated_at text,
    first_action_date text,
    last_action_date text,
    abstract text
);

create table [OPENSTATES_SCHEMA].bill_sponsor (
    openstates_bill_id text,
    name text,
    full_name text,
    title text,
    district text,
    primary_author text,
    type text
);

create table [OPENSTATES_SCHEMA].bill_action (
    openstates_bill_id text,
    chamber text,
    description text,
    action_date text,
    action_order text
);

create table [OPENSTATES_SCHEMA].bill_vote (
    openstates_bill_id text,
    motion_text text,
    vote_date text,
    vote_location text,
    vote_result text,
    vote_threshold text,
    yes_count text,
    no_count text,
    other_count text
);

