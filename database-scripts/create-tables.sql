create schema if not exists ca;


-- BILL DATA

create table if not exists ca.bill (
	bill_id integer primary key,
	name text,
	bill_number text,
	full_text text,
	author text,
	origin_house_id integer,
	committee_id integer,
	status text
);

create table if not exists ca.bill_history (
	bill_history_id integer primary key,
	bill_id integer,
	entry_date date,
	entry_text text
);

create table if not exists ca.house_vote_result (
	hosue_vote_result_id integer primary key,
	vote_date date,
	bill_id integer,
	house_id integer,
	votes_for integer,
	votes_against integer
);

create table if not exists ca.committee_vote_result (
	committee_vote_result_id integer primary key,
	vote_date date,
	bill_id integer,
	committee_id integer,
	votes_for integer,
	votes_against integer
);



-- HOUSE AND COMMITTEE DATA

create table if not exists ca.house (
	house_id integer primary key,
	name text
);
insert into ca.house (house_id, name) values (1, 'Assembly'), (2, 'Senate');

create table if not exists ca.committee (
	committee_id integer primary key,
	house_id integer,
	name text,
	webpage_link text
);