grant usage on schema [OPENSTATES_SCHEMA] to [BACKEND_USER];

grant select on all tables in schema [OPENSTATES_SCHEMA] to [BACKEND_USER];

grant update, insert, delete on
    [OPENSTATES_SCHEMA].bill,
    [OPENSTATES_SCHEMA].bill_sponsor,
    [OPENSTATES_SCHEMA].bill_action,
    [OPENSTATES_SCHEMA].bill_vote
to [BACKEND_USER];