create table query_call_details (
     query_call_id integer primary key not null,
     details varchar not null
);

CREATE INDEX query_call_details_details_idx ON query_call_details (details);
