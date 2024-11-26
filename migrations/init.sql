-- queries_calls DDL
CREATE SEQUENCE queries_id_seq;

CREATE TABLE queries (
    id INTEGER PRIMARY KEY DEFAULT nextval('queries_id_seq'),
    sql VARCHAR NOT NULL,
    sql_type VARCHAR
);

CREATE INDEX queries_sql_idx ON queries (sql);
CREATE INDEX queries_sql_type_idx ON queries (sql_type, sql);

-- query_calls DDL
CREATE SEQUENCE query_calls_id_seq;

CREATE TABLE query_calls (
    id INTEGER PRIMARY KEY DEFAULT nextval('query_calls_id_seq'),
    query_id INTEGER NOT NULL,
    start_time DATETIME NOT NULL,
);

CREATE INDEX query_calls_query_id_start_time_idx ON query_calls (query_id, start_time);
CREATE INDEX query_calls_start_time_idx ON query_calls (start_time);

-- query_call_stats DDL
CREATE table query_call_stats (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     query_time DECIMAL NOT NULL,
     lock_time DECIMAL NOT NULL,
     rows_sent INTEGER NOT NULL,
     rows_examined INTEGER NOT NULL
);

-- query_call_details DDL
CREATE table query_call_details (
     query_call_id integer primary key not null,
     details varchar not null
);

CREATE INDEX query_call_details_details_idx ON query_call_details (details);

-- query_call_session DDL
CREATE table query_call_session (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     user_name VARCHAR,
     sys_user_name VARCHAR,
     host_name VARCHAR,
     ip_address VARCHAR,
     thread_id INT NOT NULL
);

-- db_objects DDL
CREATE SEQUENCE db_objects_id_seq;

CREATE TABLE db_objects (
    id INTEGER PRIMARY KEY DEFAULT nextval('db_objects_id_seq'),
    schema_name VARCHAR,
    object_name VARCHAR NOT NULL
);

CREATE INDEX db_objects_schema_name_object_name_idx ON db_objects (schema_name, object_name);

-- db_objects DDL
CREATE TABLE query_objects (
    query_id INTEGER NOT NULL,
    db_object_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, db_object_id)
);

-- db_objects DDL
CREATE TABLE query_call_context (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     request_id VARCHAR,
     caller VARCHAR,
     function VARCHAR,
     line INTEGER
);