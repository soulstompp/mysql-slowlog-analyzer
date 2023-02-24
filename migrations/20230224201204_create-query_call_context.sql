CREATE TABLE query_call_context (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     request_id VARCHAR,
     caller VARCHAR,
     function VARCHAR,
     line INTEGER
);
