CREATE table query_call_stats (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     query_time DECIMAL NOT NULL,
     lock_time DECIMAL NOT NULL,
     rows_sent INTEGER NOT NULL,
     rows_examined INTEGER NOT NULL
);