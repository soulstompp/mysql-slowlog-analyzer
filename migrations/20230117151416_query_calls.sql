CREATE TABLE query_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query_id INTEGER NOT NULL,
    start_time DATETIME NOT NULL,
    log_time DATETIME NOT NULL
);

CREATE INDEX query_calls_query_id_start_time_idx ON query_calls (query_id, start_time);
CREATE INDEX query_calls_start_time_idx ON query_calls (start_time);
