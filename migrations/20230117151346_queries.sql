CREATE TABLE queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    sql VARCHAR NOT NULL
);

CREATE INDEX queries_sql_idx ON queries (sql);
