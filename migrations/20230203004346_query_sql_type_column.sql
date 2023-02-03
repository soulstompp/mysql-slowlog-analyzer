ALTER TABLE queries ADD COLUMN sql_type VARCHAR;

CREATE INDEX queries_sql_type_idx ON queries (sql_type, sql);
