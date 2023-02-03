CREATE TABLE db_objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    schema_name VARCHAR,
    object_name VARCHAR NOT NULL
);

CREATE INDEX db_objects_schema_name_object_name_idx ON db_objects (schema_name, object_name);

CREATE TABLE query_objects (
    query_id INTEGER NOT NULL,
    db_object_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, db_object_id)
);