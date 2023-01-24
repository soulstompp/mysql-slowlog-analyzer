CREATE table query_call_session (
     query_call_id INTEGER PRIMARY KEY NOT NULL,
     user_name VARCHAR,
     sys_user_name VARCHAR,
     host_name VARCHAR,
     ip_address VARCHAR,
     thread_id INT NOT NULL
);
