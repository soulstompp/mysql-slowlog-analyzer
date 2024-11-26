# MySQL Slowlog Analyzer

This library parses a MySQL slow query log and records the normalized (all values are removed and replaced with
a `?` binding) and relevant information into a relational database for querying.

## The Database
The database is filled immediately via

### Engine
Currently only SQLite is supported, in order to avoid any library requirements on the client where this is run. This
will eventually support MySQL 8 for the database but will not be a feature until after 0.1 release.

### Schema
The database contains the following tables:

#### queries
This has a normalized version of the query with any values information removed.

#### query_calls
The individual calls made for the query, which ties to the other log entry information.

Note: query_id contains the primary key from queries table

#### query_call_stats
Statistics from the entry.
* query_time
* lock_time
* rows_sent
* rows_examined

Note: query_call_id contains the primary key from query_calls table

#### query_call_details
This table contains a single JSON blob. This contains key value pairs that are parsed from the query preceding the
query. These comments are often used to store a unique identifier from the web request that generated the query. The
JSON will contain a key and a string value for each of the pairs parsed from the comment.

Note: query_call_id contains the primary key from query_calls table

#### query_call_session
This table contains information about the client making the call and MySQL's connection id for the entry.

## Usage
This library attempts to make writing reports off the data recorded as simple as possible. See lib documentation for
details.
