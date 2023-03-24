use bytes::Bytes;
use mysql_slowlog_parser::{
    Entry, EntryCall, EntrySession, EntrySqlAttributes, EntrySqlStatementObject, EntryStats,
    SqlStatementContext,
};
use arrow2::datatypes::{Field, Schema, TimeUnit};
use std::ops::Deref;
use arrow2::array::{Array, ListArray, MutableListArray, MutableUtf8Array, TryPush, Utf8Array};
use arrow2::datatypes::DataType::{Duration, Int32, List, Timestamp, UInt32, Utf8};
use arrow2::io::parquet::write::Encoding;
use arrow2::io::parquet::write::Encoding::Plain;

/// Extension of parser::Entry
#[derive(Clone, Debug, PartialEq)]
pub struct QueryEntry(pub Entry);

impl Deref for QueryEntry {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl QueryEntry {
    pub fn call(&self) -> QueryCall {
        QueryCall(self.0.call.clone())
    }

    pub fn stats(&self) -> QueryStats {
        QueryStats(self.0.stats.clone())
    }

    pub fn session(&self) -> QuerySession {
        QuerySession(self.0.session.clone())
    }

    pub fn sql_attributes(&self) -> QuerySqlAttributes {
        QuerySqlAttributes(self.0.sql_attributes.clone())
    }

    pub fn arrow2_schema() -> Schema {
        let mut fields = vec![];

        fields.append(&mut QuerySqlAttributes::arrow2_fields());
        fields.append(&mut QueryCall::arrow2_fields());
        fields.append(&mut QuerySession::arrow2_fields());
        fields.append(&mut QueryStats::arrow2_fields());
        fields.append(&mut QueryContext::arrow2_fields());

        Schema {
            fields: fields,
            ..Default::default()
        }
    }

    pub fn encodings() -> Vec<Vec<Encoding>> {
        let mut acc = vec![];
        acc.append(&mut QuerySqlAttributes::arrow2_encodings());
        acc.append(&mut QueryCall::arrow2_encodings());
        acc.append(&mut QuerySession::arrow2_encodings());
        acc.append(&mut QueryStats::arrow2_encodings());
        acc.append(&mut QueryContext::arrow2_encodings());
        acc
    }
}

pub trait ArrowFields {
    fn arrow2_fields() -> Vec<Field>;

    fn arrow2_encodings() -> Vec<Vec<Encoding>>;

    fn utf8_array(s: &str) -> Box<dyn Array> {
        Utf8Array::<i32>::from(&[Some(s)]).boxed()
    }

    fn utf8_list_array(data: Vec<Bytes>) -> Box<ListArray<i32>> {
        let mut list = MutableListArray::<i32, MutableUtf8Array<i32>>::new();

        list.try_push(Some(
            data.iter()
                .map(|d| Some(String::from_utf8_lossy(d)))
                .into_iter(),
        ))
        .unwrap();

        Box::new(list.into())
    }
}

/// Extension of parser::EntryAttributes
#[derive(Clone, Debug, PartialEq)]
pub struct QuerySqlAttributes(pub EntrySqlAttributes);

impl Deref for QuerySqlAttributes {
    type Target = EntrySqlAttributes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArrowFields for QuerySqlAttributes {
    fn arrow2_fields() -> Vec<Field> {
        vec![
            Field::new("sql", Utf8, false),
            Field::new("sql_type", Utf8, true),
            Field::new(
                "objects",
                List(Box::new(Field::new("item", Utf8, false))),
                true,
            ),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain]]
    }
}

/// Extension of parser::EntryCall
#[derive(Clone, Debug, PartialEq)]
pub struct QueryCall(pub EntryCall);

impl Deref for QueryCall {
    type Target = EntryCall;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArrowFields for QueryCall {
    fn arrow2_fields() -> Vec<Field> {
        vec![
            Field::new("start_time", Timestamp(TimeUnit::Second, None), false),
            Field::new("log_time", Timestamp(TimeUnit::Second, None), false),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain]]
    }
}

/// Extension of parser::EntrySession
#[derive(Clone, Debug, PartialEq)]
pub struct QuerySession(pub EntrySession);

impl Deref for QuerySession {
    type Target = EntrySession;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArrowFields for QuerySession {
    fn arrow2_fields() -> Vec<Field> {
        vec![
            Field::new("user_name", Utf8, false),
            Field::new("sys_user_name", Utf8, true),
            Field::new("host_name", Utf8, true),
            Field::new("ip_address", Utf8, true),
            Field::new("thread_id", Int32, true),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![
            vec![Plain],
            vec![Plain],
            vec![Plain],
            vec![Plain],
            vec![Plain],
        ]
    }
}

/// Extension of parser::EntryStats
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStats(pub EntryStats);

impl Deref for QueryStats {
    type Target = EntryStats;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArrowFields for QueryStats {
    fn arrow2_fields() -> Vec<Field> {
        vec![
            Field::new("query_time", Duration(TimeUnit::Microsecond), false),
            Field::new("lock_time", Duration(TimeUnit::Microsecond), false),
            Field::new("rows_sent", Int32, true),
            Field::new("rows_examined", Int32, true),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain], vec![Plain]]
    }
}

/// Extension of parser::EntryContext
#[derive(Clone, Debug, PartialEq)]
pub struct QueryContext(pub SqlStatementContext);

impl Deref for QueryContext {
    type Target = SqlStatementContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> ArrowFields for QueryContext {
    fn arrow2_fields() -> Vec<Field> {
        vec![
            Field::new("request_id", Utf8, true),
            Field::new("caller", Utf8, true),
            Field::new("function", Utf8, true),
            Field::new("line", UInt32, true),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain], vec![Plain]]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySqlStatementObjects(pub EntrySqlStatementObject);

impl Deref for QuerySqlStatementObjects {
    type Target = EntrySqlStatementObject;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
