use alloc::borrow::Cow;
use bytes::Bytes;
use mysql_slowlog_parser::{
    Entry, EntryCall, EntrySession, EntrySqlAttributes, EntrySqlStatementObject, EntryStats,
    SqlStatementContext,
};
use polars::datatypes::ArrowDataType::{Int32, List, UInt32};
use polars::export::arrow::array::{
    Array, Int64Array, ListArray, MutableListArray, MutableUtf8Array, NullArray, PrimitiveArray,
    TryPush, UInt32Array, Utf8Array,
};
use polars::export::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use polars::export::arrow::io::parquet::write::Encoding;
use polars::export::arrow::io::parquet::write::Encoding::Plain;
use polars::prelude::ArrowDataType::{Duration, Timestamp, Utf8};
use polars::prelude::ArrowField;
use std::ops::Deref;

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

    pub fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];
        acc.append(&mut self.sql_attributes().arrow2_arrays());
        acc.append(&mut self.call().arrow2_arrays());
        acc.append(&mut self.session().arrow2_arrays());
        acc.append(&mut self.stats().arrow2_arrays());

        if let Some(d) = &self.sql_attributes.statement.sql_context() {
            let c = QueryContext(d.clone());
            acc.append(&mut c.arrow2_arrays());
        } else {
            acc.append(&mut vec![
                NullArray::new_null(DataType::Null, 1).boxed(),
                NullArray::new_null(DataType::Null, 1).boxed(),
                NullArray::new_null(DataType::Null, 1).boxed(),
                NullArray::new_null(DataType::Null, 1).boxed(),
            ]);
        }

        acc
    }
}

trait ArrowFields {
    fn arrow2_fields() -> Vec<ArrowField>;

    fn arrow2_encodings() -> Vec<Vec<Encoding>>;

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>>;

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
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            ArrowField::new("sql", Utf8, false),
            ArrowField::new("sql_type", Utf8, true),
            ArrowField::new(
                "objects",
                List(Box::new(ArrowField::new("item", Utf8, false))),
                true,
            ),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain]]
    }

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(Self::utf8_array(&self.sql()));
        acc.push(Utf8Array::<i32>::from(&[Some(&self.sql())]).boxed());

        let o = self.objects().unwrap_or(vec![]);

        if o.len() > 0 {
            let mut o_acc = vec![];

            for o in o {
                o_acc.push(o.full_object_name_bytes());
            }

            acc.push(Self::utf8_list_array(o_acc));
        } else {
            acc.push(NullArray::new_null(DataType::Null, 1).boxed());
        }

        acc
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
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            Field::new("start_time", Timestamp(TimeUnit::Second, None), false),
            Field::new("log_time", Timestamp(TimeUnit::Second, None), false),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain]]
    }

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(PrimitiveArray::from(&[Some(self.start_time().unix_timestamp())]).boxed());
        acc.push(PrimitiveArray::from(&[Some(self.log_time().unix_timestamp())]).boxed());

        acc
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
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            ArrowField::new("user_name", Utf8, false),
            ArrowField::new("sys_user_name", Utf8, true),
            ArrowField::new("host_name", Utf8, true),
            ArrowField::new("ip_address", Utf8, true),
            ArrowField::new("thread_id", Int32, true),
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

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(Self::utf8_array(&self.user_name()));
        acc.push(Self::utf8_array(&self.sys_user_name()));
        acc.push(Self::utf8_array(
            &self.host_name().clone().unwrap_or(Cow::from("localhost")),
        ));
        acc.push(Self::utf8_array(
            &self.ip_address().clone().unwrap_or(Cow::from("127.0.0.1")),
        ));
        acc.push(UInt32Array::from(&[Some(self.thread_id())]).boxed());

        acc
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
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            ArrowField::new("query_time", Duration(TimeUnit::Microsecond), false),
            ArrowField::new("lock_time", Duration(TimeUnit::Microsecond), false),
            ArrowField::new("rows_sent", Int32, true),
            ArrowField::new("rows_examined", Int32, true),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain], vec![Plain]]
    }

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(Int64Array::from(&[Some((self.query_time() * 1000000.0) as i64)]).boxed());
        acc.push(Int64Array::from(&[Some((self.lock_time() * 1000000.0) as i64)]).boxed());
        acc.push(UInt32Array::from(&[Some(self.rows_sent())]).boxed());
        acc.push(UInt32Array::from(&[Some(self.rows_examined())]).boxed());

        acc
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
    fn arrow2_fields() -> Vec<ArrowField> {
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

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        vec![
            Utf8Array::<i32>::from(&[self.request_id()]).boxed(),
            Utf8Array::<i32>::from(&[self.caller()]).boxed(),
            Utf8Array::<i32>::from(&[self.function()]).boxed(),
            UInt32Array::from(&[self.line()]).boxed(),
        ]
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
