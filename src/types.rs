use mysql_slowlog_parser::EntryStatement::{AdminCommand, SqlStatement};
use mysql_slowlog_parser::{Entry, EntrySqlStatementObject, EntryStatement};
use polars::datatypes::ArrowDataType::{Float64, Int32, List, UInt32};
use polars::export::arrow::array::{
    Array, Float64Array, ListArray, MutableListArray, MutableUtf8Array, NullArray, PrimitiveArray,
    TryPush, UInt32Array, Utf8Array,
};
use polars::export::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use polars::export::arrow::io::parquet::write::Encoding;
use polars::export::arrow::io::parquet::write::Encoding::Plain;
use polars::prelude::ArrowDataType::{Timestamp, Utf8};
use polars::prelude::ArrowField;
use time::format_description::well_known::Iso8601;
use time::OffsetDateTime;

#[derive(Clone, Debug)]
pub struct QueryEntry {
    pub attributes: QueryAttributes,
    pub call: QueryCall,
    pub session: QuerySession,
    pub stats: Stats,
    pub context: Option<QueryContext>,
}

impl QueryEntry {
    pub fn arrow2_schema() -> Schema {
        let mut fields = vec![];

        fields.append(&mut QueryAttributes::arrow2_fields());
        fields.append(&mut QueryCall::arrow2_fields());
        fields.append(&mut QuerySession::arrow2_fields());
        fields.append(&mut Stats::arrow2_fields());
        fields.append(&mut QueryContext::arrow2_fields());

        Schema {
            fields: fields,
            ..Default::default()
        }
    }

    pub fn encodings() -> Vec<Vec<Encoding>> {
        let mut acc = vec![];
        acc.append(&mut QueryAttributes::arrow2_encodings());
        acc.append(&mut QueryCall::arrow2_encodings());
        acc.append(&mut QuerySession::arrow2_encodings());
        acc.append(&mut Stats::arrow2_encodings());
        acc.append(&mut QueryContext::arrow2_encodings());
        acc
    }

    pub fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];
        acc.append(&mut self.attributes.arrow2_arrays());
        acc.append(&mut self.call.arrow2_arrays());
        acc.append(&mut self.session.arrow2_arrays());
        acc.append(&mut self.stats.arrow2_arrays());

        if let Some(d) = &self.context {
            acc.append(&mut d.arrow2_arrays());
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

impl From<Entry> for QueryEntry {
    fn from(e: Entry) -> Self {
        let (sql, sql_type, objects, context) = match e.statement() {
            SqlStatement(s) => (
                s.statement.to_string(),
                Some(s.entry_sql_type().to_string()),
                Some(s.objects()),
                s.context.clone(),
            ),
            EntryStatement::InvalidStatement(s) => (s.into(), None, None, None),
            AdminCommand(ac) => (ac.command.to_string(), None, None, None),
        };

        let attributes = QueryAttributes {
            sql,
            sql_type,
            objects,
        };

        let call = QueryCall {
            start_time: OffsetDateTime::from_unix_timestamp(e.start_timestamp() as i64).unwrap(),
            log_time: OffsetDateTime::parse(&e.time().to_string(), &Iso8601::DEFAULT).unwrap(),
        };

        let session = QuerySession {
            user_name: e.user().to_string(),
            sys_user_name: e.sys_user().to_string(),
            host_name: e.host().to_string(),
            ip_address: e.ip_address().to_string(),
            thread_id: e.thread_id(),
        };

        let stats = Stats {
            query_time: e.query_time(),
            lock_time: e.lock_time(),
            rows_sent: e.rows_sent(),
            rows_examined: e.rows_examined(),
        };

        let context = context.and_then(|c| {
            Some(QueryContext {
                request_id: c.id,
                caller: c.caller,
                function: c.function,
                line: c.line,
            })
        });

        Self {
            attributes,
            call,
            session,
            stats,
            context,
        }
    }
}

trait ArrowFields {
    fn arrow2_fields() -> Vec<ArrowField>;

    fn arrow2_encodings() -> Vec<Vec<Encoding>>;

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>>;

    fn utf8_array(s: &str) -> Box<dyn Array> {
        Utf8Array::<i32>::from(&[Some(s)]).boxed()
    }

    fn utf8_list_array(data: Vec<String>) -> Box<ListArray<i32>> {
        let mut list = MutableListArray::<i32, MutableUtf8Array<i32>>::new();

        list.try_push(Some(data.iter().map(|d| Some(d)).into_iter()))
            .unwrap();

        Box::new(list.into())
    }
}

#[derive(Clone, Debug)]
pub struct QueryAttributes {
    pub(crate) sql: String,
    pub(crate) sql_type: Option<String>,
    pub(crate) objects: Option<Vec<EntrySqlStatementObject>>,
}

impl QueryAttributes {
    fn object_names(&self) -> Vec<String> {
        match &self.objects {
            Some(o) => o.iter().fold(vec![], |mut acc, o| {
                let mut name = String::new();

                if let Some(sn) = &o.schema_name {
                    name.push_str(sn);
                }

                name.push_str(&o.object_name);

                acc.push(name);

                acc
            }),
            None => vec![],
        }
    }
}

impl ArrowFields for QueryAttributes {
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

        acc.push(Self::utf8_array(&self.sql));
        acc.push(Utf8Array::<i32>::from(&[Some(&self.sql)]).boxed());

        let o = self.objects.clone().unwrap_or(vec![]);

        if o.len() > 0 {
            acc.push(Self::utf8_list_array(self.object_names()));
        } else {
            acc.push(NullArray::new_null(DataType::Null, 1).boxed());
        }

        acc
    }
}

#[derive(Clone, Debug)]
pub struct QueryCall {
    pub(crate) start_time: OffsetDateTime,
    pub(crate) log_time: OffsetDateTime,
}

impl ArrowFields for QueryCall {
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            Field::new("start_time", Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("log_time", Timestamp(TimeUnit::Millisecond, None), false),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain]]
    }

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(PrimitiveArray::from(&[Some(self.start_time.unix_timestamp())]).boxed());
        acc.push(PrimitiveArray::from(&[Some(self.log_time.unix_timestamp())]).boxed());

        acc
    }
}

#[derive(Clone, Debug)]
pub struct QuerySession {
    pub(crate) user_name: String,
    pub(crate) sys_user_name: String,
    pub(crate) host_name: String,
    pub(crate) ip_address: String,
    pub(crate) thread_id: u32,
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

        acc.push(Self::utf8_array(&self.user_name));
        acc.push(Self::utf8_array(&self.sys_user_name));
        acc.push(Self::utf8_array(&self.host_name));
        acc.push(Self::utf8_array(&self.ip_address));
        acc.push(UInt32Array::from(&[Some(self.thread_id)]).boxed());

        acc
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Stats {
    pub(crate) query_time: f64,
    pub(crate) lock_time: f64,
    pub(crate) rows_sent: u32,
    pub(crate) rows_examined: u32,
}

impl ArrowFields for Stats {
    fn arrow2_fields() -> Vec<ArrowField> {
        vec![
            ArrowField::new("query_time", Float64, false),
            ArrowField::new("lock_time", Float64, false),
            ArrowField::new("rows_sent", Int32, true),
            ArrowField::new("rows_examined", Int32, true),
        ]
    }

    fn arrow2_encodings() -> Vec<Vec<Encoding>> {
        vec![vec![Plain], vec![Plain], vec![Plain], vec![Plain]]
    }

    fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];

        acc.push(Float64Array::from(&[Some(self.query_time)]).boxed());
        acc.push(Float64Array::from(&[Some(self.lock_time)]).boxed());
        acc.push(UInt32Array::from(&[Some(self.rows_sent)]).boxed());
        acc.push(UInt32Array::from(&[Some(self.rows_examined)]).boxed());

        acc
    }
}

#[derive(Clone, Debug)]
pub struct QueryContext {
    pub request_id: Option<String>,
    pub caller: Option<String>,
    pub function: Option<String>,
    pub line: Option<u32>,
}

impl ArrowFields for QueryContext {
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
            Utf8Array::<i32>::from(&[self.request_id.clone()]).boxed(),
            Utf8Array::<i32>::from(&[self.caller.clone()]).boxed(),
            Utf8Array::<i32>::from(&[self.function.clone()]).boxed(),
            UInt32Array::from(&[self.line.clone()]).boxed(),
        ]
    }
}
