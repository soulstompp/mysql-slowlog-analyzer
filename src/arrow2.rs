use crate::types::QueryEntry;
use crate::Error;
use alloc::borrow::Cow;
use async_compat::{Compat, CompatExt};
use bytes::Bytes;
use futures::SinkExt;
use mysql_slowlog_parser::EntrySqlType;
use polars::export::arrow::array::{
    Array, Int64Array, MutableArray, MutableListArray, MutableUtf8Array, PrimitiveArray, TryPush,
    UInt32Array, Utf8Array,
};
use polars::export::arrow::chunk::Chunk;
use polars::export::arrow::io::parquet::write::{
    CompressionOptions, FileSink, GzipLevel, Version, WriteOptions,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::swap;
use std::path::Path;
use time::OffsetDateTime;
use tokio::fs::File;

pub fn write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: Version::V2,
        compression: CompressionOptions::Gzip(Some(GzipLevel::default())),
        data_pagesize_limit: Some(5000),
    }
}

#[derive(Default, Debug)]
pub struct QueryEntrySet<'a> {
    //fields.append(&mut QuerySqlAttributes::arrow2_fields());
    pub sql: Vec<Cow<'a, str>>,
    pub sql_type: Vec<Option<Cow<'a, str>>>,
    pub objects: Vec<Option<Vec<Cow<'a, str>>>>,
    //fields.append(&mut QueryCall::arrow2_fields());
    pub start_time: Vec<OffsetDateTime>,
    pub log_time: Vec<OffsetDateTime>,

    // fields.append(&mut QuerySession::arrow2_fields());
    pub user_name: Vec<Bytes>,
    pub sys_user_name: Vec<Bytes>,
    pub host_name: Vec<Option<Bytes>>,
    pub ip_address: Vec<Option<Bytes>>,
    pub thread_id: Vec<u32>,

    //fields.append(&mut QueryStats::arrow2_fields());
    pub query_time: Vec<i64>,
    pub lock_time: Vec<i64>,
    pub rows_sent: Vec<u32>,
    pub rows_examined: Vec<u32>,

    //fields.append(&mut QueryContext::arrow2_fields());
    pub request_id: Vec<Option<Cow<'a, str>>>,
    pub caller: Vec<Option<Cow<'a, str>>>,
    pub function: Vec<Option<Cow<'a, str>>>,
    pub line: Vec<Option<u32>>,
}

impl<'a> QueryEntrySet<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, e: QueryEntry) -> () {
        let attributes = e.sql_attributes.clone();

        self.sql
            .push(String::from_utf8(attributes.sql.to_vec()).unwrap().into());
        self.sql_type.push(
            attributes
                .sql_type()
                .and_then(|v| Some(Cow::from(v.to_string()))),
        );

        if let Some(o) = attributes.objects() {
            self.objects.push(Some(
                o.into_iter()
                    .map(|v| {
                        let v = v.to_owned();
                        let on = v.full_object_name_bytes().to_owned();
                        Cow::from(String::from_utf8(on.to_vec()).unwrap())
                    })
                    .collect::<Vec<_>>(),
            ))
        } else {
            self.objects.push(None);
        }

        //fields.append(&mut QueryCall::arrow2_fields());
        self.start_time.push(e.call.start_time());
        self.log_time.push(e.call.log_time());

        self.user_name.push(e.session.user_name_bytes());
        self.sys_user_name.push(e.session.sys_user_name_bytes());
        self.host_name.push(e.session.host_name_bytes());
        self.ip_address.push(e.session.ip_address_bytes());
        self.thread_id.push(e.session.thread_id());

        self.query_time
            .push((e.stats.query_time() * 1000000.0) as i64);
        self.lock_time
            .push((e.stats.lock_time() * 1000000.0) as i64);
        self.rows_sent.push(e.stats.rows_sent());
        self.rows_examined.push(e.stats.rows_examined());

        if let Some(context) = e.sql_attributes.statement.sql_context() {
            self.caller.push(
                context
                    .request_id()
                    .and_then(|r| Some(r.to_string().into())),
            );
            self.caller
                .push(context.caller().and_then(|c| Some(c.to_string().into())));
            self.function
                .push(context.function().and_then(|f| Some(f.to_string().into())));
            self.line.push(context.line());
        } else {
            self.request_id.push(None);
            self.caller.push(None);
            self.function.push(None);
            self.line.push(None);
        }
    }

    pub fn len(&self) -> usize {
        self.sql.len()
    }

    pub fn arrow2_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![];
        acc.append(&mut self.sql_attributes_arrays());
        acc.append(&mut self.call_arrays());
        acc.append(&mut self.session_arrays());
        acc.append(&mut self.stats_arrays());
        acc.append(&mut self.context_arrays());

        acc
    }

    pub fn into_chunk(&self) -> Chunk<Box<dyn Array>> {
        Chunk::new(self.arrow2_arrays())
    }

    fn sql_attributes_arrays(&self) -> Vec<Box<dyn Array>> {
        let mut acc = vec![
            Utf8Array::<i32>::from(
                self.sql
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.sql_type
                    .clone()
                    .into_iter()
                    .map(|v| v)
                    .collect::<Vec<_>>(),
            )
            .boxed(),
        ];

        let mut objects = MutableListArray::<i32, MutableUtf8Array<i32>>::new();

        for o in self.objects.clone() {
            if let Some(o) = o {
                objects
                    .try_push(o.into_iter().map(|v| Some(v)).collect::<Vec<_>>().into())
                    .unwrap();
            } else {
                objects.push_null();
            }
        }

        acc.push(objects.as_box());

        acc
    }

    fn call_arrays(&self) -> Vec<Box<dyn Array>> {
        vec![
            PrimitiveArray::from(
                self.start_time
                    .clone()
                    .into_iter()
                    .map(|t| Some(t.unix_timestamp()))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            PrimitiveArray::from(
                self.log_time
                    .clone()
                    .into_iter()
                    .map(|t| Some(t.unix_timestamp()))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
        ]
    }

    pub fn session_arrays(&self) -> Vec<Box<dyn Array>> {
        vec![
            Utf8Array::<i32>::from(
                self.user_name
                    .clone()
                    .into_iter()
                    .map(|v| Some(Cow::from(String::from_utf8(v.to_vec()).unwrap())))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.sys_user_name
                    .clone()
                    .into_iter()
                    .map(|v| Some(Cow::from(String::from_utf8(v.to_vec()).unwrap())))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.host_name
                    .clone()
                    .into_iter()
                    .map(|v| {
                        v.or(Some(Bytes::from("localhost")))
                            .and_then(|v| Some(Cow::from(String::from_utf8(v.to_vec()).unwrap())))
                    })
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.ip_address
                    .clone()
                    .into_iter()
                    .map(|v| {
                        v.or(Some(Bytes::from("127.0.0.1")))
                            .and_then(|v| Some(Cow::from(String::from_utf8(v.to_vec()).unwrap())))
                    })
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            UInt32Array::from(
                self.thread_id
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
        ]
    }

    fn stats_arrays(&self) -> Vec<Box<dyn Array>> {
        vec![
            Int64Array::from(
                self.query_time
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Int64Array::from(
                self.lock_time
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            UInt32Array::from(
                self.rows_sent
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            UInt32Array::from(
                self.rows_examined
                    .clone()
                    .into_iter()
                    .map(|v| Some(v))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
        ]
    }

    fn context_arrays(&self) -> Vec<Box<dyn Array>> {
        vec![
            Utf8Array::<i32>::from(
                self.request_id
                    .clone()
                    .into_iter()
                    .map(|v| v.and_then(|v| Some(v)))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.caller
                    .clone()
                    .into_iter()
                    .map(|v| v.and_then(|v| Some(v)))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Utf8Array::<i32>::from(
                self.function
                    .clone()
                    .into_iter()
                    .map(|v| v.and_then(|v| Some(v)))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            UInt32Array::from(
                self.line
                    .clone()
                    .into_iter()
                    .map(|v| v.and_then(|v| Some(v)))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use polars::export::arrow::array::{
        Array, DictionaryArray, DictionaryKey, PrimitiveArray, Utf8Array,
    };
    use polars::export::arrow::datatypes::DataType;

    #[test]
    fn try_new_ok() {
        let values = Utf8Array::<i32>::from_slice(["a", "aa"]);
        let data_type =
            DataType::Dictionary(i32::KEY_TYPE, Box::new(values.data_type().clone()), false);
        let array = DictionaryArray::try_new(
            data_type,
            PrimitiveArray::from_vec(vec![1, 0]),
            values.boxed(),
        )
        .unwrap();

        assert_eq!(array.keys(), &PrimitiveArray::from_vec(vec![1i32, 0]));
        assert_eq!(
            &Utf8Array::<i32>::from_slice(["a", "aa"]) as &dyn Array,
            array.values().as_ref(),
        );
        assert!(!array.is_ordered());

        assert_eq!(format!("{array:?}"), "DictionaryArray[aa, a]");
    }
}

pub struct ParquetSinks<'a> {
    sinks: HashMap<&'a str, RefCell<FileSink<'a, Compat<File>>>>,
    data_dir: &'a Path,
    entries: RefCell<HashMap<&'a str, QueryEntrySet<'a>>>,
}

impl<'a> ParquetSinks<'a> {
    pub fn partitions() -> Vec<&'a str> {
        vec![
            "create",
            "contextual",
            "delete",
            "read",
            "transactional",
            "unparseable",
            "update",
        ]
    }

    pub async fn open(d: &'a Path) -> ParquetSinks<'a> {
        let partitions = Self::partitions();

        let mut sinks = HashMap::new();
        let mut entries = HashMap::new();

        for name in partitions {
            let mut p = d.to_path_buf();
            p.push(name);

            let f = File::create(&p).await.unwrap();

            let s = Self::build_sink(&f).await.unwrap();

            let _ = entries.insert(name, QueryEntrySet::new());
            let _ = sinks.insert(name, RefCell::from(s));
        }

        Self {
            sinks: sinks.into(),
            entries: entries.into(),
            data_dir: d,
        }
    }

    async fn build_sink(f: &File) -> Result<FileSink<'a, Compat<File>>, Error> {
        Ok(FileSink::try_new(
            f.try_clone().await?.compat(),
            QueryEntry::arrow2_schema(),
            QueryEntry::encodings(),
            write_options(),
        )?)
    }

    pub fn partition_name(&self, ot: Option<EntrySqlType>) -> &'a str {
        if let Some(t) = ot {
            match t {
                EntrySqlType::Query => "read",
                EntrySqlType::Insert => "create",
                EntrySqlType::Update => "update",
                EntrySqlType::Delete => "delete",
                EntrySqlType::StartTransaction => "transactional",
                EntrySqlType::SetTransaction => "transactional",
                EntrySqlType::Commit => "transactional",
                EntrySqlType::Rollback => "transactional",
                EntrySqlType::Savepoint => "transactional",
                _ => "contextual",
            }
        } else {
            "unparseable"
        }
    }

    pub async fn feed(&self, t: Option<EntrySqlType>, e: QueryEntry) -> Result<(), Error> {
        let name = self.partition_name(t.clone());

        let mut entries = self.entries.borrow_mut();
        let set = entries.get_mut(name).unwrap();
        set.push(e);

        if set.len() == 3000 {
            self.write_chunk(name, set).await.unwrap();

            let mut chunk_set = QueryEntrySet::default();

            swap(set, &mut chunk_set);
        }

        Ok(())
    }

    pub async fn write_chunk(&self, name: &str, set: &QueryEntrySet<'_>) -> Result<(), Error> {
        if set.len() == 0 {
            return Ok(());
        }

        let mut sink = self
            .sinks
            .get(name)
            .expect(&format!("expected {} sink to exist", name))
            .clone()
            .borrow_mut();

        let _ = sink.feed(set.into_chunk()).await.unwrap();

        Ok(())
    }

    pub async fn close_all(&mut self) -> Result<(), Error> {
        for name in Self::partitions() {
            self.write_chunk(name, self.entries.borrow().get(name).unwrap())
                .await
                .unwrap();
            let mut sink = self.sinks.get(name).unwrap().borrow_mut();
            sink.close().await.unwrap();
        }

        self.sinks = HashMap::new();

        Ok(())
    }
}
