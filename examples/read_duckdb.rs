use duckdb::Row;
use mysql_slowlog_analyzer::db::AggregateStats;
use mysql_slowlog_analyzer::{
    query_column_set, ColumnSet, Error, LogData, LogDataConfig, SortingPlan,
};
use mysql_slowlog_parser::{EntryCodecConfig, EntryMasking};
use std::path::PathBuf;
use tokio::time::Instant;

#[derive(Debug)]
struct FilterObject {
    schema_name: Option<String>,
    object_name: String,
}

impl ColumnSet for FilterObject {
    fn set_sql(_: &Option<SortingPlan>) -> String {
        r#"
            SELECT o.schema_name, o.object_name, qc.id AS query_call_id
            FROM db_objects o
            JOIN query_objects qo ON qo.db_object_id = o.id
            JOIN query_calls qc ON qc.query_id = qo.query_id
            "#
        .to_string()
    }

    fn from_row(r: &Row) -> Result<Self, Error> {
        Ok(Self {
            schema_name: r.get("schema_name")?,
            object_name: r.get("object_name")?,
        })
    }

    fn columns() -> Vec<String> {
        vec!["schema_name".into(), "object_name".into()]
    }

    fn key() -> String {
        "query_call_id".to_string()
    }

    fn display_values(&self) -> Vec<(&str, String)> {
        vec![
            (
                "schema_name",
                self.schema_name
                    .clone()
                    .unwrap_or("NULL".into())
                    .to_string(),
            ),
            ("object_name", self.object_name.to_string()),
        ]
    }
}

#[tokio::main]
async fn main() {
    let p = PathBuf::from("/home/soulstompp/dev/lobsters8/data/lobsters8-mysql-slow.log");
    //    let p = PathBuf::from("data/slow-test-queries-small.log");
    let config = LogDataConfig {
        data_path: None,
        codec_config: EntryCodecConfig {
            masking: EntryMasking::PlaceHolder,
            map_comment_context: None,
        },
    };

    LogData::drop_db(&p, config.clone()).await.unwrap();
    let mut s = LogData::open(&p, config.clone()).await.unwrap();

    let instant = Instant::now();

    s.record_db().await.unwrap();

    let c = s.connection();

    println!("wrote in {}", instant.elapsed().as_secs_f64());

    let stats = query_column_set::<AggregateStats<FilterObject>>(c, None)
        .await
        .unwrap();

    stats.display_vertical();
}
