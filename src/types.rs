use mysql_slowlog_parser::Entry;
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
