use mysql_slowlog_parser::{
    Entry, EntryCall, EntrySession, EntrySqlAttributes, EntrySqlStatementObject, EntryStats,
    SqlStatementContext,
};
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

/// Extension of parser::EntryCall
#[derive(Clone, Debug, PartialEq)]
pub struct QueryCall(pub EntryCall);

impl Deref for QueryCall {
    type Target = EntryCall;

    fn deref(&self) -> &Self::Target {
        &self.0
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

/// Extension of parser::EntryStats
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStats(pub EntryStats);

impl Deref for QueryStats {
    type Target = EntryStats;

    fn deref(&self) -> &Self::Target {
        &self.0
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

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySqlStatementObjects(pub EntrySqlStatementObject);

impl Deref for QuerySqlStatementObjects {
    type Target = EntrySqlStatementObject;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
