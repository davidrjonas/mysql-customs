use std::cell::RefCell;
use std::rc::Rc;

use color_eyre::eyre::Result;
use mysql::prelude::*;
use serde::Deserialize;

use crate::TableInfo;

#[derive(Deserialize, Clone, Debug)]
pub struct TraceFilter {
    pub name: String,
    pub source: TraceFilterSource,
    pub match_columns: Vec<String>,
    #[serde(skip)]
    initialized: Rc<RefCell<String>>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct TraceFilterSource {
    pub db: String,
    pub table: String,
    pub column: String,
    pub filter: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct TraceFilterList(Vec<TraceFilter>);

impl TraceFilter {
    fn setup(&self, conn: &mut mysql::Conn, current_db_name: &str) -> Result<()> {
        println!("# Setting up trace filter '{}'", self.name);

        let tmp_table_name = self.tmp_table_name();

        let sql = format!("DROP TEMPORARY TABLE IF EXISTS {tmp_table_name}");
        dbg!(&sql);
        conn.query_drop(sql)?;

        let sql = format!(
            "CREATE TEMPORARY TABLE {} AS (SELECT `{}` FROM `{}`.`{}` WHERE {} ORDER BY `{}` ASC)",
            tmp_table_name,
            self.source.column,
            self.source.db,
            self.source.table,
            self.source.filter,
            self.source.column,
        );

        dbg!(&sql);

        conn.query_drop(sql)?;

        let count: usize = conn
            .query_first(format!("SELECT COUNT(*) FROM {tmp_table_name}"))?
            .unwrap_or(0);

        println!("# Found {count} rows");

        self.initialized.replace(current_db_name.to_owned());

        Ok(())
    }

    fn tmp_table_name(&self) -> String {
        let prefix = "_customs_tmp_";
        match self.initialized.borrow() {
            s if s.is_empty() => format!("`{}{}`", prefix, self.name),
            s => format!("`{}`.`{}{}`", s, prefix, self.name),
        }
    }

    fn get_join_filter(&self, info: &TableInfo) -> (String, String) {
        let tmp_table = self.tmp_table_name();
        let table_name = &info.table_name;

        match self.match_column(info) {
            Some(match_column) => (
                format!(
                    "LEFT JOIN {tmp_table} ON `{table_name}`.`{match_column}` = {tmp_table}.id"
                ),
                format!("{tmp_table}.id IS NOT NULL"),
            ),
            None => (String::new(), String::new()),
        }
    }

    fn match_column(&self, info: &TableInfo) -> Option<String> {
        if info.db_name == self.source.db && info.table_name == self.source.table {
            return Some(self.source.column.clone());
        }

        self.match_columns
            .iter()
            .find(|c| info.column_names.contains(c))
            .map(|s| s.to_owned())
    }
}

impl std::ops::Deref for TraceFilterList {
    type Target = [TraceFilter];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TraceFilterList {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn append(&self, list: Option<&TraceFilterList>) -> Self {
        let mut new = Self(self.0.clone());
        if let Some(x) = list {
            new.0.extend(x.0.clone());
        }
        new
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn setup(&self, conn: &mut mysql::Conn, current_db_name: &str) -> Result<()> {
        for tf in self.as_ref() {
            tf.setup(conn, current_db_name)?;
        }

        Ok(())
    }

    pub fn get_join_filter(&self, info: &TableInfo) -> (String, String) {
        let mut joins: Vec<String> = Vec::new();
        let mut join_filters: Vec<String> = Vec::new();

        for tf in self.as_ref() {
            let (join, filter) = tf.get_join_filter(info);
            if !join.is_empty() {
                joins.push(join);
                join_filters.push(filter);
            }
        }

        (joins.join(" "), join_filters.join(" AND "))
    }
}
