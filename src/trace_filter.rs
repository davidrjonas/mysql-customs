use color_eyre::eyre::Result;
use mysql::prelude::*;
use serde::Deserialize;

use crate::TableInfo;

#[derive(Deserialize)]
pub struct TraceFilter {
    pub name: String,
    pub source: TraceFilterSource,
    pub match_columns: Vec<String>,
}

#[derive(Deserialize)]
pub struct TraceFilterSource {
    pub db: String,
    pub table: String,
    pub column: String,
    pub filter: String,
}

#[derive(Deserialize)]
pub struct TraceFilterList(Vec<TraceFilter>);

impl TraceFilter {
    fn setup(&self, conn: &mut mysql::Conn) -> Result<()> {
        println!("# Setting up trace filter '{}'", self.name);

        let table_name = self.table_name();

        let sql = format!("DROP TEMPORARY TABLE IF EXISTS `{table_name}`");
        dbg!(&sql);
        conn.query_drop(sql)?;

        let sql = format!(
            "CREATE TEMPORARY TABLE `{}` AS (SELECT `{}` FROM `{}`.`{}` WHERE {} ORDER BY `{}` ASC)",
            table_name,
            self.source.column,
            self.source.db,
            self.source.table,
            self.source.filter,
            self.source.column,
        );

        dbg!(&sql);

        conn.query_drop(sql)?;

        let count: usize = conn
            .query_first(format!("SELECT COUNT(*) FROM `{table_name}`"))?
            .unwrap_or(0);

        println!("# Found {count} rows");

        Ok(())
    }

    fn table_name(&self) -> String {
        format!("_customs_tmp_{}", self.name)
    }

    fn get_join_filter(&self, info: &TableInfo) -> (String, String) {
        let tmp_table = self.table_name();
        let table_name = &info.table_name;

        match self.match_column(info) {
            Some(match_column) => (
                format!(
                    "LEFT JOIN `{tmp_table}` ON `{table_name}`.`{match_column}` = `{tmp_table}`.id"
                ),
                format!("`{tmp_table}`.id IS NOT NULL"),
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
    pub fn setup(&self, conn: &mut mysql::Conn) -> Result<()> {
        for tf in self.as_ref() {
            tf.setup(conn)?;
        }

        Ok(())
    }

    pub fn get_join_filter(&self, db: &crate::Database, info: &TableInfo) -> (String, String) {
        match &db.trace_filters {
            Some(tf_list) if tf_list.len() > 0 => {
                let mut joins: Vec<String> = Vec::new();
                let mut join_filters: Vec<String> = Vec::new();

                for tf in tf_list.as_ref() {
                    let (join, filter) = tf.get_join_filter(info);
                    if !join.is_empty() {
                        joins.push(join);
                        join_filters.push(filter);
                    }
                }

                (joins.join(" "), join_filters.join(" AND "))
            }
            _ => (String::new(), String::new()),
        }
    }
}
