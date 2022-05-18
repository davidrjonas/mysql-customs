use color_eyre::eyre::Result;
use mysql::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct TraceFilter {
    pub name: String,
    pub source: TraceFilterSource,
    pub match_columns: Vec<String>,
}

#[derive(Deserialize)]
pub struct TraceFilterSource {
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

        let sql = format!("DROP TABLE IF EXISTS `{table_name}`");
        dbg!(&sql);
        conn.query_drop(sql)?;

        let sql = format!(
            "CREATE TEMPORARY TABLE IF NOT EXISTS `{}` AS (SELECT `{}` FROM `{}` WHERE {} ORDER BY `{}` ASC)",
            table_name,
            self.source.column,
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

    fn get_join_filter(&self, table_name: &str, match_column: &str) -> (String, String) {
        let tmp_table = self.table_name();

        let join = format!(
            "LEFT JOIN `{tmp_table}` ON `{table_name}`.`{match_column}` = `{tmp_table}`.id"
        );

        let filter = format!("`{tmp_table}`.id IS NOT NULL");

        (join, filter)
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

    pub fn get_join_filter(
        &self,
        db: &crate::Database,
        info: &crate::TableInfo,
    ) -> (String, String) {
        match &db.trace_filters {
            Some(tf_list) if tf_list.len() > 0 => {
                let mut joins: Vec<String> = Vec::new();
                let mut join_filters: Vec<String> = Vec::new();

                for tf in tf_list.as_ref() {
                    if let Some(match_column) = tf
                        .match_columns
                        .iter()
                        .find(|c| info.column_names.contains(c))
                    {
                        let (join, filter) = tf.get_join_filter(&info.table_name, match_column);
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
