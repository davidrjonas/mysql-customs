use std::collections::HashMap;

use color_eyre::eyre::ContextCompat;
use color_eyre::eyre::Result;
use mysql::prelude::*;

pub struct TableInfo {
    pub db_name: String,
    pub table_name: String,
    columns_by_name: HashMap<String, usize>,
    pub column_types: Vec<mysql::consts::ColumnType>,
    pub column_names: Vec<String>,
}

impl TableInfo {
    pub fn get(conn: &mut mysql::Conn, db_name: &str, table_name: &str) -> Result<Option<Self>> {
        let sql = format!("SELECT `{table_name}`.* FROM `{table_name}` LIMIT 1");
        dbg!(&sql);

        match conn.query_first(sql)? {
            None => Ok(None),
            Some(row) => Ok(Some(Self {
                db_name: db_name.into(),
                table_name: table_name.into(),
                columns_by_name: Self::index_columns(&row),
                column_types: Self::column_types(&row),
                column_names: row
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().to_string())
                    .collect(),
            })),
        }
    }

    pub fn get_column_index(&self, column_name: &str) -> usize {
        *self
            .columns_by_name
            .get(column_name)
            .wrap_err_with(|| {
                format!(
                    "Failed to find column named {} in {}.{}. Columns: {}",
                    column_name,
                    self.db_name,
                    self.table_name,
                    serde_json::to_string(&self.column_names).expect("valid json")
                )
            })
            .expect("valid column")
    }

    fn index_columns(row: &mysql::Row) -> HashMap<String, usize> {
        let columns = row.columns_ref();
        let mut index = HashMap::new();

        for (i, c) in columns.iter().enumerate() {
            index.insert(c.name_str().into_owned(), i);
        }

        index
    }

    fn column_types(row: &mysql::Row) -> Vec<mysql::consts::ColumnType> {
        row.columns_ref().iter().map(|c| c.column_type()).collect()
    }
}
