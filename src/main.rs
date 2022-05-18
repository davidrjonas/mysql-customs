use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::{Result, WrapErr};
use indexmap::IndexMap;
use mysql::prelude::*;
use serde::Deserialize;

mod output;
mod ser_mysql;
mod table_info;
mod trace_filter;
mod transforms;

use output::*;
use table_info::*;
use trace_filter::*;
use transforms::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(
        short,
        long,
        env,
        default_value = "mysql://root:password@127.0.0.1:3306/mysql"
    )]
    database_url: String,

    #[clap(short, long, env, default_value = "config.yaml")]
    configfile: PathBuf,

    #[clap(arg_enum, short, long, env, default_value = "dir")]
    output: OutputKind,

    #[clap(short, long, env, default_value = "trunk")]
    target_directory: PathBuf,

    #[clap(long, env)]
    compress: bool,
}

#[derive(Deserialize)]
struct Config {
    databases: IndexMap<String, Database>,
}

#[derive(Deserialize)]
pub struct Database {
    pub tables: IndexMap<String, Table>,
    pub trace_filters: Option<TraceFilterList>,
}

#[derive(Deserialize)]
pub struct Table {
    pub order_column: Option<String>,
    pub filter: Option<String>,
    pub transforms: Option<Vec<Transform>>,
    pub related_only: Option<RelatedTable>,
}

#[derive(Deserialize)]
pub struct RelatedTable {
    pub table: String,
    pub column: String,
    pub foreign_column: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = File::open(args.configfile).wrap_err("Could open config file")?;
    let config: Config = serde_yaml::from_reader(f).wrap_err("Failed to parse config file")?;

    let output = Output::new(args.output, &args.target_directory, args.compress)?;
    let opts = mysql::Opts::from_url(&args.database_url)?;

    for (db_name, db) in config.databases.iter() {
        let mut conn =
            mysql::Conn::new(mysql::OptsBuilder::from_opts(opts.clone()).db_name(Some(db_name)))?;

        if let Some(tf_list) = &db.trace_filters {
            tf_list.setup(&mut conn)?;
        }

        for (table_name, table) in db.tables.iter() {
            process_table(
                &mut conn,
                output.writer(db_name, table_name)?,
                db_name,
                db,
                table_name,
                table,
                args.output,
            )?;
        }
    }

    Ok(())
}

fn process_table(
    conn: &mut mysql::Conn,
    writer: Box<dyn Write>,
    db_name: &str,
    db: &Database,
    table_name: &str,
    table: &Table,
    output_kind: OutputKind,
) -> Result<()> {
    let mut filter = table.filter.as_deref().unwrap_or("1").to_owned();

    let info = match TableInfo::get(conn, db_name, table_name, &filter)? {
        Some(info) => info,
        None => {
            eprintln!("## Table is empty, not writing; {db_name}.{table_name}");
            return Ok(());
        }
    };

    let mut join = String::new();
    if let Some(tf_list) = &db.trace_filters {
        let (tf_join, tf_join_filter) = tf_list.get_join_filter(db, &info);

        if !tf_join_filter.is_empty() {
            filter.push_str(" AND ");
            filter.push_str(&tf_join_filter);

            join.push_str(&tf_join);
        }
    }

    if let Some(related_only) = &table.related_only {
        // If table has related_only then we want to join to that other table and let its filtering
        // filter this table. So we'll need to add the join and then add the trace filters that the
        // _other table_ would have. OR we could select into a temp table the filter data we need
        // from the other table and join on that. That seems safer/easier but two steps.

        join.push_str(
            format!(
                " LEFT JOIN `{}` ON `{}`.`{}` = `{}`.`{}`",
                related_only.table,
                related_only.table,
                related_only.column,
                table_name,
                related_only.foreign_column
            )
            .as_str(),
        );

        filter.push_str(
            format!(
                " AND `{}`.`{}` IS NOT NULL",
                related_only.table, related_only.column
            )
            .as_str(),
        );

        if let Some(tf_list) = &db.trace_filters {
            let related_info = match TableInfo::get(conn, db_name, table_name, &filter)? {
                Some(info) => info,
                None => {
                    eprintln!("## Related table is empty, not writing; {db_name}.{table_name}");
                    return Ok(());
                }
            };

            let (related_join, related_filter) = tf_list.get_join_filter(db, &related_info);

            if !related_join.is_empty() {
                join.push_str(&related_join);
                filter.push_str(&related_filter);
            }
        }
    }

    let sql = format!(
        "SELECT `{}`.* FROM `{}` {} WHERE {} ORDER BY {} ASC",
        table_name,
        table_name,
        join,
        filter,
        table.order_column.as_deref().unwrap_or("id"),
    );

    dbg!(&sql);

    let rows: Vec<mysql::Row> = conn.query(sql)?;

    let mut progress =
        output_kind.progress_writer(format!("{db_name}.{table_name}").as_str(), info.row_count);
    let mut wtr = csv::WriterBuilder::new().from_writer(writer);
    wtr.serialize(&info.column_names)?;

    let mut count = 0;
    for row in rows.into_iter() {
        //dbg!("{:?}", &row);
        let mut values = row.unwrap();
        for transform in table.transforms.as_ref().unwrap_or(&Vec::new()) {
            let item = values
                .get_mut(info.get_column_index(transform.column.as_str()))
                .expect("valid index");
            transform.kind.apply(transform.config.as_ref(), item);
        }

        let ser = &ser_mysql::Row::new(&info.column_types, &values);
        wtr.serialize(ser)?;

        count += 1;
        progress.update(count);
    }

    Ok(())
}
