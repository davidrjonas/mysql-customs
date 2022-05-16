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
mod transforms;

use output::*;
use table_info::*;
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
struct Database {
    tables: IndexMap<String, Table>,
}

#[derive(Deserialize)]
struct Table {
    order_column: Option<String>,
    filter: Option<String>,
    transforms: Option<Vec<Transform>>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = File::open(args.configfile).wrap_err("Could open config file")?;
    let config: Config = serde_yaml::from_reader(f).wrap_err("Failed to parse config file")?;

    let output = Output::new(args.output, &args.target_directory, args.compress)?;
    let pool = mysql::Pool::new(mysql::Opts::from_url(&args.database_url)?)?;

    for (db_name, db) in config.databases.iter() {
        for (table_name, table) in db.tables.iter() {
            process_table(
                pool.clone(),
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
    mysql: mysql::Pool,
    writer: Box<dyn Write>,
    db_name: &str,
    _db: &Database,
    table_name: &str,
    table: &Table,
    output_kind: OutputKind,
) -> Result<()> {
    let filter = table.filter.as_deref().unwrap_or("1");
    let info = match TableInfo::get(&mysql, db_name, table_name, filter)? {
        Some(info) => info,
        None => {
            eprintln!("## Table is empty, not writing; {db_name}.{table_name}");
            return Ok(());
        }
    };

    let sql = format!(
        "SELECT * FROM `{db_name}`.`{table_name}` WHERE {} ORDER BY {} ASC",
        filter,
        table.order_column.as_deref().unwrap_or("id"),
    );

    let rows: Vec<mysql::Row> = mysql.get_conn()?.query(sql)?;

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
