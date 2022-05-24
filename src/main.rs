use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::{Result, WrapErr};
use indexmap::IndexMap;
use mysql::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use xxhash_rust::xxh3;

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

#[derive(Deserialize, Debug)]
struct Config {
    databases: IndexMap<String, Database>,
    trace_filters: Option<TraceFilterList>,
}

#[derive(Deserialize, Debug)]
pub struct Database {
    pub tables: IndexMap<String, Table>,
    pub trace_filters: Option<TraceFilterList>,
}

#[derive(Deserialize, Debug)]
pub struct Table {
    pub order_column: Option<String>,
    pub filter: Option<String>,
    pub transforms: Option<Vec<Transform>>,
    pub related_only: Option<RelatedTable>,
}

#[derive(Deserialize, Debug)]
pub struct RelatedTable {
    pub table: String,
    pub column: String,
    pub foreign_column: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = File::open(args.configfile).wrap_err("Could open config file")?;
    let config: Config = serde_yaml::from_reader(f).wrap_err("Failed to parse config file")?;

    let output = Output::new(args.output, &args.target_directory, args.compress)?;
    let opts = mysql::Opts::from_url(&args.database_url)?;

    let first_db_name = config
        .databases
        .keys()
        .next()
        .expect("at least one database is required");

    let mut conn =
        mysql::Conn::new(mysql::OptsBuilder::from_opts(opts.clone()).db_name(Some(first_db_name)))?;

    if let Some(tf_list) = &config.trace_filters {
        tf_list.setup(&mut conn, first_db_name)?;
    }

    for (db_name, db) in config.databases.iter() {
        conn.select_db(db_name);

        if let Some(tf_list) = &db.trace_filters {
            tf_list.setup(&mut conn, db_name)?;
        }

        for (table_name, table) in db.tables.iter() {
            let tf_list = config
                .trace_filters
                .as_ref()
                .map(|x| x.append(db.trace_filters.as_ref()))
                .unwrap_or_else(|| TraceFilterList::new());

            process_table(
                &mut conn,
                output.writer(db_name, table_name)?,
                tf_list,
                db_name,
                db,
                table_name,
                table,
                args.output,
            )?;
        }

        if let Some(tf_list) = &db.trace_filters {
            tf_list.cleanup(&mut conn)?;
        }
    }

    if let Some(tf_list) = &config.trace_filters {
        tf_list.cleanup(&mut conn)?;
    }

    Ok(())
}

fn process_table(
    conn: &mut mysql::Conn,
    output: &Output,
    trace_filters: TraceFilterList,
    db_name: &str,
    db: &Database,
    table_name: &str,
    table: &Table,
) -> Result<()> {
    let writer = output.writer(db_name, table_name)?;
    let info = match TableInfo::get(conn, db_name, table_name)? {
        Some(info) => info,
        None => {
            eprintln!("## Table is empty, not writing; {db_name}.{table_name}");
            return Ok(());
        }
    };

    let mut join_filter = JoinFilter::default();

    join_filter.append(trace_filters.get_join_filter(&info));

    if let Some(related_only) = &table.related_only {
        // If table has related_only then we want to join to that other table and let its filtering
        // filter this table. So we'll need to add the join and then add the trace filters that the
        // _other table_ would have. OR we could select into a temp table the filter data we need
        // from the other table and join on that. That seems safer/easier but two steps.

        join_filter.add(
            format!(
                "LEFT JOIN `{}` ON `{}`.`{}` = `{}`.`{}`",
                related_only.table,
                related_only.table,
                related_only.column,
                table_name,
                related_only.foreign_column.as_deref().unwrap_or("id"),
            ),
            format!(
                "`{}`.`{}` IS NOT NULL",
                related_only.table, related_only.column
            ),
        );

        if !trace_filters.is_empty() {
            let related_info = match TableInfo::get(conn, db_name, &related_only.table)? {
                Some(info) => info,
                None => {
                    eprintln!("## Related table is empty, not writing; {db_name}.{table_name}");
                    return Ok(());
                }
            };

            let related_filter = db
                .tables
                .get(&related_only.table)
                .and_then(|t| t.filter.as_deref())
                .unwrap_or("1")
                .to_owned();

            let related_jf = trace_filters.get_join_filter(&related_info);

            if !related_jf.is_empty() {
                join_filter.append(related_jf);
                join_filter.add_filter(related_filter);
            }
        }
    }

    let from_where_sql = format!(
        "FROM `{}` {} WHERE {}",
        table_name,
        join_filter.join_string(),
        join_filter.filter_string()
    );

    let sql = format!("SELECT COUNT(*) {from_where_sql}");

    dbg!(&sql);

    let row_count: usize = conn.query_first(sql)?.unwrap_or(0);
    let order_column = table.order_column.as_deref().unwrap_or_else(|| {
        if info.column_names.iter().any(|s| s == "id") {
            "id"
        } else {
            &info
                .column_names
                .first()
                .map(|s| s.as_str())
                .unwrap_or("id")
        }
    });

    let sql = format!(
        "SELECT `{}`.* {} ORDER BY `{}`.{} ASC",
        table_name, &from_where_sql, table_name, order_column,
    );

    dbg!(&sql);

    let rows: Vec<mysql::Row> = conn.query(sql)?;

    let mut progress =
        output.progress_writer(format!("{db_name}.{table_name}").as_str(), row_count);
    let mut wtr = csv::WriterBuilder::new().from_writer(writer);
    wtr.serialize(&info.column_names)?;

    let mut rng = get_rng_for_table(db_name, table_name);

    let mut count = 0;
    for row in rows.into_iter() {
        //dbg!("{:?}", &row);
        let mut values = row.unwrap();
        for transform in table.transforms.as_ref().unwrap_or(&Vec::new()) {
            let item = values
                .get_mut(info.get_column_index(transform.column.as_str()))
                .expect("valid index");

            transform
                .kind
                .apply(&mut rng, transform.config.as_ref(), item);
        }

        let ser = &ser_mysql::Row::new(&info.column_types, &values);
        wtr.serialize(ser)?;

        count += 1;
        progress.update(count);
    }

    Ok(())
}

fn get_rng_for_table(db_name: &str, table_name: &str) -> StdRng {
    StdRng::seed_from_u64(xxh3::xxh3_64(
        format!("{}.{}", db_name, table_name).as_bytes(),
    ))
}
