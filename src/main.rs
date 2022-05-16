use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Parser;
use color_eyre::eyre::ContextCompat;
use color_eyre::eyre::{Result, WrapErr};
use flate2::{write::GzEncoder, Compression};
use indexmap::IndexMap;
use mysql::prelude::*;
use serde::Deserialize;
use xxhash_rust::xxh3;

mod ser_mysql;

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

//#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ArgEnum)]
#[derive(Copy, Clone, Debug, PartialEq, clap::ArgEnum)]
#[clap(rename_all = "lowercase")]
enum OutputKind {
    Dir,
    Stdout,
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

#[derive(Deserialize)]
struct Transform {
    column: String,
    kind: TransformKind,
    config: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransformKind {
    Empty,
    Replace,
    Firstname,
    Lastname,
    EmailHash,
}

struct Output {
    kind: OutputKind,
    dir: PathBuf,
    compress: bool,
}

impl Output {
    fn new(kind: OutputKind, dir: &Path, compress: bool) -> Result<Self> {
        match kind {
            OutputKind::Dir => Self::init_dir(dir)?,
            OutputKind::Stdout => {}
        }

        Ok(Self {
            kind,
            dir: dir.to_owned(),
            compress,
        })
    }

    fn init_dir(dir: &Path) -> Result<()> {
        if !dir.is_dir() {
            println!("Creating directory {:?}", dir);
            std::fs::create_dir_all(dir)?;
        }
        Ok(())
    }

    fn writer(&self, db_name: &str, table_name: &str) -> Result<Box<dyn Write>> {
        match self.kind {
            OutputKind::Stdout => {
                println!("## {}.{}", db_name, table_name);
                Ok(Box::new(std::io::stdout()))
            }
            OutputKind::Dir => {
                let ext = if self.compress { "csv.gz" } else { "csv" };
                let filename = self.dir.join(Path::new(
                    format!("{}.{}.{}", db_name, table_name, ext).as_str(),
                ));

                println!("Creating file {:?}", filename);

                let fh = File::create(&filename).wrap_err_with(|| {
                    format!("Failed to create file for writing; {:?}", &filename)
                })?;

                if self.compress {
                    Ok(Box::new(GzEncoder::new(fh, Compression::default())))
                } else {
                    Ok(Box::new(fh))
                }
            }
        }
    }
}

impl OutputKind {
    fn progress_writer(&self, label: &str, total: usize) -> Box<dyn Progress> {
        match self {
            OutputKind::Stdout => Box::new(NullProgress {}),
            OutputKind::Dir => Box::new(FileProgress::new(label, total)),
        }
    }
}

trait Progress {
    fn update(&mut self, _count: usize);
}

struct FileProgress {
    bar: Option<progress::Bar>,
    total: usize,
    one_perc: usize,
}

impl FileProgress {
    fn new(label: &str, total: usize) -> Self {
        let bar = if total > 100 {
            let mut bar = progress::Bar::new();
            bar.set_job_title(label);
            Some(bar)
        } else {
            println!("{label} is pretty small, no progress bar needed");
            None
        };

        Self {
            bar,
            total,
            one_perc: total / 100,
        }
    }
}

struct NullProgress;

impl Progress for NullProgress {
    fn update(&mut self, _count: usize) {
        ()
    }
}

impl Progress for FileProgress {
    fn update(&mut self, count: usize) {
        if let Some(ref mut bar) = self.bar {
            if self.one_perc > 0 && count % self.one_perc == 0 {
                let percent_done = ((count as f64 / self.total as f64) * 100.0) as i32;
                bar.reach_percent(percent_done);
            }
        }
    }
}

impl Drop for FileProgress {
    fn drop(&mut self) {
        if let Some(ref mut bar) = self.bar {
            bar.jobs_done();
        }
    }
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

struct TableInfo {
    db_name: String,
    table_name: String,
    columns_by_name: HashMap<String, usize>,
    pub column_types: Vec<mysql::consts::ColumnType>,
    pub column_names: Vec<String>,
    row_count: usize,
}

impl TableInfo {
    pub fn get(
        mysql: &mysql::Pool,
        db_name: &str,
        table_name: &str,
        filter: &str,
    ) -> Result<Option<Self>> {
        let row_count: usize = mysql
            .get_conn()?
            .query_first(format!(
                "SELECT COUNT(*) FROM `{db_name}`.`{table_name}` WHERE {} LIMIT 1",
                filter
            ))?
            .wrap_err_with(|| format!("failed to get count of {db_name}.{table_name}"))?;

        let maybe_row: Option<mysql::Row> = mysql
            .get_conn()?
            .query_first(format!("SELECT * FROM `{db_name}`.`{table_name}` LIMIT 1",))?;
        match maybe_row {
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
                row_count,
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

impl TransformKind {
    fn apply(&self, config: Option<&String>, value: &mut mysql::Value) {
        use fake::faker::name::en::*;
        use fake::Fake;

        match self {
            TransformKind::Empty => *value = mysql::Value::Bytes(Vec::new()),
            TransformKind::Replace => match config {
                Some(s) => *value = mysql::Value::Bytes(s.as_bytes().to_owned()),
                None => *value = mysql::Value::Bytes(Vec::new()),
            },
            TransformKind::Firstname => {
                let name: &str = FirstName().fake();
                *value = mysql::Value::Bytes(name.as_bytes().to_owned())
            }
            TransformKind::Lastname => {
                let name: &str = LastName().fake();
                *value = mysql::Value::Bytes(name.as_bytes().to_owned())
            }
            TransformKind::EmailHash => {
                let email = match value {
                    mysql::Value::Bytes(b) => hash_email(b),
                    _ => random_string(10),
                };
                *value = mysql::Value::Bytes(email.into())
            }
        }
    }
}

fn random_string(len: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn hash_email(b: &[u8]) -> String {
    let mut name = base64::encode(xxh3::xxh3_64(b).to_le_bytes());
    name.truncate(11);
    format!("{name}@example.com",)
}
