use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::ArgEnum;
use color_eyre::eyre::{Result, WrapErr};
use flate2::{write::GzEncoder, Compression};

#[derive(Copy, Clone, Debug, PartialEq, ArgEnum)]
#[clap(rename_all = "lowercase")]
pub enum OutputKind {
    Dir,
    Stdout,
}

pub struct Output {
    kind: OutputKind,
    dir: PathBuf,
    compress: bool,
}

impl Output {
    pub fn new(kind: OutputKind, dir: &Path, compress: bool) -> Result<Self> {
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

    pub fn writer(&self, db_name: &str, table_name: &str) -> Result<Box<dyn Write>> {
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
    pub fn progress_writer(&self, label: &str, total: usize) -> Box<dyn Progress> {
        match self {
            OutputKind::Stdout => Box::new(NullProgress {}),
            OutputKind::Dir => Box::new(FileProgress::new(label, total)),
        }
    }
}

pub trait Progress {
    fn update(&mut self, _count: usize);
}

struct FileProgress {
    bar: Option<progress::Bar>,
    total: usize,
    one_perc: usize,
}

impl FileProgress {
    pub fn new(label: &str, total: usize) -> Self {
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
