use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::ArgEnum;
use color_eyre::eyre::{Result, WrapErr};
use flate2::{write::GzEncoder, Compression};
use indicatif::ProgressBar as Bar;
use indicatif::ProgressStyle;

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
            eprintln!("## Creating directory {:?}", dir);
            std::fs::create_dir_all(dir)?;
        }
        Ok(())
    }

    pub fn writer(&self, db_name: &str, table_name: &str) -> Result<Box<dyn Write>> {
        match self.kind {
            OutputKind::Stdout => {
                println!("--- {}.{}", db_name, table_name);
                Ok(Box::new(std::io::stdout()))
            }
            OutputKind::Dir => {
                let ext = if self.compress { "csv.gz" } else { "csv" };
                let filename = self.dir.join(Path::new(
                    format!("{}.{}.{}", db_name, table_name, ext).as_str(),
                ));

                eprintln!("## Creating file {:?}", filename);

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

    pub fn progress_writer(&self, label: &str, total: usize) -> Box<dyn Progress> {
        self.kind.progress_writer(label, total)
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
    bar: Bar,
}

impl FileProgress {
    pub fn new(label: &str, total: usize) -> Self {
        let bar = Bar::new(total as u64).with_message(std::borrow::Cow::Owned(label.into()));
        bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .progress_chars("##-"),
        );
        bar.set_draw_delta(10);

        Self { bar }
    }
}

struct NullProgress;

impl Progress for NullProgress {
    fn update(&mut self, _count: usize) {}
}

impl Progress for FileProgress {
    fn update(&mut self, _count: usize) {
        self.bar.inc(1)
    }
}

impl Drop for FileProgress {
    fn drop(&mut self) {
        self.bar.finish_at_current_pos();
    }
}
