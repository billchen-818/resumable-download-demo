use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(crate) struct Checkpoint {
    pub url: String,
    pub response_url: String,
    pub etag: Option<String>,
    pub total: Option<u64>,
}

pub(crate) struct Paths {
    pub part: PathBuf,
    pub checkpoint: PathBuf,
    lock: PathBuf,
}

impl Paths {
    pub fn new(output: &Path) -> Self {
        let sibling = |suffix| {
            let mut name = output.as_os_str().to_owned();
            name.push(suffix);
            PathBuf::from(name)
        };
        Self {
            part: sibling(".part"),
            checkpoint: sibling(".part.json"),
            lock: sibling(".part.lock"),
        }
    }

    pub fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock)?;
        file.try_lock()
            .context("cannot lock the download destination")?;
        Ok(file)
    }

    pub fn load(&self) -> Result<Option<Checkpoint>> {
        match std::fs::read(&self.checkpoint) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        let parent = self
            .checkpoint
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer(&mut temp, checkpoint)?;
        temp.flush()?;
        temp.persist(&self.checkpoint)?;
        Ok(())
    }

    pub fn offset(&self) -> Result<u64> {
        match std::fs::metadata(&self.part) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    pub fn clear_checkpoint(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.checkpoint) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}
