//! A real filesystem-backed [`Io`] implementation.
//!
//! Everything through M1 ran on [`MemIo`](crate::io::MemIo). The seam was
//! exercised hard — fault injection, silent lost writes, filesystem-wide crash —
//! but no test had touched a real filesystem, so real fsync ordering and
//! partial-write behaviour were unverified. `m1-findings.md` §10 carried that
//! forward as an M2 item; this is it.
//!
//! Two details that `MemIo` cannot model and which are easy to get wrong:
//!
//! * **Directory fsync.** Creating a file is not durable until the *directory*
//!   is synced. Without that, a crash can leave a file the manifest references
//!   but which the filesystem has forgotten. LazyFS explicitly does not simulate
//!   this class of bug, so it has to be handled by construction rather than
//!   found by testing.
//! * **Short writes.** `write_at` may write fewer bytes than requested; the loop
//!   below is not decoration.

use crate::io::{File, Io};
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Filesystem-backed I/O rooted at a directory.
#[derive(Debug)]
pub struct PosixIo {
    root: PathBuf,
    /// Kept open so the directory can be fsynced after file creation.
    dir: Mutex<fs::File>,
}

impl PosixIo {
    /// Open (creating if needed) a database directory.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let dir = fs::File::open(&root)?;
        // The directory entry for `root` itself must be durable before we start
        // creating files inside it.
        dir.sync_all()?;
        Ok(PosixIo {
            root,
            dir: Mutex::new(dir),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, name: &str) -> io::Result<PathBuf> {
        // Reject anything that could escape the root. A database file name is
        // never legitimately a path.
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || name == "."
            || name == ".."
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("illegal file name: {name:?}"),
            ));
        }
        Ok(self.root.join(name))
    }

    /// fsync the directory, making creations and removals durable.
    fn sync_dir(&self) -> io::Result<()> {
        self.dir.lock().unwrap().sync_all()
    }
}

impl Io for PosixIo {
    fn open(&self, path: &str) -> io::Result<Arc<dyn File>> {
        let p = self.path_for(path)?;
        let existed = p.exists();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&p)?;
        if !existed {
            // A newly created file is not durable until its directory entry is.
            self.sync_dir()?;
        }
        Ok(Arc::new(PosixFile { file: f }) as Arc<dyn File>)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        let p = self.path_for(path)?;
        match fs::remove_file(&p) {
            Ok(()) => self.sync_dir(),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn exists(&self, path: &str) -> bool {
        self.path_for(path).map(|p| p.exists()).unwrap_or(false)
    }

    fn list(&self) -> Vec<String> {
        let mut out: Vec<String> = fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        out.sort();
        out
    }
}

/// A file on a real filesystem.
#[derive(Debug)]
pub struct PosixFile {
    file: fs::File,
}

impl File for PosixFile {
    fn pread(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Loop: a short read is legal and does not mean EOF.
        let mut total = 0;
        while total < buf.len() {
            match self.file.read_at(&mut buf[total..], offset + total as u64) {
                Ok(0) => break, // genuine EOF
                Ok(n) => total += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(total)
    }

    fn pwrite(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        // Loop: a short write is legal, and silently accepting one would
        // truncate a record while reporting success.
        let mut total = 0;
        while total < buf.len() {
            match self.file.write_at(&buf[total..], offset + total as u64) {
                Ok(0) => {
                    return Err(io::Error::new(
                        ErrorKind::WriteZero,
                        "write_at made no progress",
                    ))
                }
                Ok(n) => total += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(total)
    }

    fn sync(&self) -> io::Result<()> {
        // `sync_all` (fsync), not `sync_data` (fdatasync): file length changes
        // on every append, and length is metadata.
        self.file.sync_all()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

/// A temporary directory that removes itself on drop. Test support.
#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a uniquely-named directory under the system temp dir.
    pub fn new(tag: &str) -> io::Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("chakradb-{tag}-{pid}-{nanos}-{n}"));
        fs::create_dir_all(&path)?;
        Ok(TempDir { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_at(tag: &str) -> (TempDir, PosixIo) {
        let d = TempDir::new(tag).unwrap();
        let io = PosixIo::open(d.path()).unwrap();
        (d, io)
    }

    #[test]
    fn write_then_read_roundtrips_on_disk() {
        let (_d, io) = io_at("rt");
        let f = io.open("a").unwrap();
        f.pwrite(0, b"hello world").unwrap();
        f.sync().unwrap();
        let mut buf = [0u8; 11];
        assert_eq!(f.pread(0, &mut buf).unwrap(), 11);
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn data_survives_reopening_the_file() {
        let d = TempDir::new("reopen").unwrap();
        {
            let io = PosixIo::open(d.path()).unwrap();
            let f = io.open("a").unwrap();
            f.pwrite(0, b"persisted").unwrap();
            f.sync().unwrap();
        }
        let io2 = PosixIo::open(d.path()).unwrap();
        let f2 = io2.open("a").unwrap();
        let mut buf = [0u8; 9];
        f2.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"persisted");
    }

    #[test]
    fn read_past_end_returns_zero() {
        let (_d, io) = io_at("past-end");
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abc").unwrap();
        let mut buf = [0u8; 4];
        assert_eq!(f.pread(100, &mut buf).unwrap(), 0);
    }

    #[test]
    fn partial_read_at_tail() {
        let (_d, io) = io_at("tail");
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abcdef").unwrap();
        let mut buf = [0u8; 10];
        assert_eq!(f.pread(4, &mut buf).unwrap(), 2);
        assert_eq!(&buf[..2], b"ef");
    }

    #[test]
    fn sparse_write_zero_fills() {
        let (_d, io) = io_at("sparse");
        let f = io.open("a").unwrap();
        f.pwrite(4, b"xy").unwrap();
        assert_eq!(f.len().unwrap(), 6);
        let mut buf = [9u8; 6];
        f.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"\0\0\0\0xy");
    }

    #[test]
    fn truncate_shrinks_and_grows() {
        let (_d, io) = io_at("trunc");
        let f = io.open("a").unwrap();
        f.pwrite(0, b"abcdef").unwrap();
        f.truncate(3).unwrap();
        assert_eq!(f.len().unwrap(), 3);
        f.truncate(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
    }

    #[test]
    fn exists_list_and_remove() {
        let (_d, io) = io_at("catalog");
        assert!(!io.exists("x"));
        io.open("x").unwrap();
        io.open("y").unwrap();
        assert!(io.exists("x"));
        assert_eq!(io.list(), vec!["x".to_string(), "y".to_string()]);
        io.remove("x").unwrap();
        assert!(!io.exists("x"));
        // Removing a missing file is not an error — recovery relies on this.
        io.remove("x").unwrap();
    }

    #[test]
    fn path_traversal_is_rejected() {
        let (_d, io) = io_at("traversal");
        for bad in ["../escape", "a/b", "..", ".", "", "sub\\file"] {
            assert!(
                io.open(bad).is_err(),
                "accepted illegal file name {bad:?}"
            );
        }
    }

    #[test]
    fn opening_the_same_path_twice_sees_one_file() {
        let (_d, io) = io_at("shared");
        let a = io.open("shared").unwrap();
        a.pwrite(0, b"z").unwrap();
        a.sync().unwrap();
        let b = io.open("shared").unwrap();
        let mut buf = [0u8; 1];
        b.pread(0, &mut buf).unwrap();
        assert_eq!(&buf, b"z");
    }

    #[test]
    fn open_does_not_truncate_an_existing_file() {
        let (_d, io) = io_at("no-trunc");
        {
            let f = io.open("a").unwrap();
            f.pwrite(0, b"keep me").unwrap();
            f.sync().unwrap();
        }
        let f2 = io.open("a").unwrap();
        assert_eq!(f2.len().unwrap(), 7, "reopen truncated the file");
    }

    #[test]
    fn large_write_and_read_back() {
        let (_d, io) = io_at("large");
        let f = io.open("big").unwrap();
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
        f.pwrite(0, &data).unwrap();
        f.sync().unwrap();
        let mut back = vec![0u8; data.len()];
        assert_eq!(f.pread(0, &mut back).unwrap(), data.len());
        assert_eq!(back, data, "large roundtrip corrupted");
    }

    #[test]
    fn concurrent_writes_to_disjoint_ranges() {
        use std::thread;
        let (_d, io) = io_at("concurrent");
        let f = io.open("a").unwrap();
        let hs: Vec<_> = (0..8u64)
            .map(|t| {
                let f = f.clone();
                thread::spawn(move || {
                    let block = vec![t as u8; 1024];
                    f.pwrite(t * 1024, &block).unwrap();
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        f.sync().unwrap();
        let mut buf = vec![0u8; 8 * 1024];
        f.pread(0, &mut buf).unwrap();
        for t in 0..8usize {
            assert!(
                buf[t * 1024..(t + 1) * 1024].iter().all(|&b| b == t as u8),
                "block {t} was interleaved"
            );
        }
    }

    #[test]
    fn is_empty_reflects_length() {
        let (_d, io) = io_at("empty");
        let f = io.open("a").unwrap();
        assert!(f.is_empty().unwrap());
        f.pwrite(0, b"x").unwrap();
        assert!(!f.is_empty().unwrap());
    }

    #[test]
    fn temp_dir_cleans_up_after_itself() {
        let path = {
            let d = TempDir::new("cleanup").unwrap();
            let p = d.path().to_path_buf();
            assert!(p.exists());
            p
        };
        assert!(!path.exists(), "TempDir did not remove itself");
    }
}
