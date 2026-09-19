//! Shared process-wide and cross-process resources for integration tests.
//!
//! Test binaries launched by Cargo are separate processes, so an in-process
//! mutex cannot coordinate their network resources.  The port allocator below
//! uses create-new lease files in the system temporary directory.  A lease is
//! retained until the test process exits; abandoned leases are reclaimed when
//! their owner PID is no longer alive.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Mutex, OnceLock};

const FIRST_PORT: u16 = 20_000;
const LAST_PORT: u16 = 59_999;

static NEXT_PORT: AtomicU16 = AtomicU16::new(FIRST_PORT);
static LEASES: OnceLock<Mutex<Vec<PortLease>>> = OnceLock::new();

/// Allocate a loopback TCP port and retain its cross-process lease.
pub fn allocate_port() -> io::Result<u16> {
    let lease = PortLease::acquire()?;
    let port = lease.port;
    LEASES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .map_err(|_| io::Error::other("test port lease registry is poisoned"))?
        .push(lease);
    Ok(port)
}

/// Return a diagnostic when a child process has exited unexpectedly.
///
/// Readiness loops should call this before retrying a connection.  A failed
/// bind is a process-start failure, not a transient unavailable service.
pub fn child_exit_message(child: &mut Child, context: &str) -> Option<String> {
    match child.try_wait() {
        Ok(Some(status)) => Some(format!(
            "{context} exited before becoming ready with status {status}"
        )),
        Err(error) => Some(format!("{context} could not be inspected: {error}")),
        Ok(None) => None,
    }
}

struct PortLease {
    port: u16,
    path: PathBuf,
    _marker: File,
}

impl PortLease {
    fn acquire() -> io::Result<Self> {
        let directory = lease_directory()?;
        fs::create_dir_all(&directory)?;
        let current_pid = std::process::id();
        let start = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        let span = u32::from(LAST_PORT) - u32::from(FIRST_PORT) + 1;

        for offset in 0..span {
            let port =
                FIRST_PORT + ((u32::from(start) - u32::from(FIRST_PORT) + offset) % span) as u16;
            let path = directory.join(format!("{port}.lease"));
            let marker = match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut marker) => {
                    writeln!(marker, "{current_pid}")?;
                    marker
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    reclaim_dead_lease(&path, current_pid);
                    continue;
                }
                Err(error) => return Err(error),
            };

            if TcpListener::bind(("127.0.0.1", port)).is_ok() {
                return Ok(Self {
                    port,
                    path,
                    _marker: marker,
                });
            }

            drop(marker);
            let _ = fs::remove_file(path);
        }

        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no coordinated loopback test ports are available",
        ))
    }
}

impl Drop for PortLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn lease_directory() -> io::Result<PathBuf> {
    if let Some(path) = std::env::var_os("CLUSTODIAN_TEST_PORT_DIR") {
        return Ok(PathBuf::from(path));
    }
    Ok(std::env::temp_dir().join("clustodian-test-port-leases-v1"))
}

fn reclaim_dead_lease(path: &Path, current_pid: u32) {
    let Ok(mut file) = File::open(path) else {
        return;
    };
    let mut contents = String::new();
    if file.read_to_string(&mut contents).is_err() {
        return;
    }
    let Ok(owner_pid) = contents.trim().parse::<u32>() else {
        return;
    };
    if owner_pid != current_pid && !process_is_alive(owner_pid) {
        let _ = fs::remove_file(path);
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    // The test suite's supported CI hosts are Unix.  On other platforms,
    // retaining an abandoned lease is safer than stealing a live one.
    true
}
