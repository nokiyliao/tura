use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{io::Read, sync::OnceLock};

static CURRENT_EXECUTABLE_SHA256: OnceLock<String> = OnceLock::new();

pub(crate) fn current_process_start_time(pid: u32) -> Option<u64> {
    let mut system = sysinfo::System::new_all();
    system.refresh_processes();
    system
        .process(sysinfo::Pid::from_u32(pid))
        .map(sysinfo::Process::start_time)
}

pub(crate) fn current_executable_sha256() -> Result<&'static str> {
    if let Some(digest) = CURRENT_EXECUTABLE_SHA256.get() {
        return Ok(digest.as_str());
    }
    let executable = std::env::current_exe().context("resolve current router executable")?;
    let mut file = std::fs::File::open(&executable)
        .with_context(|| format!("open current router executable {}", executable.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes = file
            .read(&mut buffer)
            .with_context(|| format!("hash current router executable {}", executable.display()))?;
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }
    let digest = format!("{:x}", hasher.finalize());
    let _ = CURRENT_EXECUTABLE_SHA256.set(digest);
    CURRENT_EXECUTABLE_SHA256
        .get()
        .map(String::as_str)
        .context("cache current router executable digest")
}
