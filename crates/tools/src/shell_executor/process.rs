#![allow(unsafe_code)]

#[cfg(windows)]
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellProcessScopeStrategy {
    WindowsJobObject,
    UnixProcessGroup,
    DirectChildOnly,
}

pub fn current_shell_process_scope_strategy() -> ShellProcessScopeStrategy {
    if cfg!(windows) {
        ShellProcessScopeStrategy::WindowsJobObject
    } else if cfg!(unix) {
        ShellProcessScopeStrategy::UnixProcessGroup
    } else {
        ShellProcessScopeStrategy::DirectChildOnly
    }
}

pub(super) fn configure_process_scope(command: &mut Command) {
    configure_platform_spawn(command);
}

pub(super) fn configure_tokio_process_scope(command: &mut tokio::process::Command) {
    command.kill_on_drop(true);
    configure_tokio_platform_spawn(command);
}

pub(super) fn attach_shell_process_scope(
    pid: u32,
    owner_scope: Option<&str>,
) -> Option<ShellProcessScope> {
    ShellProcessScope::attach(pid, owner_scope)
}

pub(super) fn terminate_process_tree(pid: u32) {
    terminate_platform_process_tree(pid);
}

pub(super) fn process_is_alive(pid: u32) -> bool {
    process_is_alive_platform(pid)
}

pub(super) fn panic_cleanup_process_scope_empty(
    owner_scope: &str,
    pid: u32,
) -> Result<bool, String> {
    #[cfg(not(windows))]
    let _ = owner_scope;
    #[cfg(unix)]
    unsafe {
        return Ok(kill(-(pid as i32), 0) != 0);
    }
    #[cfg(windows)]
    {
        let mut proofs = windows_panic_scope_proofs()
            .lock()
            .expect("Windows panic scope proof registry poisoned");
        let key = (owner_scope.to_string(), pid);
        let empty = proofs.get(&key).copied().ok_or_else(|| {
            format!("WINDOWS_JOB_OBJECT_PANIC_CLEANUP_PROOF_MISSING:{owner_scope}:{pid}")
        })?;
        if empty {
            proofs.remove(&key);
        }
        return Ok(empty);
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(format!("PANIC_CLEANUP_PROCESS_SCOPE_UNSUPPORTED:{pid}"))
    }
}

#[cfg(windows)]
fn windows_panic_scope_proofs() -> &'static Mutex<HashMap<(String, u32), bool>> {
    static PROOFS: OnceLock<Mutex<HashMap<(String, u32), bool>>> = OnceLock::new();
    PROOFS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn retain_shell_process_scope(scope: ShellProcessScope, owner_scope: Option<&str>) {
    if !scope.has_live_members() {
        return;
    }
    retained_shell_process_scopes()
        .lock()
        .expect("retained shell process scope registry poisoned")
        .push(RetainedShellProcessScope {
            owner_scope: owner_scope.map(str::to_string),
            process_scope: scope,
        });
}

pub fn terminate_retained_shell_process_scopes() -> usize {
    let scopes = retained_shell_process_scopes()
        .lock()
        .expect("retained shell process scope registry poisoned")
        .drain(..)
        .collect::<Vec<_>>();
    let count = scopes.len();
    for scope in &scopes {
        scope.process_scope.terminate();
    }
    count
}

pub fn terminate_retained_shell_process_scopes_for_scope(owner_scope: &str) -> usize {
    let mut scopes = retained_shell_process_scopes()
        .lock()
        .expect("retained shell process scope registry poisoned");
    let (matching, retained) = std::mem::take(&mut *scopes)
        .into_iter()
        .partition::<Vec<_>, _>(|scope| {
            owner_scope_matches(scope.owner_scope.as_deref(), owner_scope)
        });
    *scopes = retained;
    drop(scopes);
    let count = matching.len();
    for scope in &matching {
        scope.process_scope.terminate();
    }
    count
}

pub fn retained_shell_process_scope_count() -> usize {
    let mut scopes = retained_shell_process_scopes()
        .lock()
        .expect("retained shell process scope registry poisoned");
    scopes.retain(|scope| scope.process_scope.has_live_members());
    scopes.len()
}

pub fn retained_shell_process_scope_count_for_scope(owner_scope: &str) -> usize {
    let mut scopes = retained_shell_process_scopes()
        .lock()
        .expect("retained shell process scope registry poisoned");
    scopes.retain(|scope| scope.process_scope.has_live_members());
    scopes
        .iter()
        .filter(|scope| owner_scope_matches(scope.owner_scope.as_deref(), owner_scope))
        .count()
}

fn owner_scope_matches(stored: Option<&str>, requested: &str) -> bool {
    stored == Some(requested)
}

#[derive(Debug)]
struct RetainedShellProcessScope {
    owner_scope: Option<String>,
    process_scope: ShellProcessScope,
}

fn retained_shell_process_scopes() -> &'static Mutex<Vec<RetainedShellProcessScope>> {
    static SCOPES: OnceLock<Mutex<Vec<RetainedShellProcessScope>>> = OnceLock::new();
    SCOPES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(windows)]
fn terminate_platform_process_tree(pid: u32) {
    let children = collect_descendant_processes(pid);
    for child_pid in children.into_iter().rev() {
        terminate_process(child_pid);
    }
    terminate_process(pid);
}

#[cfg(windows)]
fn collect_descendant_processes(root_pid: u32) -> Vec<u32> {
    use std::collections::{HashMap, HashSet};
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
        TH32CS_SNAPPROCESS,
    };

    let mut children_by_parent: HashMap<u32, Vec<u32>> = HashMap::new();
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            return Vec::new();
        }
        let mut entry = std::mem::zeroed::<PROCESSENTRY32W>();
        entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
        if Process32FirstW(snapshot, &mut entry) != 0 {
            loop {
                children_by_parent
                    .entry(entry.th32ParentProcessID)
                    .or_default()
                    .push(entry.th32ProcessID);
                if Process32NextW(snapshot, &mut entry) == 0 {
                    break;
                }
            }
        }
        CloseHandle(snapshot);
    }

    let mut seen = HashSet::new();
    let mut stack = children_by_parent
        .get(&root_pid)
        .cloned()
        .unwrap_or_default();
    let mut descendants = Vec::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        descendants.push(pid);
        if let Some(children) = children_by_parent.get(&pid) {
            stack.extend(children.iter().copied());
        }
    }
    descendants
}

#[cfg(windows)]
fn terminate_process(pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess};

    unsafe {
        let process = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if process.is_null() {
            return;
        }
        let _ = TerminateProcess(process, 1);
        CloseHandle(process);
    }
}

#[cfg(windows)]
fn process_is_alive_platform(_pid: u32) -> bool {
    // Recovery stays fail-closed until a native process-handle probe is added.
    true
}

#[cfg(unix)]
fn terminate_platform_process_tree(pid: u32) {
    unsafe {
        let _ = kill(-(pid as i32), SIGTERM);
        let _ = kill(-(pid as i32), SIGKILL);
    }
}

#[cfg(not(any(unix, windows)))]
fn terminate_platform_process_tree(_pid: u32) {}

#[cfg(unix)]
fn process_is_alive_platform(pid: u32) -> bool {
    unsafe { kill(pid as i32, 0) == 0 }
}

#[cfg(not(any(unix, windows)))]
fn process_is_alive_platform(_pid: u32) -> bool {
    true
}

#[cfg(windows)]
#[derive(Debug)]
pub(super) struct ShellProcessScope {
    job: windows_sys::Win32::Foundation::HANDLE,
    pid: u32,
    owner_scope: Option<String>,
}

#[cfg(windows)]
unsafe impl Send for ShellProcessScope {}

#[cfg(windows)]
unsafe impl Sync for ShellProcessScope {}

#[cfg(windows)]
impl ShellProcessScope {
    fn attach(pid: u32, owner_scope: Option<&str>) -> Option<Self> {
        use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_SET_QUOTA, PROCESS_TERMINATE,
        };

        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return None;
            }

            let mut info = std::mem::zeroed::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let configured = SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            if configured == 0 {
                CloseHandle(job);
                return None;
            }

            let process: HANDLE = OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid);
            if process.is_null() {
                CloseHandle(job);
                return None;
            }

            let assigned = AssignProcessToJobObject(job, process);
            CloseHandle(process);
            if assigned == 0 {
                CloseHandle(job);
                return None;
            }

            Some(Self {
                job,
                pid,
                owner_scope: owner_scope.map(str::to_string),
            })
        }
    }

    pub(super) fn terminate(&self) {
        unsafe {
            windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1);
        }
    }

    pub(super) fn has_live_members(&self) -> bool {
        use windows_sys::Win32::System::JobObjects::{
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
            QueryInformationJobObject,
        };

        unsafe {
            let mut info = std::mem::zeroed::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>();
            let queried = QueryInformationJobObject(
                self.job,
                JobObjectBasicAccountingInformation,
                &mut info as *mut _ as *mut _,
                std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            );
            queried != 0 && info.ActiveProcesses > 0
        }
    }
}

#[cfg(windows)]
impl Drop for ShellProcessScope {
    fn drop(&mut self) {
        let had_live_members = self.has_live_members();
        let mut empty = !had_live_members;
        if had_live_members {
            self.terminate();
            let started = std::time::Instant::now();
            while self.has_live_members() && started.elapsed() < std::time::Duration::from_secs(5) {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            empty = !self.has_live_members();
            if let Some(owner_scope) = self.owner_scope.as_ref() {
                windows_panic_scope_proofs()
                    .lock()
                    .expect("Windows panic scope proof registry poisoned")
                    .insert((owner_scope.clone(), self.pid), empty);
            }
        }
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.job);
        }
    }
}

#[cfg(unix)]
#[derive(Debug)]
pub(super) struct ShellProcessScope {
    pgid: i32,
}

#[cfg(unix)]
impl ShellProcessScope {
    fn attach(pid: u32, _owner_scope: Option<&str>) -> Option<Self> {
        Some(Self { pgid: pid as i32 })
    }

    pub(super) fn terminate(&self) {
        unsafe {
            let _ = kill(-self.pgid, SIGTERM);
            let _ = kill(-self.pgid, SIGKILL);
        }
    }

    pub(super) fn has_live_members(&self) -> bool {
        unsafe { kill(-self.pgid, 0) == 0 }
    }
}

#[cfg(unix)]
impl Drop for ShellProcessScope {
    fn drop(&mut self) {
        if self.has_live_members() {
            self.terminate();
        }
    }
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug)]
pub(super) struct ShellProcessScope;

#[cfg(not(any(unix, windows)))]
impl ShellProcessScope {
    fn attach(_pid: u32, _owner_scope: Option<&str>) -> Option<Self> {
        None
    }

    pub(super) fn terminate(&self) {}

    pub(super) fn has_live_members(&self) -> bool {
        false
    }
}

#[cfg(windows)]
fn configure_platform_spawn(command: &mut Command) {
    tura_path::process_hardening::hide_child_console_window_and_create_group(command);
}

#[cfg(unix)]
fn configure_platform_spawn(command: &mut Command) {
    command.process_group(0);
    configure_parent_death_signal(command);
}

#[cfg(not(any(unix, windows)))]
fn configure_platform_spawn(_command: &mut Command) {}

#[cfg(windows)]
fn configure_tokio_platform_spawn(command: &mut tokio::process::Command) {
    command.creation_flags(
        tura_path::process_hardening::WINDOWS_CREATE_NO_WINDOW
            | tura_path::process_hardening::WINDOWS_CREATE_NEW_PROCESS_GROUP,
    );
}

#[cfg(unix)]
fn configure_tokio_platform_spawn(command: &mut tokio::process::Command) {
    command.process_group(0);
    configure_tokio_parent_death_signal(command);
}

#[cfg(not(any(unix, windows)))]
fn configure_tokio_platform_spawn(_command: &mut tokio::process::Command) {}

#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(unix)]
const SIGTERM: i32 = 15;

#[cfg(target_os = "linux")]
const PR_SET_PDEATHSIG: i32 = 1;

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn prctl(option: i32, arg2: usize, arg3: usize, arg4: usize, arg5: usize) -> i32;
    fn getppid() -> i32;
}

#[cfg(target_os = "linux")]
fn configure_parent_death_signal(command: &mut Command) {
    let parent_pid = std::process::id() as i32;
    unsafe {
        command.pre_exec(move || {
            if prctl(PR_SET_PDEATHSIG, SIGTERM as usize, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if getppid() != parent_pid {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "shell parent died before command exec",
                ));
            }
            Ok(())
        });
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn configure_parent_death_signal(_command: &mut Command) {}

#[cfg(target_os = "linux")]
fn configure_tokio_parent_death_signal(command: &mut tokio::process::Command) {
    let parent_pid = std::process::id() as i32;
    unsafe {
        command.pre_exec(move || {
            if prctl(PR_SET_PDEATHSIG, SIGTERM as usize, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if getppid() != parent_pid {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "shell parent died before async command exec",
                ));
            }
            Ok(())
        });
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn configure_tokio_parent_death_signal(_command: &mut tokio::process::Command) {}

#[cfg(test)]
mod tests {
    use super::{ShellProcessScopeStrategy, current_shell_process_scope_strategy};

    #[test]
    fn shell_process_scope_strategy_matches_current_platform() {
        let strategy = current_shell_process_scope_strategy();
        if cfg!(windows) {
            assert_eq!(strategy, ShellProcessScopeStrategy::WindowsJobObject);
        } else if cfg!(unix) {
            assert_eq!(strategy, ShellProcessScopeStrategy::UnixProcessGroup);
        } else {
            assert_eq!(strategy, ShellProcessScopeStrategy::DirectChildOnly);
        }
    }

    #[test]
    fn shell_process_scope_strategy_contract_covers_all_os_families() {
        let strategies = [
            ShellProcessScopeStrategy::WindowsJobObject,
            ShellProcessScopeStrategy::UnixProcessGroup,
            ShellProcessScopeStrategy::DirectChildOnly,
        ];
        assert!(strategies.contains(&ShellProcessScopeStrategy::WindowsJobObject));
        assert!(strategies.contains(&ShellProcessScopeStrategy::UnixProcessGroup));
        assert!(strategies.contains(&ShellProcessScopeStrategy::DirectChildOnly));
    }

    #[test]
    fn async_shell_scope_enables_kill_on_drop() {
        let mut command = tokio::process::Command::new("missing-test-binary");
        assert!(!command.get_kill_on_drop());
        super::configure_tokio_process_scope(&mut command);
        assert!(command.get_kill_on_drop());
    }

    #[test]
    fn retained_process_ownership_is_session_scoped() {
        assert!(super::owner_scope_matches(Some("session-a"), "session-a"));
        assert!(!super::owner_scope_matches(Some("session-b"), "session-a"));
        assert!(!super::owner_scope_matches(None, "session-a"));
    }

    #[cfg(unix)]
    #[test]
    fn retained_process_cleanup_is_exactly_owner_scoped() {
        let mut first = std::process::Command::new("/bin/sh");
        first.args(["-c", "sleep 60"]);
        super::configure_process_scope(&mut first);
        let mut first = first.spawn().expect("first retained process");
        let first_scope = super::attach_shell_process_scope(first.id(), None).expect("first scope");
        super::retain_shell_process_scope(first_scope, Some("owner-a"));

        let mut second = std::process::Command::new("/bin/sh");
        second.args(["-c", "sleep 60"]);
        super::configure_process_scope(&mut second);
        let mut second = second.spawn().expect("second retained process");
        let second_scope =
            super::attach_shell_process_scope(second.id(), None).expect("second scope");
        super::retain_shell_process_scope(second_scope, Some("owner-b"));

        assert_eq!(
            super::retained_shell_process_scope_count_for_scope("owner-a"),
            1
        );
        assert_eq!(
            super::retained_shell_process_scope_count_for_scope("owner-b"),
            1
        );
        assert_eq!(
            super::terminate_retained_shell_process_scopes_for_scope("owner-a"),
            1
        );
        first.wait().expect("first retained process reaped");
        assert_eq!(
            super::retained_shell_process_scope_count_for_scope("owner-a"),
            0
        );
        assert_eq!(
            super::retained_shell_process_scope_count_for_scope("owner-b"),
            1
        );
        assert!(second.try_wait().expect("second status").is_none());

        assert_eq!(
            super::terminate_retained_shell_process_scopes_for_scope("owner-b"),
            1
        );
        second.wait().expect("second retained process reaped");
        assert_eq!(
            super::retained_shell_process_scope_count_for_scope("owner-b"),
            0
        );
    }
}
