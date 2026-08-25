#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$root"

rustfmt --edition 2024 --check \
  crates/session_lifecycle/src/lib.rs \
  crates/runtime_contract/src/lib.rs \
  crates/runtime/src/checkpoint/session_snapshot.rs \
  crates/runtime/src/mano/mod.rs \
  crates/runtime/src/mano/process.rs \
  crates/runtime/src/runtime_event_writer.rs \
  crates/runtime/src/worker.rs \
  crates/router/src/daemon.rs \
  crates/router/src/runtime_dispatch.rs \
  crates/router/src/services/command_run.rs \
  crates/router/src/services/execution.rs \
  crates/session_log/src/ipc.rs \
  crates/gateway/src/session_feed.rs \
  crates/tools/src/shell_executor/execution.rs \
  crates/tools/src/shell_executor/mod.rs \
  crates/tools/src/shell_executor/process.rs \
  crates/tools/src/shell_executor/response.rs \
  crates/tools/src/shell_executor/shell.rs \
  crates/tools/src/shell_executor/tests.rs \
  tests/os_testing/session_lifecycle_e2e.rs
cargo test -q -p session_lifecycle
cargo test -q -p runtime
cargo test -q -p router
cargo test -q -p session_log
cargo test -q --features os-tests --test session_lifecycle_e2e

if command -v pwsh >/dev/null 2>&1; then
  pwsh -NoLogo -NoProfile -File tests/equivalence/runtime_session/run.ps1 -Mode gate
else
  printf '%s\n' 'BLOCKED: pwsh is required for the frozen runtime/session differential gate' >&2
  exit 2
fi
