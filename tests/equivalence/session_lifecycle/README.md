# Session Lifecycle P0-P5 Gate

This gate proves behavior rather than source text. The manifest maps each
mission phase to an executable boundary and records the protected path audit.

Run from the repository root:

```sh
tests/equivalence/session_lifecycle/run.sh
```

The gate must not update the frozen runtime/session reference. A lifecycle
change that alters an existing capture requires an explicit compatibility
decision; changing the reference merely to make the gate pass is forbidden.

The isolated E2E uses temporary `TURA_HOME`, Session DB, router, receipt, and
checkpoint roots. It must not connect to port 4126, modify the operator's
profile, or launch a remote/paid workflow.
