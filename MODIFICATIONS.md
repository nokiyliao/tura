# Modification Notice

This repository branch is a modified version of the public
[Tura-AI/tura](https://github.com/Tura-AI/tura) project. It remains distributed
under `AGPL-3.0-or-later`; see [`LICENSE`](LICENSE).

## Source Lineage

| Role | Identity |
|---|---|
| Public upstream | `https://github.com/Tura-AI/tura` |
| Exact upstream base | `91ac336a49ff3cc3250e9422167d87de0c8cffeb` |
| Modified source parent at publication | `5b8f1a720404a2c395889a4155302489fbb7b78f` |
| Modification interval | `91ac336a49ff3cc3250e9422167d87de0c8cffeb..5b8f1a720404a2c395889a4155302489fbb7b78f` |
| Commits in interval | 95 |
| Git author recorded for interval | `nokiyliao <132989292+nokiyliao@users.noreply.github.com>` |
| Publication date | 2026-08-31 |

The publication commit adds only this notice, the corresponding-source notice,
and the README disclosure above its exact modified-source parent. Git remains
the authoritative per-file and per-commit change record.

## Major Modification Families

The interval above includes substantial work in these areas:

- durable session, runtime, lifecycle, and lease ownership;
- command execution receipts and interruption recovery;
- terminalization, callback continuation, ACK, and Commander convergence;
- public child admission and canonical child dispatch;
- official Codex App Server provider integration and route validation;
- bounded startup recovery, health truth, and process cleanup;
- context/tool projection, typed failures, and no-blind-retry handling;
- deterministic integration, fault, and lifecycle tests.

These are broad categories for reviewer navigation, not a substitute for the
exact diff or test evidence.

## Evidence Boundaries

Two source identities have distinct public-review roles:

- `aab663adb87b1a72b925973073ddef2502609c4a` is the exact isolated candidate
  used by the labeled internal paired benchmark referenced by the companion
  Codex Collaboration Harness project.
- `5b8f1a720404a2c395889a4155302489fbb7b78f` is a later source lineage adding
  callback, health, ownership, recovery, and canonical child-dispatch changes.

The later source is not retroactively granted the earlier benchmark result.
Neither identity alone proves that a binary is installed or currently running.
Source, candidate artifact, installed bytes, running process, and live canary
remain separate verification states.

## Attribution and Scope

The upstream project and its contributors retain their original authorship and
license notices. This modification notice identifies the additional commit
lineage recorded under the maintainer account; it does not reassign upstream
copyright or claim affiliation with OpenAI.

No private UTM repository, task corpus, raw conversation, credential, account
payload, or protected runtime receipt is intended to be part of this public
branch. Please report a suspected disclosure through the repository security
channel before opening a public issue.
