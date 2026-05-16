# TOML Reference Doc + `examples/tomls/` + `jm doctor` — Design

**Date:** 2026-05-16
**Status:** Draft (awaiting user review)
**Topic:** Consolidated TOML format reference, exhaustive example TOMLs, and a `jm doctor` validation subcommand.

## Motivation

Today there is no single place that documents the format of the project's
TOML files. The authoritative schema lives in Rust serde structs (some in
the upstream `gaussian_job_shared` / `slurm_async_runner` crates), and the
user-facing description is scattered across `README.md`, `docs/API.md`,
`docs/architecture.md`, and the annotated `examples/simple/` fixtures.
`status.toml` is the only file with a formal schema block (in
`docs/architecture.md`). Users have no exhaustive "every field you can
set" example, and there is no automated way to validate that a `<root>`
tree is well-formed before `render`/`submit`.

This work delivers three things:

1. **`docs/toml-reference.md`** — one consolidated, field-by-field reference.
2. **`examples/tomls/`** — an exhaustive, *valid* example tree mirroring the
   real on-disk layout, showing every user-settable field.
3. **`jm doctor`** — a subcommand that validates the TOML files (and
   structural invariants) of a `<root>` tree, with an extensibility seam
   for future "preflight everything before run" checks.

## Goals

- A reader can learn the complete format of every TOML file from one doc.
- Every user-settable field has an exhaustive, copy-pasteable, *parseable*
  example.
- `jm doctor` catches malformed TOML and structural mistakes (and, run in
  CI against `examples/tomls/`, prevents the examples from rotting when
  upstream structs change).

## Non-goals (YAGNI)

- `jm doctor` does **not** check live SLURM (sbatch present, partition
  exists via `sinfo`), filesystem writability, or environment. Only the
  extensibility *seam* is built now; no preflight checks are implemented.
- No per-file split docs; no doc generator/serde round-trip generator.
- No changes to the TOML schemas themselves.

## Authoritative schema (reference, not changed by this work)

| TOML file | Rust type | Defined in | `deny_unknown_fields` |
|---|---|---|---|
| `common.toml` (root, optional) | `CommonConfig` / `DirectoryConfig` | `gaussian-job-shared2 src/config/common.rs` | yes |
| `flow.toml`, `.jm/flow.effective.toml` | `JobFlow` (+ `Job`/`JobSpec` flatten, `JobEdge`) | `gaussian-job-shared2 src/entities/workflow.rs`, `.../workflow/job.rs` | yes |
| `[jobs.*.config]` subtable | `SlurmJobConfig` | `slurm-async-runner2 src/entities/slurm/sbatch_options.rs` | yes |
| `plan.toml` | `ExperimentPlan` | `job-manager src/plan/mod.rs` | yes |
| `.jm/<JobId>/status.toml` | `JobRun` + `Lifecycle` | `job-manager src/job/run.rs`, `src/job/lifecycle.rs` | yes (`JobRun`) |

On-disk filenames are confirmed from `src/persistence/path.rs`:
`<flow_dir>/.jm/flow.effective.toml` (path.rs:72) and
`<job_dir>/status.toml` — no dot prefix (path.rs:84). The runtime
`status.toml` is reached by the `read_flow` partition-injection path
(`src/persistence/flow.rs`), which doctor reuses (it must not duplicate
parsing logic).

String-encoded nested types (no `deny_unknown_fields`; the whole value
parses from a string via custom `Visitor`): `JobTimeLimit`,
`SlurmArraySpec`, `SlurmDependency`, `ResourceSpec`, `MailTypeInput`.
`JobState`/`JobReason` are token enums used inside `[slurm_status]`.

---

## Deliverable 1 — `docs/toml-reference.md`

Single consolidated reference. Structure:

1. **Intro**
   - On-disk layout tree (`<root>/common.toml`,
     `<root>/<flow_uuid>/{flow,plan}.toml`,
     `<root>/<flow_uuid>/.jm/flow.effective.toml`,
     `<root>/<flow_uuid>/.jm/<JobId>/status.toml`).
   - User-authored vs program-written distinction (the latter is
     read-only from the user's perspective).
   - `deny_unknown_fields` ⇒ a typo'd key is a hard parse error.
   - "Authoritative schema = the Rust structs; run `cargo doc --no-deps
     --open`." Link to this doc from where the schema lives.
   - "Validate your tree with `jm doctor`."

2. **File-by-file sections**, in user-facing order:
   1. `common.toml` → `CommonConfig`
   2. `flow.toml` → `JobFlow` (+ `Job`/`JobSpec` flatten, `JobEdge`,
      `[jobs.*.config]` = `SlurmJobConfig`)
   3. `plan.toml` → `ExperimentPlan`
   4. `.jm/flow.effective.toml` → `JobFlow` (program-written, read-only,
      Cargo.lock analogue)
   5. `.jm/<JobId>/status.toml` → `JobRun` + `Lifecycle` (program-written,
      read-only)

   Each section: a field table — **TOML key | Type | Required? | Default
   | Notes** — plus the source struct + `file:line` + which repo, the
   `deny_unknown_fields` status, and a link to the matching file under
   `examples/tomls/`.

3. **SLURM config value grammars** (the part users most often get wrong):
   `time_limit`, `array_spec`, `dependency`, `resource_spec`,
   `mail_types` — accepted forms with BNF + examples — plus the
   `partition` requirement and the `common.toml` → `flow.toml`
   partition-injection / `merge_with_defaults` rules.

4. **Lifecycle / SLURM status values**: the 5 `Lifecycle` strings
   (`queued|running|success|failed|skipped`); the canonical `JobState`
   token list; `JobReason` — list common variants and state that unknown
   strings fall back to `Other(String)` (60+ variants, not all listed).

5. **Cross-links**: add a one-line pointer to this doc from `README.md`
   and `docs/API.md` (and the `docs/architecture.md` `status.toml`
   block).

Schema content is sourced from the extraction already performed against
all three crates; this spec does not restate every field — the doc is
the artifact.

---

## Deliverable 2 — `examples/tomls/`

**Style: valid + exhaustive.** Every file parses cleanly through serde
(round-trips into its struct), and every user-settable field is present
with a value. Fields that cannot coexist or are program-managed are
commented out with an explanatory note.

**Layout mirrors the real on-disk structure** (so it doubles as the
canonical layout example and is directly checkable with
`jm --root examples/tomls doctor`):

```
examples/tomls/
  README.md
  common.toml
  019xxxxxxxxx-xxxx-7xxx-xxxx-xxxxxxxxxxxx/   # UUID v7, MUST match dir name
    flow.toml
    plan.toml
    .jm/
      flow.effective.toml
      <JobId>/status.toml
```

Per-file content:

- `common.toml` — every `CommonConfig` field; `[slurm_default]` shows
  every `SlurmJobConfig` field; `[directories].project_root`.
- `flow.toml` — `uuid` (= dir name), `created_at`, `[tags]`; multiple
  jobs so that **every `JobEdge.kind`** value is demonstrated; a
  `[jobs.*.config]` with every `SlurmJobConfig` field. `partition` is set
  explicitly (the standalone example must not depend on injection).
  `resource_spec` uses the CPU form; the GPU form is shown commented.
  `dependency` is shown commented with a note that job-manager normally
  manages dependencies via `parents[]`, not this field.
- `plan.toml` — exercises the arbitrary-value map: string, int, float,
  bool, array, and nested table values, so users see the flexibility.
- `flow.effective.toml` — a materialized snapshot (defaults baked in),
  header clearly marked **PROGRAM-WRITTEN — DO NOT EDIT**.
- `status.toml` — every `JobRun` field incl. the `[slurm_status]` table;
  header clearly marked **PROGRAM-WRITTEN — DO NOT EDIT**; comments list
  the `Lifecycle` values.
- `README.md` — index: what each file is, the valid+exhaustive
  convention, editable vs reference-only, and a pointer to
  `docs/toml-reference.md`.

These files are validated in CI (see Testing).

---

## Deliverable 3 — `jm doctor`

### CLI surface

Follows the existing `jm` pattern (`src/bin/jm.rs`, clap `Subcommand`,
`--root` global, `JM_ROOT` fallback):

- `jm --root <root> doctor` — validate `<root>/common.toml` (if present)
  and **every** flow directory under `<root>`.
- `jm --root <root> doctor <flow_uuid|path>` — validate just that flow
  (plus the root `common.toml`). `<flow_uuid|path>` accepts the same
  forms as the other subcommands (`parse_target`).

`jm` is deployed `--no-default-features` on SLURM nodes; doctor is pure
serde + fs and links cleanly in that build (no pyo3).

### What it checks (this iteration)

For each in scope:

**Parse checks (FAIL on error):**
- `common.toml` → `CommonConfig` (only if the file exists; absence is OK).
- `flow.toml` → `JobFlow`, via the real `read_flow` partition-injection
  path so it validates exactly what `submit` would see.
- `plan.toml` → `ExperimentPlan`.
- `.jm/flow.effective.toml` → `JobFlow` *if present* (absence = not yet
  rendered, OK; malformed-when-present = FAIL — corruption detection).
- each `.jm/<JobId>/status.toml` → `JobRun` *if present* (same rule).

**Structural checks:**
- `flow.toml` `uuid` matches the flow directory name — **FAIL** on
  mismatch.
- every `JobEdge.from` references a `JobId` that exists in
  `flow.jobs` — **FAIL** on a dangling parent.
- `partition` resolvable for every job (set on the job or injectable from
  `common.toml`) — **FAIL** if neither (this is what `submit` would hit).
- `plan.jobs` covers every `flow.jobs` JobId — **WARN** on a missing or
  extra plan entry (an empty plan table is allowed by existing
  convention, so this is advisory, not fatal).

### Severity model

- **FAIL** — exit code non-zero; blocks (parse errors, uuid mismatch,
  dangling parent, unresolvable partition).
- **WARN** — exit code 0; advisory (plan coverage drift).
- Output: one line per check, `PASS|WARN|FAIL  <path>  <message>`, grouped
  by flow. Summary line with counts. Exit `1` iff any FAIL.

### Module shape & extensibility seam

New library module `src/doctor/` (re-exported so downstream/tests can
call it without the CLI):

- A `Check` abstraction (trait or enum of check kinds) producing a
  `Vec<Finding { severity, path, message }>`.
- A `run_doctor(resolver, scope) -> DoctorReport` orchestrator that
  enumerates flows, runs the registered checks, and aggregates findings.
- The current checks (parse + structural) are the first implementations.
- **Seam only (not implemented):** the registry is a `Vec<Box<dyn
  Check>>`-style list so future preflight checks (sbatch present,
  partition exists via `sinfo`, `project_root` writable, env sanity) can
  be appended without restructuring. A short doc-comment names these as
  the intended future extensions. No SLURM/fs/env checks are written now.

Parsing reuses `persistence::flow::read_flow` / `read_common` /
`read_flow_effective` and `job_run` readers — doctor must not
re-implement TOML parsing.

`src/bin/jm.rs` gains a `Doctor { target: Option<String> }` variant and a
thin `cmd_doctor` that calls `run_doctor` and prints the report / sets
the exit code.

---

## Testing

- **Unit tests** for each check: `#[rstest] #[case(...)]` matrices with
  good and deliberately-broken in-memory/`tempfile` fixtures (typo'd key,
  uuid mismatch, dangling parent, missing partition, plan drift),
  co-located in `src/doctor/`.
- **`tests/doctor_examples.rs`** — runs `run_doctor` against
  `examples/tomls/` and asserts **zero FAIL**. This is the drift guard:
  if an upstream struct changes incompatibly, this test (and `jm doctor`)
  goes red. It runs under the existing CI gate (`cargo test
  --all-features`), so no CI command change is needed.

## Documentation updates

- `docs/toml-reference.md` — new (Deliverable 1).
- `examples/tomls/README.md` — new (Deliverable 2).
- `README.md` — add `jm doctor` to the commands list; add a pointer to
  `docs/toml-reference.md`.
- `CLAUDE.md` — add `jm doctor` to the CLI cheatsheet; note the
  `examples/tomls/` tree and the doctor drift test.
- `docs/API.md` / `docs/development.md` — one-line pointers to the new
  reference and to `jm doctor`.

## Work breakdown (for the implementation plan)

1. `docs/toml-reference.md` (+ README/API/architecture cross-links).
2. `examples/tomls/` tree + `README.md`.
3. `src/doctor/` module: `Check` seam, parse checks, structural checks,
   `run_doctor`, findings/report types, unit tests.
4. `jm doctor` CLI wiring in `src/bin/jm.rs`.
5. `tests/doctor_examples.rs` drift guard.
6. Doc updates (`README.md`, `CLAUDE.md`, `docs/API.md`,
   `docs/development.md`).

Conventional Commits, one issue per commit, stacked PRs per project
convention.

## Risks / notes

- Keeping `examples/tomls/` *valid* against three crates is exactly why
  the doctor drift test exists; without it the examples silently rot.
- The exhaustive `flow.toml` must set `partition` explicitly (no reliance
  on injection) or `jm --root examples/tomls doctor` would depend on the
  example `common.toml` — acceptable since both exist in the tree; the
  doc must explain that real `flow.toml`s may omit it.
- `JobReason` has 60+ variants; the reference lists common ones and
  documents the `Other(String)` fallback rather than enumerating all.
