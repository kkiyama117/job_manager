# job-manager docs

| Doc | Purpose |
|---|---|
| [architecture.md](./architecture.md) | Module map, on-disk layout, data flow, lifecycle model, Pyclass Single Owner rule |
| [development.md](./development.md) | Setup, build, test, format, stub generation, common pitfalls |
| [references/orchestration-systems.md](./references/orchestration-systems.md) | Airflow / Prefect vocabulary alignment that informs the SP-3 design |
| `superpowers/specs/2026-05-12-job-manager-sp1-design.md` | SP-1 design spec (data layer scope) |
| `superpowers/specs/2026-05-12-job-manager-sp2-design.md` | SP-2 design spec (plan + jobid helpers) |
| `superpowers/specs/2026-05-13-job-manager-sp3-design.md` | SP-3 v1 design spec (superseded by v2) |
| `superpowers/specs/2026-05-13-job-manager-sp3-rearch-design.md` | **SP-3 v2 design spec (current)** — FlowRun/JobRun/Lifecycle/Executor/Querier/FlowRunner + `jm` CLI |
| `superpowers/plans/2026-05-12-job-manager-sp1.md` | SP-1 TDD implementation plan |
| `superpowers/plans/2026-05-12-job-manager-sp2.md` | SP-2 TDD implementation plan |
| `superpowers/plans/2026-05-13-job-manager-sp3.md` | SP-3 v1 implementation plan (superseded) |
| `superpowers/plans/2026-05-13-job-manager-sp3-rearch.md` | **SP-3 v2 implementation plan (current)** |

For a one-page overview of capabilities, the `jm` CLI, and the Python
async API, see the top-level [`../README.md`](../README.md).
