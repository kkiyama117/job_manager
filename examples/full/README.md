# `examples/full/` — exhaustive, valid TOML examples

Every file here parses cleanly (round-trips through serde) **and** shows
every user-settable field. It mirrors the real on-disk layout, so
`jm --root examples/full doctor` validates it (CI does this — see
`tests/doctor_examples.rs`).

```
examples/full/
  common.toml                                  # editable — CommonConfig
  01999999-0000-7000-8000-000000000000/
    flow.toml                                  # editable — JobFlow
    plan.toml                                  # editable — ExperimentPlan
    .jm/
      flow.effective.toml                      # PROGRAM-WRITTEN — do not edit
      opt/status.toml                          # PROGRAM-WRITTEN — do not edit
```

Editable = you author it. PROGRAM-WRITTEN = job-manager generates it; shown
for reference only. Field-by-field semantics: see
[`docs/toml-reference.md`](../../docs/toml-reference.md).
