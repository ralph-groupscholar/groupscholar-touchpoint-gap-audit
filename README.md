# Group Scholar Touchpoint Gap Audit

Group Scholar Touchpoint Gap Audit is a local-first Go CLI that scans outreach logs and flags scholars whose touchpoints are drifting beyond a target cadence. It surfaces gap tiers, program rollups, and top-risk scholars so support teams can prioritize follow-ups quickly.

## Features

- Parse outreach CSVs with flexible column naming.
- Compute gap tiers (on track, due soon, overdue, critical).
- Summarize program-level gap health and last-channel distribution.
- Emit a JSON report for downstream dashboards.

## Usage

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --top 5
```

Optional JSON output:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --json out.json
```

## CSV Format

Required columns:
- `scholar_id`
- `contact_date`

Optional columns:
- `program`
- `channel`
- `status`

Accepted date formats include `YYYY-MM-DD`, `YYYY/MM/DD`, and `MM/DD/YYYY`.

## Output Tiers

- `on_track`: gap is within cadence days
- `due_soon`: gap exceeds cadence but within cadence + due window
- `overdue`: gap exceeds due window but within 2x cadence
- `critical`: gap exceeds 2x cadence

## Tech

- Go 1.25
