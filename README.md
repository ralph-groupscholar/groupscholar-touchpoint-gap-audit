# Group Scholar Touchpoint Gap Audit

Group Scholar Touchpoint Gap Audit is a local-first Go CLI that scans outreach logs and flags scholars whose touchpoints are drifting beyond a target cadence. It surfaces gap tiers, program rollups, and top-risk scholars so support teams can prioritize follow-ups quickly.

## Features

- Parse outreach CSVs with flexible column naming.
- Compute gap tiers (on track, due soon, overdue, critical).
- Summarize program-level gap health and last-channel distribution.
- Capture engagement tempo metrics (average interval, contacts per month).
- Emit a JSON report for downstream dashboards.
- Export alert-ready CSVs for overdue and critical follow-ups, including next due dates.
- Provide due-date bucket summaries for upcoming outreach planning.
- Ignore future-dated touchpoints relative to `--as-of` and report how many were skipped.
- Optionally dedupe multiple contacts on the same day per scholar.

## Usage

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --top 5
```

Optional JSON output:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --json out.json
```

Optional alert CSV output (overdue+ by default):

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --alerts alerts.csv
```

Include due-soon alerts too:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --alerts alerts.csv --min-tier due_soon
```

Program and channel summary CSVs:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --programs-csv programs.csv --channels-csv channels.csv
```

Program and status summary CSVs:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --programs-csv programs.csv --statuses-csv statuses.csv
```

Due-date bucket CSV:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --due-csv due-buckets.csv
```

Deduplicate multiple contacts logged on the same day:

```bash
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --dedupe-day
```

Alert exports include `next_due_date`, `days_past_due`, and engagement tempo fields (`avg_interval_days`, `contacts_per_month`).

## Database storage

Store audit runs in Postgres for longitudinal tracking.

```bash
export TOUCHPOINT_GAP_AUDIT_DB_URL="postgres://user:pass@host:port/dbname"
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --db --db-tag "weekly-touchpoints"
```

Initialize the schema and seed it (only if empty):

```bash
export TOUCHPOINT_GAP_AUDIT_DB_URL="postgres://user:pass@host:port/dbname"
go run . --input sample/touchpoints.csv --as-of 2026-02-07 --cadence 30 --init-db
```

Tables are created in the `touchpoint_gap_audit` schema by default. Override with `--db-schema`. Stored tables include `audit_runs`, `audit_scholar_gaps` (with tempo fields like avg interval and contacts per month), `audit_program_summary`, and `audit_channel_summary`.

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
