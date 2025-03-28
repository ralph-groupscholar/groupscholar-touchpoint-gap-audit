# Ralph Progress Log

## Iteration 100
- Added due-date bucket summaries to highlight upcoming outreach windows and overdue load.
- Added `--due-csv` export plus report console output for the new due buckets.
- Added tests covering due-bucket classification logic and updated documentation.

## Iteration 116
- Added `--dedupe-day` to collapse multiple same-day touchpoints per scholar without losing last-status context.
- Updated documentation with the new dedupe option.
- Added Go tests covering deduped vs raw contact cadence calculations.

## Iteration 44
- Rolled a 10 and started groupscholar-touchpoint-gap-audit, a Go CLI that audits scholar outreach logs for cadence gaps.
- Implemented CSV parsing, gap tiering, program/channel rollups, and optional JSON exports.
- Added sample data and a README with usage guidance.

## Iteration 34
- Added optional Postgres persistence for audit runs, including schema/table creation and run tagging.
- Fixed EOF handling and refreshed the README with database usage guidance.
- Seeded the production database with a sample audit run (run_id: 4b2b53f6-a2d1-46bf-961f-7ee270a502f4).

## Iteration 44 (continued)
- Added alert CSV export with configurable minimum tier for follow-up lists.
- Included first-contact dates in summaries and exports for context.
- Documented alert export usage in the README.

## Iteration 54
- Added database seeding support to initialize the Postgres schema only when empty.
- Expanded DB persistence to capture first-contact dates plus program/channel summaries.
- Documented seed workflow and audit tables in the README.

## Iteration 82
- Added optional CSV exports for program and channel summaries to support downstream reporting.
- Documented new summary export flags in the README.

## Iteration 100
- Added next-due-date and days-past-due calculations for each scholar.
- Expanded alert exports and JSON/DB persistence to include due-date fields.
- Updated schema migrations and documentation to reflect the new follow-up planning data.
