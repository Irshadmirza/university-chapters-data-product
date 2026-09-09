# Data Product Contract - University Chapters v1

## 1. Product

**Name:** University Chapters

**Owner:** Data Engineering

**Consumers:** Analytics and reporting users that need university chapter information.

## 2. Interface

**Gold path:** `gold/university_chapters/v1/`

**Format:** Parquet

**Grain:** one row per `chapter_id`

| Column | Type | Description |
|---|---|---|
| chapter_id | string | Stable key from `ChapterID` |
| chapter_name | string | Chapter display name |
| city | string | City from the source |
| state | string | CA, OR or WA |
| longitude | double | WGS84 longitude |
| latitude | double | WGS84 latitude |
| dq_status | string | `OK` or `WARNING` |
| dq_warnings | array<string> | Warning reason codes |

`OBJECTID` is kept as technical/source information and is not part of the Gold contract.

## 3. Freshness

The intended refresh is daily by **06:00 UTC**. This take-home is manually run, so scheduling is not included.

## 4. Data quality

### DQ-Q1 - invalid coordinates

A row fails when longitude or latitude is missing, null, non-numeric, or outside these ranges:

- longitude: -180 to 180
- latitude: -90 to 90

The row is written to quarantine with reason:

`INVALID_COORDINATES`

It must not appear in Silver or Gold.

### DQ-W1 - missing city

A row gets a warning when city is null, blank or `UNKNOWN`, ignoring case.

The row is still published with:

```text
dq_status = WARNING
MISSING_OR_UNKNOWN_CITY in dq_warnings
```

Clean rows have `dq_status = OK` and no warnings.

### Batch checks

- CA, OR and WA are always the requested scope.
- OR or WA can have zero rows.
- A completely empty source batch is treated as a failure.
- CA having zero publishable rows is treated as a failure/investigation condition.
- Quarantined rows must never appear in Gold.

Each run records:

`rows_in`, `rows_quarantined`, `rows_warned`, `rows_ok`

## 5. Versioning

Breaking changes will use a new version such as `gold/university_chapters/v2/`. This keeps the v1 consumer contract stable.

## 6. Classification

Public source data. No PII is expected.
