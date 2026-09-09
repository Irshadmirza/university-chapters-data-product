# University Chapters - Data Engineer Assignment

This project is my solution for the University Chapters Azure Medallion Data Product assignment.

I kept the implementation fairly small because the main things I wanted to show are:

- getting the data from the ArcGIS API
- keeping the original response in Bronze
- using PySpark for the Silver/Gold transformations
- handling the two required data quality cases differently
- publishing a simple Gold dataset that another team can use

## Project structure

```text
university-chapters-data-product/
│
├── fixtures/
│   └── sample_bad_and_warning.json
├── src/
│   ├── __init__.py
│   ├── ingest.py
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
├── docs/
│   ├── architecture.md
│   └── data-product-contract.md
├── requirements.txt
├── .gitignore
└── README.md
```

The `data/` folder is created when the pipeline runs and is ignored by Git.

## Tools used

- Python
- PySpark
- requests
- Parquet for Silver/Gold output
- JSON for the raw Bronze payload

The folder structure follows an ADLS-style layout. I used local files for the take-home so that the reviewer can run it without needing an Azure subscription.

## Source

The source is the public ArcGIS FeatureServer given in the assignment:

`https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query`

Only CA, OR and WA are requested.

## Setup

I used Python 3.10+.

Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows:

```text
.venv\Scripts\activate
```

Install the packages:

```bash
pip install -r requirements.txt
```

PySpark also needs a compatible Java installation.

## Run the sample data first

I included a small fixture so the two DQ cases can be tested without depending on the live API.

Run ingestion using the fixture:

```bash
python -m src.ingest --fixture fixtures/sample_bad_and_warning.json
```

This creates a Bronze file under:

```text
data/bronze/university_chapters/<run_id>/payload.json
```

Then run the Spark transformation using that file:

```bash
python -m src.pipeline --input data/bronze/university_chapters/<run_id>/payload.json
```

Replace `<run_id>` with the folder printed by the ingestion command.

The sample has:

- one normal CA row
- one CA row with `UNKNOWN` city
- one CA row with an invalid longitude
- one normal OR row

So the expected DQ result is:

```text
rows_in = 4
rows_quarantined = 1
rows_warned = 1
rows_ok = 2
```

The invalid coordinate row goes to quarantine and does not appear in Silver or Gold. The `UNKNOWN` city row is kept and is marked as `WARNING`.

## Run tests

```bash
pytest -q
```

The test checks both required DQ paths and verifies that the bad coordinate row does not reach Gold.

## Run with the live API

```bash
python -m src.ingest
```

Then use the printed Bronze path with:

```bash
python -m src.pipeline --input data/bronze/university_chapters/<run_id>/payload.json
```

The ingestion fails if the API returns an error or no features. The pipeline also stops instead of publishing an apparently successful empty Gold batch.

OR and WA having zero records is allowed. CA is expected to have data based on the assignment, so a zero CA result is treated as something that needs investigation.

## Output

After a successful run the main folders are:

```text
data/
├── bronze/university_chapters/<run_id>/payload.json
├── silver/university_chapters/
├── gold/university_chapters/v1/
├── quarantine/university_chapters/<run_id>/
└── logs/latest_run.json
```

Gold contains only the consumer-facing columns:

```text
chapter_id
chapter_name
city
state
longitude
latitude
dq_status
dq_warnings
```

## DQ rules

### DQ-Q1 - invalid coordinates

If longitude or latitude is missing, cannot be converted to a number, or is outside the valid range, the row is quarantined with:

`INVALID_COORDINATES`

The raw attributes and geometry are kept in quarantine so that the row can be investigated later.

### DQ-W1 - missing city

If city is null, blank or `UNKNOWN`, the row is still published.

It gets:

```text
dq_status = WARNING
dq_warnings = ["MISSING_OR_UNKNOWN_CITY"]
```

Clean rows have `dq_status = OK` and an empty warning list.

## A few design decisions

**Bronze:** I kept the API response close to the original format and added the run ID. This gives a simple history by run and means the transformation can be rerun without calling the API again.

**Silver:** This is where I flatten the API structure, cast the coordinates, filter the states and apply the DQ rules.

**Gold:** I only publish the fields that are part of the data product contract. `OBJECTID` is kept in the landed data but is not exposed to consumers.

**Idempotency:** For this local assignment, Silver and Gold are overwritten on each run. Bronze keeps separate run folders. In production I would use Delta tables and consider a `MERGE` by `chapter_id`.

## Azure mapping

If this were moved to Azure, the local folders could be mapped to ADLS Gen2 paths and the PySpark code could run in Databricks, Fabric or Synapse Spark. The API is public, so there is no secret or credential in this project.

## What I would add for production

- scheduled orchestration and alerts
- retries for API failures
- API pagination if the source grows
- Delta tables/schema enforcement
- more DQ checks for missing IDs and duplicates
- CI checks and more tests
- monitoring of row counts and freshness
