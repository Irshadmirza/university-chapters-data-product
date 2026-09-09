from pyspark.sql import Row, SparkSession

from src.pipeline import transform


def test_dq_rules():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("university-chapters-test")
        .getOrCreate()
    )

    try:
        rows = [
            Row(
                ingest_run_id="test-run",
                attributes={
                    "ChapterID": "CA-1",
                    "University_Chapter": "Normal Chapter",
                    "City": "Los Angeles",
                    "State": "CA",
                },
                geometry=Row(x=-118.24, y=34.05),
            ),
            Row(
                ingest_run_id="test-run",
                attributes={
                    "ChapterID": "CA-2",
                    "University_Chapter": "Warning Chapter",
                    "City": "UNKNOWN",
                    "State": "CA",
                },
                geometry=Row(x=-117.16, y=32.71),
            ),
            Row(
                ingest_run_id="test-run",
                attributes={
                    "ChapterID": "CA-3",
                    "University_Chapter": "Bad Chapter",
                    "City": "Irvine",
                    "State": "CA",
                },
                geometry=Row(x=200.0, y=33.68),
            ),
        ]

        bronze = spark.createDataFrame(rows)
        quarantine, silver, gold = transform(bronze)

        assert quarantine.count() == 1
        assert quarantine.first()["dq_reason"] == "INVALID_COORDINATES"

        assert silver.filter("chapter_id = 'CA-3'").count() == 0
        assert gold.filter("chapter_id = 'CA-3'").count() == 0

        warning = gold.filter("chapter_id = 'CA-2'").first()
        assert warning["dq_status"] == "WARNING"
        assert "MISSING_OR_UNKNOWN_CITY" in warning["dq_warnings"]

    finally:
        spark.stop()
