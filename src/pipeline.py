import argparse
import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

STATES = ["CA", "OR", "WA"]


def read_bronze(spark, input_file):
    df = spark.read.json(input_file)

    return (
        df.select("ingest_run_id", F.explode("features").alias("feature"))
        .select("ingest_run_id", "feature.*")
    )


def transform(bronze):
    # Flatten the API response into one row per chapter.
    df = (
        bronze
        .withColumn("chapter_id", F.col("attributes.ChapterID"))
        .withColumn("chapter_name", F.col("attributes.University_Chapter"))
        .withColumn("city", F.col("attributes.City"))
        .withColumn("state", F.upper(F.trim(F.col("attributes.State"))))
        .withColumn("longitude", F.col("geometry.x").cast("double"))
        .withColumn("latitude", F.col("geometry.y").cast("double"))
    )

    df = df.filter(F.col("state").isin(STATES))

    # DQ-Q1: invalid coordinates are a hard failure.
    invalid_coordinates = (
        F.col("longitude").isNull()
        | F.col("latitude").isNull()
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
    )

    # DQ-W1: bad/missing city is a warning, not a quarantine.
    missing_city = (
        F.col("city").isNull()
        | (F.trim(F.col("city")) == "")
        | (F.upper(F.trim(F.col("city"))) == "UNKNOWN")
    )

    df = df.withColumn(
        "dq_warnings",
        F.when(
            missing_city,
            F.array(F.lit("MISSING_OR_UNKNOWN_CITY")),
        ).otherwise(F.array().cast("array<string>")),
    )

    df = df.withColumn(
        "dq_status",
        F.when(invalid_coordinates, "QUARANTINED")
        .when(missing_city, "WARNING")
        .otherwise("OK"),
    )

    df = df.withColumn(
        "dq_reason",
        F.when(invalid_coordinates, "INVALID_COORDINATES")
        .otherwise(F.lit(None).cast("string")),
    )

    quarantine = df.filter(F.col("dq_status") == "QUARANTINED")
    silver = df.filter(F.col("dq_status") != "QUARANTINED")

    # Keep one row per chapter_id.
    window = Window.partitionBy("chapter_id").orderBy("chapter_id")
    silver = (
        silver.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    gold = silver.select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "dq_status",
        "dq_warnings",
    )

    return quarantine, silver, gold


def run(input_file, output_root):
    spark = (
        SparkSession.builder
        .appName("UniversityChapters")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        bronze = read_bronze(spark, input_file)
        quarantine, silver, gold = transform(bronze)

        rows_in = bronze.count()
        rows_quarantined = quarantine.count()
        rows_warned = silver.filter(F.col("dq_status") == "WARNING").count()
        rows_ok = silver.filter(F.col("dq_status") == "OK").count()

        counts = {
            "rows_in": rows_in,
            "rows_quarantined": rows_quarantined,
            "rows_warned": rows_warned,
            "rows_ok": rows_ok,
        }

        if rows_in == 0:
            raise RuntimeError("Bronze batch is empty. Gold was not published.")

        # CA is expected to have data. OR/WA can be empty.
        ca_rows = gold.filter(F.col("state") == "CA").count()
        if ca_rows == 0:
            raise RuntimeError("CA has zero publishable rows. Please investigate the batch.")

        silver_path = os.path.join(output_root, "silver/university_chapters")
        gold_path = os.path.join(output_root, "gold/university_chapters/v1")
        quarantine_path = os.path.join(
            output_root,
            "quarantine/university_chapters",
            bronze.select("ingest_run_id").first()["ingest_run_id"],
        )       

        silver.write.mode("overwrite").parquet(silver_path)
        gold.write.mode("overwrite").parquet(gold_path)

        # Keep enough of the original row to investigate a bad record.
        quarantine.select(
            "ingest_run_id", "attributes", "geometry", "dq_reason"
        ).write.mode("overwrite").json(quarantine_path)

        log_path = os.path.join(output_root, "logs")
        os.makedirs(log_path, exist_ok=True)
        with open(
            os.path.join(log_path, "latest_run.json"), "w", encoding="utf-8"
        ) as file:
            json.dump(counts, file, indent=2)

        print(json.dumps(counts, indent=2))

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="data")
    args = parser.parse_args()
    run(args.input, args.out)


if __name__ == "__main__":
    main()
