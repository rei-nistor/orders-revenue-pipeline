"""Run with: python -m order_pipeline.pipeline"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from order_pipeline.transforms import (
    daily_revenue,
    DQ_RULES,
    eligible_orders,
    flag_dq_issues,
    KNOWN_STATUSES,
    RAW_COLUMNS,
    rejected_orders,
    standardise_orders,
    summarize,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]

RAW_SCHEMA = StructType([StructField(name, StringType()) for name in RAW_COLUMNS])


def read_orders(spark: SparkSession, path: Path) -> DataFrame:
    """Read the orders CSV"""
    return (
        spark.read.schema(RAW_SCHEMA)
        .options(
            header=True,
            enforceSchema=False,
            mode="FAILFAST",
        )
        .csv(str(path))
    )


def write_parquet(df: DataFrame, path: Path) -> None:
    """Write as Parquet"""
    df.write.mode("overwrite").parquet(str(path))


def log_summary(summary: Row) -> None:
    """Logging the DQ issues and the statuses"""
    logger.info(
        "Rows in=%s = rejected=%s + exact_duplicates=%s + not_completed=%s + counted=%s",
        summary["rows_in"],
        summary["rejected"],
        summary["exact_duplicates"],
        summary["not_completed"],
        summary["counted"],
    )
    logger.info(
        "Rejected rows by DQ issue: %s",
        ", ".join(f"{r}={summary[r]}" for r in DQ_RULES),
    )
    logger.info("Statuses: %s", ", ".join(f"{s}={summary[s]}" for s in KNOWN_STATUSES))
    if summary["unknown_status"]:
        logger.warning("%s rows have an unknown status", summary["unknown_status"])


def run(
    spark: SparkSession, input_path: Path, output_dir: Path
) -> tuple[DataFrame, DataFrame]:
    """Build report and quarantine"""
    flagged = flag_dq_issues(standardise_orders(read_orders(spark, input_path)))
    flagged.cache()
    report = daily_revenue(eligible_orders(flagged))
    rejected = rejected_orders(flagged)

    write_parquet(report, output_dir / "daily_revenue")
    write_parquet(rejected, output_dir / "rejected_orders")
    log_summary(summarize(flagged))
    logger.info("Wrote the report and the rejected rows to %s", output_dir)
    return report, rejected


def main() -> None:
    # configured logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("associate-order-assignment")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark, ROOT / "data" / "orders.csv", ROOT / "output")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
