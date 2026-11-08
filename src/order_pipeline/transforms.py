"""Order transformations. Review correctness and test coverage."""

from pyspark.sql import Column, DataFrame, Row, Window
from pyspark.sql import functions as F

from pyspark.sql.types import DecimalType

RAW_COLUMNS = ("order_id", "customer_id", "order_date", "amount", "status")

COMPLETED = "completed"  # revenue needs completed orders
KNOWN_STATUSES = ("completed", "pending", "cancelled")  # all recognized order statuses
DATE_FORMAT = "yyyy-MM-dd"  # matching csv format
MONEY = DecimalType(14, 2)  # precision for monetary values
DQ_ISSUES = "dq_issues"  # column for data quality issues from broken rules

# Data quality issue types
MISSING_ORDER_ID = "missing_order_id"
MISSING_CUSTOMER_ID = "missing_customer_id"
INVALID_ORDER_DATE = "invalid_order_date"
INVALID_AMOUNT = "invalid_amount"
NEGATIVE_AMOUNT = "negative_amount"
CONFLICTING_ORDER_ID = "conflicting_order_id"

# all rule names in one tuple for logging purposes
DQ_RULES = (
    MISSING_ORDER_ID,
    MISSING_CUSTOMER_ID,
    INVALID_ORDER_DATE,
    INVALID_AMOUNT,
    NEGATIVE_AMOUNT,
    CONFLICTING_ORDER_ID,
)

# Defining an order
ORDER_DETAILS = ("customer_id", "order_date", "amount", "status")


# replaced original prepare_orders function because it was trying to do 3 things at once: type casting, date conversion, and deduplication
def empty_to_null(name: str) -> Column:
    """an empty value becomes NULL"""
    return F.nullif(F.trim(F.col(name)), F.lit(""))


def standardise_orders(raw: DataFrame) -> DataFrame:
    """same standard for every column to avoid bad input"""

    return raw.select(
        empty_to_null("order_id").alias("order_id"),
        empty_to_null("customer_id").alias("customer_id"),
        F.try_to_date(empty_to_null("order_date"), DATE_FORMAT).alias("order_date"),
        empty_to_null("amount").try_cast(MONEY).alias("amount"),
        F.lower(empty_to_null("status")).alias("status"),
        F.col("order_date").alias("raw_order_date"),
        F.col("amount").alias("raw_amount"),
    )


def dq_rules() -> dict[str, Column]:
    """establishing matching issues for each business rule with breaking conditions"""
    versions = F.collect_set(F.struct(*ORDER_DETAILS)).over(
        Window.partitionBy("order_id")
    )

    return {
        MISSING_ORDER_ID: F.col("order_id").isNull(),
        MISSING_CUSTOMER_ID: F.col("customer_id").isNull(),
        INVALID_ORDER_DATE: F.col("order_date").isNull(),  # bad dates became NULL
        INVALID_AMOUNT: F.col("amount").isNull(),  # bad amounts became NULL
        NEGATIVE_AMOUNT: F.col("amount") <= 0,  # negative amounts are invalid
        CONFLICTING_ORDER_ID: F.col("order_id").isNotNull() & (F.size(versions) > 1),
    }


def flag_dq_issues(standardised: DataFrame) -> DataFrame:
    """detect data quality issues for each row"""
    issues = [F.when(broken, F.lit(name)) for name, broken in dq_rules().items()]
    return standardised.withColumn(DQ_ISSUES, F.array_compact(F.array(*issues)))


def valid_order() -> Column:
    """A valid order is one that breaks no rule"""
    return F.size(DQ_ISSUES) == 0


def completed_order() -> Column:
    """Status filter needed for completed orders"""
    return F.col("status") == COMPLETED


def eligible_orders(flagged: DataFrame) -> DataFrame:
    """Valid and completed orders, each order_id appears once"""
    return (
        flagged.filter(valid_order() & completed_order())
        .select("order_id", "order_date", "amount")
        .dropDuplicates(["order_id"])
    )


def rejected_orders(flagged: DataFrame) -> DataFrame:
    """Defining rejected orders as bad rows with dq issues."""
    return flagged.filter(~valid_order()).select(
        "order_id",
        "customer_id",
        "raw_order_date",
        "raw_amount",
        "status",
        DQ_ISSUES,
    )


def daily_revenue(orders: DataFrame) -> DataFrame:
    """Calculate daily completed order revenue."""
    return (
        orders.groupBy("order_date")
        .agg(
            F.sum("amount").cast(MONEY).alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy("order_date")
    )


def summarize(flagged: DataFrame) -> Row:
    """Summary of rejected and completed orders."""
    valid = valid_order()
    valid_ids = F.count_distinct(F.when(valid, F.col("order_id")))
    counted_ids = F.count_distinct(F.when(valid & completed_order(), F.col("order_id")))
    known_status = F.coalesce(F.col("status").isin(*KNOWN_STATUSES), F.lit(False))
    return flagged.agg(
        F.count("*").alias("rows_in"),
        F.count_if(~valid).alias("rejected"),
        (F.count_if(valid) - valid_ids).alias("exact_duplicates"),
        (valid_ids - counted_ids).alias("not_completed"),
        counted_ids.alias("counted"),
        *[F.count_if(F.array_contains(DQ_ISSUES, r)).alias(r) for r in DQ_RULES],
        *[F.count_if(F.col("status") == s).alias(s) for s in KNOWN_STATUSES],
        F.count_if(~known_status).alias("unknown_status"),
    ).first()
