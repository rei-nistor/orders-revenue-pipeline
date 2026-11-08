"""Order transformations. Review correctness and test coverage."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pyspark.sql.types import DecimalType

COMPLETED = "completed"  # revenue needs completed orders
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


def daily_revenue(orders: DataFrame) -> DataFrame:
    """Calculate daily completed-order revenue."""
    return (
        orders.groupBy("order_date")
        .agg(
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy("order_date")
    )
