"""Order transformations. Review correctness and test coverage."""

from pyspark.sql import DataFrame
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


def prepare_orders(orders: DataFrame) -> DataFrame:
    """Prepare raw orders for reporting."""
    return (
        orders.withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_date", F.to_date("order_date"))
        .dropDuplicates()
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
