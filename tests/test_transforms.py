from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType

from order_pipeline.pipeline import RAW_SCHEMA
from order_pipeline.transforms import (
    daily_revenue,
    eligible_orders,
    flag_dq_issues,
    rejected_orders,
    standardise_orders,
)

VALID = ("1", "C1", "2026-10-01", "10.00", "completed")


def order(**overrides):
    row = dict(zip(RAW_SCHEMA.fieldNames(), VALID, strict=True))
    return tuple({**row, **overrides}.values())


def run(spark, rows):
    flagged = flag_dq_issues(standardise_orders(spark.createDataFrame(rows, RAW_SCHEMA)))
    report = daily_revenue(eligible_orders(flagged))
    rejected = [r.dq_issues for r in rejected_orders(flagged).collect()]
    return report, rejected


def test_revenue_is_exact_decimal_money(spark):
    report, _ = run(spark, [order(order_id="1", amount="0.10"), order(order_id="2", amount="0.20")])

    assert report.schema["revenue"].dataType == DecimalType(14, 2)
    assert [tuple(r) for r in report.collect()] == [(date(2026, 10, 1), Decimal("0.30"), 2)]


@pytest.mark.parametrize(
    ("status", "counted"),
    [("completed", 1), ("Completed", 1), ("pending", 0), ("cancelled", 0)],
)
def test_only_completed_orders_count(spark, status, counted):
    report, rejected = run(spark, [order(status=status)])

    assert report.count() == counted
    assert rejected == []


@pytest.mark.parametrize(
    ("overrides", "issue"),
    [
        ({"order_id": ""}, "missing_order_id"),
        ({"customer_id": ""}, "missing_customer_id"),
        ({"order_date": "not-a-date"}, "invalid_order_date"),
        ({"order_date": "2026-02-30"}, "invalid_order_date"),
        ({"amount": "not-a-number"}, "invalid_amount"),
        ({"amount": "0.00"}, "negative_amount"),
        ({"amount": "-45.00"}, "negative_amount"),
    ],
)
def test_invalid_rows_are_rejected(spark, overrides, issue):
    report, rejected = run(spark, [order(**overrides)])

    assert report.count() == 0
    assert rejected == [[issue]]


def test_exact_duplicates_count_once(spark):
    report, rejected = run(spark, [order(), order()])

    assert report.first().order_count == 1
    assert rejected == []


@pytest.mark.parametrize("conflict", [{"amount": "22.00"}, {"status": "cancelled"}])
def test_conflicting_versions_are_all_rejected(spark, conflict):
    report, rejected = run(spark, [order(), order(**conflict)])

    assert report.count() == 0
    assert rejected == [["conflicting_order_id"], ["conflicting_order_id"]]
