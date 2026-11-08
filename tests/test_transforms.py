from order_pipeline.transforms import (
    daily_revenue,
    eligible_orders,
    flag_dq_issues,
    rejected_orders,
    standardise_orders,
)


def test_report_has_expected_columns(spark):
    raw = spark.createDataFrame(
        [("1", "C1", "2026-10-01", "10.00", "completed")],
        ["order_id", "customer_id", "order_date", "amount", "status"],
    )
    result = daily_revenue(eligible_orders(flag_dq_issues(standardise_orders(raw))))
    assert set(result.columns) == {"order_date", "revenue", "order_count"}


# Add tests that capture business rules and edge cases.
