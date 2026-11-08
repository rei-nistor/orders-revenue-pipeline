import logging
from datetime import date
from decimal import Decimal

import pytest

from order_pipeline.pipeline import ROOT, read_orders, run

SAMPLE = ROOT / "data" / "orders.csv"
HEADER = "order_id,customer_id,order_date,amount,status\n"


def test_sample_file_gives_the_expected_report(spark, tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="order_pipeline.pipeline")
    report, rejected = run(spark, SAMPLE, tmp_path)

    assert [tuple(r) for r in report.collect()] == [
        (date(2026, 10, 1), Decimal("205.50"), 2),
        (date(2026, 10, 2), Decimal("40.00"), 1),
    ]
    assert sorted(r.order_id for r in rejected.collect()) == [
        "1003",
        "1004",
        "1005",
        "1008",
        "1009",
        "1009",
    ]
    assert spark.read.parquet(str(tmp_path / "rejected_orders")).count() == 6
    assert (
        "Rows in=11 = rejected=6 + exact_duplicates=1 + not_completed=1 + counted=3" in caplog.text
    )


@pytest.mark.parametrize(
    "content",
    [
        "order_id,customer_id,amount,order_date,status\n1,C1,10.00,2026-10-01,completed\n",
        HEADER + "1,C1,2026-10-01,10.00,completed,extra\n",
    ],
    ids=["reordered-header", "extra-field"],
)
def test_broken_file_fails_instead_of_shifting_values(spark, tmp_path, content):
    path = tmp_path / "orders.csv"
    path.write_text(content)
    with pytest.raises(Exception, match="CSV header does not conform|MALFORMED_CSV_RECORD"):
        read_orders(spark, path).collect()
