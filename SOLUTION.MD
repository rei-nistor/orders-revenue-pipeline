# Associate Data Engineer — PySpark Pipeline Challenge Solution

**Author:** **Andrei Nistor**

## Task 1: Run and inspect the existing pipeline.
#### Started by checking the csv file in data folder and ran it by the business rules established in README. Here's my takeaways:
**Valid row:**
- order and customer ids need to not be empty, order_date has to be a real date instead of not-a-number string, and the amount has to be positive (more than 0)
- a valid row only counts as revenue if the order is completed (a cancelled order is still correct, but not revenue)

**Unique order/row:**
- each order_id counts only once and if the same order shows up not as a duplicate (different values) it is safer to exclude and report issue

**CSV report validation row by row against rules above**
- 1001 meets all rules-> valid, counts
- 1002 order id appears twice, seems to be an exact duplicate -> valid but counts once
- 1003 has negative amount -> invalid
- 1004 has no customer id -> invalid
- 1005 has incorrect date value -> invalid
- 1006 has order cancelled -> valid but doesnt count towards revenue
- 1007 meets all rules -> valid, counts
- 1008 has no numeric value for amount -> invalid
- 1009 order appears twice but diffent amount -> conflict, exclude both because we dont know which one is correct

**Output:**
- the report is supposed to give the stakeholders the daily completed-order revenue coming from order_date, revenue and order_count, as daily_revenue() function says
- validation above proves that we only have 3 valid orders and a daily breakdown would show:
    - 2026-10-01 has 2 valid counted orders (1001,1002) with revenue of 120.50 + 85.00 = 205.50
    - 2026-10-02 has 1 valid counted order (1007) for date 10-02 with revenue of 40.00

**Problems found**
- pipeline crashes on the first bad value it tries to cast with order 1005 having invalid date value
- no validation rules to match the business rules
- stakeholders wants daily completed orders revenue but theres no status filter
- there's 2 cases of duplicated order_ids, 1 doesnt match the same amount so functions like dropDuplicates will not catch it. Need to investigate why order 1009 has a duplicate with different amount
- for precision monetary values should be decimal(14,2)
- the test passes but proves nothing, its just fixtures for checking column names exist and need real tests to matc the business rules

## Task 2: Fix incorrect transformations and explain the root causes
#### Split the prepare_orders functions into small steps, to accomodate business rules:
- standardise_orders, flag_dq_issues, eligible_orders,rejected_orders  
#### Also refactored daily_revenue to get the correct revenue and order count per day

**Result after the fix**
- uv run python -m order_pipeline.pipeline worked and we got the output report which matches the expected answer from Task 1:
    - 2026-10-01 -> 205.50, 2 orders
    - 2026-10-02 -> 40.00, 1 order


## Task 3: Improve readability, maintainability and validation
#### Split pipeline.py into small steps instead of everything in main:
- read_orders function: has its own schema and fails on a broken file 
- run function: contains the whole job with paths passed in so tests can call it
- write_parquet function: to write the report and the rejected rows in output folder
- log_summary + summarize functions: to log lines instead of doing show+ print, all counts in one pass

**Result**
- Every run logs where all the report rows are going: We have 11 order rows in, 6 orders are rejected, 1 order is an exact duplicate, 1 order not completed, and 3 orders are counted like we assumed by looking at CSV

## Task 4; Add meaningful pytest tests
#### Replaced column names fixtures test from before with actual value checking tests, for each rule established:
- new tests for checking that money is exact decimal, checking that only completed orders count, checking that invalid rows get the right dq issue, checking that duplicates count once, and checking that the conflicts are excluded
- the real csv gives the expected report, the 6 rejected rows and the log line
- If i intentionally break the CSV file it will fail instead of giving wrong numbers

**Result**
- uv run pytest -q runs succesfully with all 18 tests passed
- broke 5 rules on purpose (status filter, conflict rule, zero amount, lowercasing, money as double), and proved that tests fail from it


## Task 5 CI with tests, linter and format check
#### The started ci.yml file only ran uv sync and pytest so I extended it to add ruff for linting and formatting and configured it in pyproject.toml
- ci.yml now runs on every push and pull request with ruff check, ruff format check and then the pytests

**Result**
- Ran every command end to end from the ruff checks to the pipeline run, and checked the output against what I expected from the csv in Task 1 analysis

**output/daily_revenue** matches the expected report from Task 1
```
+----------+-------+-----------+
|order_date|revenue|order_count|
+----------+-------+-----------+
|2026-10-01| 205.50|          2|
|2026-10-02|  40.00|          1|
+----------+-------+-----------+
```

**output/rejected_orders** shows every invalid row from Task 1, with its reason
```
+--------+-----------+--------------+------------+---------+----------------------+
|order_id|customer_id|raw_order_date|raw_amount  |status   |dq_issues             |
+--------+-----------+--------------+------------+---------+----------------------+
|1003    |C03        |2026-10-02    |-45.00      |completed|[negative_amount]     |
|1004    |NULL       |2026-10-02    |150.00      |completed|[missing_customer_id] |
|1005    |C04        |not-a-date    |99.00       |pending  |[invalid_order_date]  |
|1008    |C07        |2026-10-02    |not-a-number|completed|[invalid_amount]      |
|1009    |C08        |2026-10-03    |200.00      |completed|[conflicting_order_id]|
|1009    |C08        |2026-10-03    |220.00      |completed|[conflicting_order_id]|
+--------+-----------+--------------+------------+---------+----------------------+
```