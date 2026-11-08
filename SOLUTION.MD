# Associate Data Engineer — PySpark Pipeline Challenge Solution

**Author:** **Andrei Nistor**
### Status: Task 1(done)


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

