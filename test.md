# SQL Indentation & Formatting Style Guide

| Aspect                      | Guideline                                                                                                                                       |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| **CTEs (WITH clauses)**     | - Start each CTE on a new line<br>- Indent inside CTE by 4 spaces<br>- Use lowercase snake_case for CTE names<br>- Uppercase SQL keywords        |
| **SELECT Clause**           | - List each column/expression on a new line<br>- Indent 4 spaces<br>- Align aliases with columns                                                |
| **CASE WHEN Statements**    | - Indent each WHEN by 4 spaces relative to CASE<br>- Nested CASE indents +4 spaces per level                                                     |
| **JOINs and ON**            | - Align JOIN with FROM<br>- Indent ON conditions by 4 spaces<br>- Put each join condition on its own line                                       |
| **WHERE, GROUP BY, ORDER BY** | - Start clause on new line aligned with SELECT/FROM<br>- Indent conditions in WHERE<br>- Break long conditions into multiple lines             |
| **General Style**           | - SQL keywords uppercase<br>- Use short, consistent table aliases<br>- Minimal blank lines inside clauses<br>- Logical blocks separated<br>- Indent function params and subqueries by 4 spaces |

---

## 💡 Examples

### CTEs (WITH clauses)
```sql
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
)
```

### SELECT Clause
```sql
SELECT
    customer_id,
    order_count AS total_orders,
    CASE WHEN order_count > 10 THEN 'VIP' END AS tier
FROM customer_orders
```
### CASE WHEN Statements
```sql
CASE
    WHEN total > 100 THEN 'High'
    WHEN total > 50 THEN
        CASE
            WHEN region = 'EU' THEN 'Medium-EU'
            ELSE 'Medium'
        END
    ELSE 'Low'
END AS risk_category
```
### JOINs and ON
```sql
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
    AND o.status = 'CONFIRMED'
```
### WHERE, GROUP BY, ORDER BY
```sql
WHERE
    order_date >= CURRENT_DATE - INTERVAL 3 MONTH
    AND status IN ('SHIPPED', 'DELIVERED')
GROUP BY customer_id
ORDER BY order_date DESC
```
### General Style (nested subqueries, formatting)
```sql
SELECT
    customer_id,
    (
        SELECT MAX(order_date)
        FROM orders o
        WHERE o.customer_id = c.customer_id
    ) AS last_order_date
FROM customers c
```
