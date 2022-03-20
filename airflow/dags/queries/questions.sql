CREATE TABLE IF NOT EXISTS cayena.analytics.results 
AS WITH 
q1 AS (
    SELECT
        rate,
        AVG(price) AS avg_price
    FROM cayena.analytics.books
    GROUP BY rate),
q2 AS (
    SELECT 
        count(*) AS qnt_books,
        max(created) AS stock_date
    FROM cayena.analytics.books
    WHERE stock <= 2
)
SELECT * FROM q1, q2;