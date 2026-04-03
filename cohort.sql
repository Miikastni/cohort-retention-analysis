-- Основной расчет retention (pipeline)

WITH cohorts AS (
    SELECT
        CustomerID,
        MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month
    FROM retail_clean
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID
),

cohort_data AS (
    SELECT
        r.CustomerID,
        DATE_TRUNC('month', r.InvoiceDate) AS order_month,
        c.cohort_month
    FROM retail_clean r
    JOIN cohorts c ON r.CustomerID = c.CustomerID
),

cohort_analysis AS (
    SELECT
        CustomerID,
        cohort_month,
        order_month,
        EXTRACT(YEAR FROM age(order_month, cohort_month)) * 12 +
        EXTRACT(MONTH FROM age(order_month, cohort_month)) AS month_number
    FROM cohort_data
),

cohort_counts AS (
    SELECT
        cohort_month,
        month_number,
        COUNT(DISTINCT CustomerID) AS users_count
    FROM cohort_analysis
    GROUP BY cohort_month, month_number
),

cohort_retention AS (
    SELECT
        cohort_month,
        month_number,
        users_count,
        FIRST_VALUE(users_count) OVER (
            PARTITION BY cohort_month
            ORDER BY month_number
        ) AS cohort_size,
        ROUND(
            users_count * 100.0 /
            FIRST_VALUE(users_count) OVER (
                PARTITION BY cohort_month
                ORDER BY month_number
            ), 2
        ) AS retention_rate
    FROM cohort_counts
)

SELECT *
FROM cohort_retention
ORDER BY cohort_month, month_number;

-- Средний retention
WITH cohort_retention AS ( SELECT
        cohort_month,
        month_number,
        users_count,
        FIRST_VALUE(users_count) OVER (
            PARTITION BY cohort_month
            ORDER BY month_number
        ) AS cohort_size,
        ROUND(
            users_count * 100.0 /
            FIRST_VALUE(users_count) OVER (
                PARTITION BY cohort_month
                ORDER BY month_number
            ), 2
        ) as retention_rate
    FROM cohort_counts) 

SELECT
    month_number,
    ROUND(AVG(retention_rate), 2) AS avg_retention
FROM cohort_retention
GROUP BY month_number
ORDER BY month_number;

-- Лучшие когорты
WITH cohort_retention AS ( SELECT
        cohort_month,
        month_number,
        users_count,
        FIRST_VALUE(users_count) OVER (
            PARTITION BY cohort_month
            ORDER BY month_number
        ) AS cohort_size,
        ROUND(
            users_count * 100.0 /
            FIRST_VALUE(users_count) OVER (
                PARTITION BY cohort_month
                ORDER BY month_number
            ), 2
        ) FROM cohort_counts) 
SELECT  cohort_month, 
        ROUND(AVG(retention_rate), 2 ) AS avg_retention 
FROM cohort_retention 
WHERE month_number > 0 
GROUP BY cohort_month 
ORDER BY avg_retention DESC ;
