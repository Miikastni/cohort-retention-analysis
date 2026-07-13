WITH cohort_days AS (SELECT 
       card,
       datetime,
       first_value(datetime) OVER (PARTITION BY card ORDER BY datetime)::date AS first_purchases_date,
       datetime::date - first_value(datetime::date) OVER (PARTITION BY card ORDER BY datetime) AS days_from_first,
       date_trunc('month', first_value(datetime) OVER (PARTITION BY card ORDER BY datetime))::date AS cohort,
       summ_with_disc 
FROM checks
WHERE card LIKE '2000%' 
    AND summ_with_disc > 0
    AND datetime BETWEEN '2000-01-01' AND CURRENT_DATE
)
SELECT 
     cohort,
     max(days_from_first) AS max_days_from_first,
     count(DISTINCT card) AS customers_count,
     round(sum(CASE WHEN days_from_first = 0 THEN summ_with_disc end) / count(DISTINCT card), 2) AS "first_purchases",
    round (sum(CASE WHEN days_from_first >= 1 and days_from_first <= 30 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 1 and days_from_first <= 30 then card end), 2) AS "1-30_day",
    round (sum(CASE WHEN days_from_first >= 31 AND days_from_first <= 60 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 31 AND days_from_first <= 60 then card end), 2) AS "31-60_day",
    round (sum(CASE WHEN days_from_first >= 61 AND days_from_first <= 90 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 61 AND days_from_first <= 90 then card end), 2) AS "61-90_day",
    round (sum(CASE WHEN days_from_first >= 91 AND days_from_first <= 120 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 91 AND days_from_first <= 120 then card end), 2) AS "91-120_day",
    round (sum(CASE WHEN days_from_first >= 121 AND days_from_first <= 150 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 121 AND days_from_first <= 150 then card end), 2) AS "121-150_day",
    round (sum(CASE WHEN days_from_first >= 151 AND days_from_first <= 180 then summ_with_disc end) / count(DISTINCT CASE WHEN days_from_first >= 151 AND days_from_first <= 180 then card end), 2) AS "151-180_day"
FROM cohort_days  
WHERE cohort NOT IN ('2021-07-01', '2022-06-01')
GROUP BY cohort
ORDER BY cohort
