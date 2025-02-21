SELECT
    Pd.MONTH AS month_num,
    CASE Pd.MONTH
        WHEN 1 THEN 'JAN'
        WHEN 2 THEN 'FEB'
        WHEN 3 THEN 'MAR'
        WHEN 4 THEN 'APR'
        WHEN 5 THEN 'MAY'
        WHEN 6 THEN 'JUN'
        WHEN 7 THEN 'JUL'
        WHEN 8 THEN 'AUG'
        WHEN 9 THEN 'SEP'
        WHEN 10 THEN 'OCT'
        WHEN 11 THEN 'NOV'
        WHEN 12 THEN 'DEC'
    END AS month_name,
    sum(T.TOTAL_AMOUNT) AS total_amount
FROM {{ ref('fct_trips') }} T
    INNER JOIN {{ ref('dim_pickup_date') }} Pd ON Pd.DATETIME_ID=T.PICKUP_DATETIME_ID
GROUP BY Pd.MONTH
