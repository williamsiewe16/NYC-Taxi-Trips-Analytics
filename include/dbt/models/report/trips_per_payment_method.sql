SELECT
    Pt.name AS payment_method,
    count(*) AS total_trips
FROM {{ ref('fct_trips') }} T
    INNER JOIN {{ ref('dim_payment_type') }} Pt ON Pt.ID=T.PAYMENT_TYPE
GROUP BY Pt.name
