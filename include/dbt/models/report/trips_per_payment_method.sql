SELECT
    Pt.name,
    count(*) AS ct
FROM {{ ref('fct_trips') }} T
    INNER JOIN {{ ref('dim_payment_type') }} Pt ON Pt.ID=T.PAYMENT_TYPE
GROUP BY Pt.name
