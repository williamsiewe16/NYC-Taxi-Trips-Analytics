SELECT
    *
FROM {{ ref('fct_trips') }}
WHERE 1=1
    AND payment_type=(SELECT id FROM {{ ref('dim_payment_type') }} WHERE upper(name)='CASH')
    AND tip_amount>0