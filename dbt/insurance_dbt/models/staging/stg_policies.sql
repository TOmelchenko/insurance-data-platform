select
    policy_id,
    customer_id,
    product,
    start_date,
    annual_premium,
    status
from {{ source('insurance', 'silver_policies') }}