select
    customer_id,
    first_name,
    last_name,
    birth_date,
    city,
    postal_code
from {{ ref('stg_customers') }}