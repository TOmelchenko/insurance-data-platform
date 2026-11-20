with source as (
    select * from {{ source('insurance', 'bronze_customers') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by trim(customer_id) order by _ingested_at desc) = 1
)

select
    trim(customer_id)              as customer_id,
    trim(first_name)               as first_name,
    trim(last_name)                as last_name,
    try_cast(birth_date as date)   as birth_date,
    trim(city)                     as city,
    trim(postal_code)              as postal_code,
    _ingested_at
from deduped
