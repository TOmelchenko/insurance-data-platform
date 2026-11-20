with source as (
    select * from {{ source('insurance', 'bronze_policies') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by trim(policy_id) order by _ingested_at desc) = 1
)

select
    trim(policy_id)                              as policy_id,
    trim(customer_id)                            as customer_id,
    trim(product)                                as product,
    try_cast(start_date as date)                 as start_date,
    try_cast(annual_premium as decimal(10, 2))   as annual_premium,
    trim(status)                                 as status,
    _ingested_at
from deduped
