-- staging = silver role: types, cleaning, one row per claim_id
with source as (
    select * from {{ source('insurance', 'bronze_claims') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by trim(claim_id) order by _ingested_at desc) = 1
)

select
    trim(claim_id)                                     as claim_id,
    trim(policy_id)                                    as policy_id,
    try_cast(claim_date as date)                       as claim_date,
    try_cast(claim_amount as decimal(12, 2))           as claim_amount,
    try_cast(claim_amount as decimal(12, 2)) is not null as is_amount_valid,
    coalesce(nullif(trim(claim_type), ''), 'unknown')  as claim_type,
    trim(status)                                       as status,
    _ingested_at
from deduped
