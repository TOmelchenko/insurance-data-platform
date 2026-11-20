{{ config(materialized='table') }}

select
    policy_id,
    count(claim_id) as claims_count,
    sum(claim_amount) as total_claim_amount
from {{ ref('stg_claims') }}
group by policy_id