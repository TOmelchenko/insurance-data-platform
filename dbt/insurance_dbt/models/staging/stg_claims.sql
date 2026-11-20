select
    claim_id,
    policy_id,
    claim_date,
    claim_amount,
    claim_type,
    status
from {{ source('insurance', 'silver_claims') }}