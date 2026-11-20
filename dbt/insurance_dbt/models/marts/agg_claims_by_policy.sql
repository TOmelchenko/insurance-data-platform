-- models/marts/agg_claims_by_policy.sql
select
    p.policy_id,
    p.product,
    count(c.claim_id)   as claims_count,
    sum(c.claim_amount) as total_claim_amount
from {{ ref('dim_policy') }} p
left join {{ ref('fct_claims') }} c on c.policy_id = p.policy_id
group by p.policy_id, p.product