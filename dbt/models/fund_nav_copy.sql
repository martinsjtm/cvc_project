with filtered_transactions as (
    select
        *,
        row_number() over (partition by fund_name, transaction_type, transaction_date order by transaction_index desc) as rn
    from {{ source('raw', 'fund_data') }}
    where transaction_type != 'Commitment'
    qualify rn = 1
),
calls_and_distributions as (
    select 
        fund_name,
        transaction_date as call_or_distribution_date,
        transaction_amount
    from filtered_transactions
    where transaction_type in ('Call', 'Distribution')
),
valuations as (
    select 
        ft.fund_name,
        ft.transaction_date,
        ft.transaction_amount as nav
    from filtered_transactions ft
    where ft.transaction_type = 'Valuation'
),
valuations_after_calls_and_distributions as (
    select
        cd.fund_name,
        cd.call_or_distribution_date as transaction_date,
        coalesce(v.nav, 0) + cd.transaction_amount as nav
    from calls_and_distributions cd
    left join lateral (
        select nav
        from valuations v
        where v.fund_name = cd.fund_name
        and v.transaction_date <= cd.call_or_distribution_date
        order by v.transaction_date desc
        limit 1
    ) v on true
),
nav as (
select 
    fund_name,
    transaction_date,
    nav
from (
    select * from valuations
    union all
    select * from valuations_after_calls_and_distributions
)
)
select * from nav order by 1, 2






