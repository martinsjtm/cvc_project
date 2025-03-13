with filtered_transactions as (
    select
        *,
        row_number() over (partition by fund_name, transaction_type, transaction_date order by transaction_index desc) as rn
    from {{ source('raw', 'fund_data') }}
    where transaction_type != 'Commitment'
    -- we assume that there is only one valid transaction per fund, transaction_type, transaction_date 
    -- the transaction with the greatest transaction_index is the one that counts even if its format seems unusual for one of the records.
    qualify rn = 1
),

calls_and_distributions as (
    select 
        fund_name,
        transaction_date as call_or_distribution_date,
        -- just in case there would be more than one Call/Distribution in the same day we aggregate the total amount
        sum(transaction_amount) as total_transaction_amount
    from filtered_transactions
    where transaction_type in ('Call', 'Distribution')
    group by all
),

valuations as (
    select 
        ft.fund_name,
        ft.transaction_date,
        ft.transaction_index,
        ft.transaction_amount as nav
    from filtered_transactions ft
    where ft.transaction_type = 'Valuation'
),

nav_after_calls_and_distributions as (
    select
        cd.fund_name,
        cd.call_or_distribution_date as transaction_date,
        coalesce(v.nav, 0) + cd.total_transaction_amount as nav
    from calls_and_distributions cd
    left join valuations v on v.fund_name = cd.fund_name 
    -- Allow same-day transactions by using <= instead of <
    where v.transaction_date <= cd.call_or_distribution_date
    -- we are only interested in applying calls and distributions to the latest valuation
    qualify row_number() over (partition by cd.fund_name order by v.transaction_date desc, v.transaction_index desc) = 1
),

nav as (
    select * exclude(transaction_index) from valuations
    union all
    select * from nav_after_calls_and_distributions
)

select * from nav order by 1, 2






