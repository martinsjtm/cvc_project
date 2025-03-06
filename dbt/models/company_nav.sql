with 
ownership as (
    select
        fund_name,
        -- handle multiple commitments (though data shows one per fund)
        sum(transaction_amount) as total_commitment_amount,
        -- assume that fund_size is static per fund
        max(fund_size),
        -- using coalesce in case there would be funds with no commitments
        coalesce(total_commitment_amount, 0)/fund_size as ownership
    from {{ source('raw', 'fund_data') }}
    where transaction_type = 'Commitment'
    group by fund_name, fund_size
),

company_nav as (
    select
        company_data.fund_name,
        company_data.company_name,
        company_data.transaction_date,
        -- ownership is constant
        ownership.ownership,
        sum(ownership.ownership * company_data.transaction_amount) as nav
    from {{ source('raw', 'company_data') }} company_data
    left join ownership on ownership.fund_name = company_data.fund_name
    group by all

)
select * from company_nav order by 1, 2, 3