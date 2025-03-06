with
fund_company_total as (
    select
        fund_name,
        transaction_date,
        sum(transaction_amount) AS total_company_valuation
    from {{ source('raw', 'company_data') }}
    group by 1, 2
),

scaled_ownership as (
    select
        f.fund_name,
        f.transaction_date,
        f.nav as fund_nav,
        -- Avoid division by zero
        case when c.total_company_valuation = 0 THEN 0
             else f.nav / c.total_company_valuation 
        end as ownership_pct
    from {{ ref('fund_nav') }} f
    join fund_company_total c on f.fund_name = c.fund_name
                                 and f.transaction_date = c.transaction_date
),

company_nav as (
    select
        cd.fund_name,
        cd.company_name,
        cd.transaction_date,
        so.ownership_pct,
        round(sum(so.ownership_pct * cd.transaction_amount)) AS nav
    from {{ source('raw', 'company_data') }} cd
    join scaled_ownership so on so.fund_name = cd.fund_name
                                and so.transaction_date = cd.transaction_date
    where cd.company_name != 'Other Assets'
    group by 1, 2, 3, 4
)

select * from company_nav 
order by 1, 2, 3
