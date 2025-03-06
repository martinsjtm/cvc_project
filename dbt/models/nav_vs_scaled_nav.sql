select 
    s.fund_name,
    s.company_name,
    s.transaction_date,
    s.nav AS nav_static,
    s.ownership as ownership_static,
    d.nav AS nav_dynamic,
    d.ownership_pct as ownership_dynamic,
    (d.nav - s.nav) AS nav_delta,
    round(nav_delta/nav_static, 3) * 100 as percentual_difference
from {{ ref('company_nav') }} s
join {{ ref('company_nav_scaled') }} d on s.company_name = d.company_name 
                                          and s.fund_name = d.fund_name
                                          and s.transaction_date = d.transaction_date
order by 1, 2, 3