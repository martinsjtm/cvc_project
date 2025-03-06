WITH fund_nav AS (
  SELECT fund_name, transaction_date, nav 
  FROM {{ ref('fund_nav') }}
),
company_agg AS (
  SELECT
    fund_name,
    transaction_date,
    SUM(transaction_amount) AS total_company_val
  FROM {{ source('raw', 'company_data') }}
  WHERE transaction_type = 'Valuation'
    AND company_name != 'Other Assets' -- Exclude non-companies
  GROUP BY 1, 2
),
ownership AS (
  SELECT
    fn.fund_name,
    fn.transaction_date,
    -- Handle division by zero (unlikely here, but safe)
    CASE 
      WHEN ca.total_company_val = 0 THEN 0 
      ELSE fn.nav / ca.total_company_val 
    END AS ratio
  FROM fund_nav fn
  JOIN company_agg ca 
    ON fn.fund_name = ca.fund_name 
    AND fn.transaction_date = ca.transaction_date -- Align dates
),
scaled_nav AS (
  SELECT
    c.company_name,
    c.transaction_date,
    c.transaction_amount * o.ratio AS nav_contribution
  FROM company_data c
  JOIN ownership o 
    ON c.fund_name = o.fund_name 
    AND c.transaction_date = o.transaction_date
  WHERE c.transaction_type = 'Valuation'
    AND c.company_name != 'Other Assets' -- Exclude non-companies
)
-- Aggregate contributions from all funds per company-date
SELECT
  company_name,
  transaction_date,
  SUM(nav_contribution) AS NAV
FROM scaled_nav
GROUP BY 1, 2
ORDER BY 1, 2