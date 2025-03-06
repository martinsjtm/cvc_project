CREATE TABLE company_data(fund_name VARCHAR, company_id DOUBLE, company_name VARCHAR, transaction_type VARCHAR, transaction_index DOUBLE, transaction_date DATE, transaction_amount DOUBLE, sector VARCHAR, country VARCHAR, region VARCHAR);
CREATE TABLE expected_output("Fund Name" VARCHAR, Date DATE, NAV DOUBLE);
CREATE TABLE fund_data(fund_name VARCHAR, fund_size DOUBLE, transaction_type VARCHAR, transaction_index DOUBLE, transaction_date DATE, transaction_amount DOUBLE, sector VARCHAR, country VARCHAR, region VARCHAR);

