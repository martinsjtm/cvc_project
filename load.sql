COPY company_data FROM './company_data.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY expected_output FROM './expected_output.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
COPY fund_data FROM './fund_data.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
