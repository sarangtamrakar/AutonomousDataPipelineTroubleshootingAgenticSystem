
CREATE EXTERNAL TABLE dim_customer 
LOCATION 's3://sarang-de/gold/dim_customers/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE dim_product 
LOCATION 's3://sarang-de/gold/dim_products/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE fact_sales 
LOCATION 's3://sarang-de/gold/fact_sales/'
TBLPROPERTIES ('table_type' = 'DELTA');