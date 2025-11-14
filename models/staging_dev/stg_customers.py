def model(dbt, session):
    customers_df = dbt.source("qwt_raw", "raw_customers")
    return customers_df