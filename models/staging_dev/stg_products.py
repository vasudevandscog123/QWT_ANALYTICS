def model(dbt, session):
    products_df=dbt.source("qwt_raw", "raw_products")
    return products_df