import awswrangler as wr
import pandas as pd

# # Delete old experiements
# wr.s3.delete_objects('s3://alpha-coat-moj-cur-v2-hourly/coat_cur_test_data/') 

# # Delete incorrectly named database
# wr.catalog.delete_database(
#     name='cloud_optimisation_and_accountabilty'
# )

# # Create database: run once only
# wr.catalog.create_database(
#     name='cloud_optimisation_and_accountability'
# )

# Read data from csv file in bucket
df = wr.s3.read_csv(path='s3://alpha-coat-moj-cur-v2-hourly/cur_row_900000_to_1000000.csv')
print( df.head(10))

# Write data to parquet with partitions and create table in database
# Every time this writes it appends data in partitions, so can duplicate data 
# wr.s3.to_parquet(
#     df=df,
#     path='s3://alpha-coat-moj-cur-v2-hourly/coat_cur_test_data/test_data.parquet',
#     dataset=True,
#     partition_cols=['billing_period'],
#     database='cloud_optimisation_and_accountability',
#     table='coat_cur_test_data',
#     dtype={
#         'bill_billing_period_end_date': 'timestamp',
#         'bill_billing_period_start_date': 'timestamp',
#         'billing_period': 'string'
#     }
# )

