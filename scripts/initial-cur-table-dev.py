import awswrangler as wr

# This script is run manually from AP to create the initial table on the entire
# set of existing parquet files in the mojap-data-prod bucket

# 1. Set some variables
bucket = "s3://mojap-data-production-coat-cur-reports-v2-hourly/"
prefix_to_billing_periods = "moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/"
path_to_partitions = bucket + prefix_to_billing_periods

database_name = "cloud_optimisation_and_accountability"
table_name = "mojap_cur_data_dev_test_2"

# 2. Read metadata information to get columns_types metadata in form: {'col0': 'bigint', 'col1': 'double'}
# with dataset as true collect partitions_value
columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path_to_partitions, dataset=True)
print(columns_types) # returns dict as expected {'bill_bill_type': 'string', ...}
print(partitions_types) # returns {'BILLING_PERIOD': 'string'}

# 3. Write table; point at the .../data/ folder under which are all the partitions
wr.catalog.create_parquet_table(
    database = database_name,
    table = table_name,
    path = path_to_partitions,
    columns_types = columns_types,
    partitions_types = partitions_types,
    compression = "snappy", # pass the existing file compression type
    table_type = "EXTERNAL_TABLE",
    mode= "overwrite" # "append", or "overwrite" to recreate any possibly existing table
)

# 4. Add partitions
# Despite setting the partition value the partitions are not recognised, need to add them one by one
# 4a. Get list of objects in s3, to loop through and add partitions
s3_objects = wr.s3.list_objects(path_to_partitions)
prefix = 's3://mojap-data-production-coat-cur-reports-v2-hourly/moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/'
billing_period_list = list(set([object.removeprefix(prefix).split('/')[0] for object in s3_objects]))
billing_period_list.sort()
print(billing_period_list) # returns ['BILLING_PERIOD=2022-04',...,'BILLING_PERIOD=2025-08']

# 4b. Create dictionary of partitions to add
partitions_values = {}
for billing_period in billing_period_list:
    key = prefix + billing_period + "/"
    value = [billing_period.split("=")[1]]
    partitions_values[key] = value

print(partitions_values)

# 4c. Add partitions
wr.catalog.add_parquet_partitions(
    database=database_name,
    table=table_name,
    partitions_values=partitions_values
)

# 4d. Check partitions is non-empty
partitions_list = wr.catalog.get_partitions(
    database=database_name,
    table=table_name,
)
print(f"Number of partitions added: {len(partitions_list)}")
