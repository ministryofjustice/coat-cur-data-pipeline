import awswrangler as wr
import pandas as pd

## Dev test script to work out tablifying of parquet files 

# 1. Some useful paths
bucket = "s3://mojap-data-production-coat-cur-reports-v2-hourly/"
prefix_to_billing_periods = "moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/"
test_billing_period = "BILLING_PERIOD=2025-08/"
test_billing_period_with_single_parquet = "BILLING_PERIOD=2025-08/MOJ-CUR-V2-HOURLY-00132.snappy.parquet"

path_to_partitions = bucket + prefix_to_billing_periods
path_to_single_partition = bucket + prefix_to_billing_periods + test_billing_period
path_to_single_parquet = bucket + prefix_to_billing_periods + test_billing_period_with_single_parquet

# 2. Read data from a single PARQUET file in a single billing period
# path = bucket + prefix_to_billing_periods + test_billing_period_with_single_parquet
# df = wr.s3.read_parquet(path=path)
# print( df.head(10))

# 3. Read meta data information to get column_types metadata in form: {'col0': 'bigint', 'col1': 'double'}
# 3a. Pull column types form the billing period level, with dataset as true also collects the partition value
# columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path_to_partitions, dataset=True)
# print(columns_types) # returns dict as expected {'bill_bill_type': 'string', ...}
# print(partitions_types) # returns {'BILLING_PERIOD': 'string'}

# 3b. Pull metadata from a single partition of parquets; no parittion info at this level
# columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path_to_single_partition, dataset=True)
# print(columns_types) # returns dict as expected {'bill_bill_type': 'string', ...}
# print(partitions_types) # returns None

# 3c. Pull metadata from a single parquet file; no partition info at this level
# columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path_to_single_parquet, dataset=False)
# print(columns_types) # returns dict as expected {'bill_bill_type': 'string', ...}
# print(partitions_types) # returns None

# # Metadata Values collected from 3a.
# columns_types = {'bill_bill_type': 'string', 'bill_billing_entity': 'string', 'bill_billing_period_end_date': 'timestamp', 'bill_billing_period_start_date': 'timestamp', 'bill_invoice_id': 'string', 'bill_invoicing_entity': 'string', 'bill_payer_account_id': 'string', 'bill_payer_account_name': 'string', 'cost_category': 'map<string,string>', 'discount': 'map<string,double>', 'discount_bundled_discount': 'double', 'discount_total_discount': 'double', 'identity_line_item_id': 'string', 'identity_time_interval': 'string', 'line_item_availability_zone': 'string', 'line_item_blended_cost': 'double', 'line_item_blended_rate': 'string', 'line_item_currency_code': 'string', 'line_item_legal_entity': 'string', 'line_item_line_item_description': 'string', 'line_item_line_item_type': 'string', 'line_item_net_unblended_cost': 'double', 'line_item_net_unblended_rate': 'string', 'line_item_normalization_factor': 'double', 'line_item_normalized_usage_amount': 'double', 'line_item_operation': 'string', 'line_item_product_code': 'string', 'line_item_resource_id': 'string', 'line_item_tax_type': 'string', 'line_item_unblended_cost': 'double', 'line_item_unblended_rate': 'string', 'line_item_usage_account_id': 'string', 'line_item_usage_account_name': 'string', 'line_item_usage_amount': 'double', 'line_item_usage_end_date': 'timestamp', 'line_item_usage_start_date': 'timestamp', 'line_item_usage_type': 'string', 'pricing_currency': 'string', 'pricing_lease_contract_length': 'string', 'pricing_offering_class': 'string', 'pricing_public_on_demand_cost': 'double', 'pricing_public_on_demand_rate': 'string', 'pricing_purchase_option': 'string', 'pricing_rate_code': 'string', 'pricing_rate_id': 'string', 'pricing_term': 'string', 'pricing_unit': 'string', 'product': 'map<string,string>', 'product_comment': 'string', 'product_fee_code': 'string', 'product_fee_description': 'string', 'product_from_location': 'string', 'product_from_location_type': 'string', 'product_from_region_code': 'string', 'product_instance_family': 'string', 'product_instance_type': 'string', 'product_instancesku': 'string', 'product_location': 'string', 'product_location_type': 'string', 'product_operation': 'string', 'product_pricing_unit': 'string', 'product_product_family': 'string', 'product_region_code': 'string', 'product_servicecode': 'string', 'product_sku': 'string', 'product_to_location': 'string', 'product_to_location_type': 'string', 'product_to_region_code': 'string', 'product_usagetype': 'string', 'reservation_amortized_upfront_cost_for_usage': 'double', 'reservation_amortized_upfront_fee_for_billing_period': 'double', 'reservation_availability_zone': 'string', 'reservation_effective_cost': 'double', 'reservation_end_time': 'string', 'reservation_modification_status': 'string', 'reservation_net_amortized_upfront_cost_for_usage': 'double', 'reservation_net_amortized_upfront_fee_for_billing_period': 'double', 'reservation_net_effective_cost': 'double', 'reservation_net_recurring_fee_for_usage': 'double', 'reservation_net_unused_amortized_upfront_fee_for_billing_period': 'double', 'reservation_net_unused_recurring_fee': 'double', 'reservation_net_upfront_value': 'double', 'reservation_normalized_units_per_reservation': 'string', 'reservation_number_of_reservations': 'string', 'reservation_recurring_fee_for_usage': 'double', 'reservation_reservation_a_r_n': 'string', 'reservation_start_time': 'string', 'reservation_subscription_id': 'string', 'reservation_total_reserved_normalized_units': 'string', 'reservation_total_reserved_units': 'string', 'reservation_units_per_reservation': 'string', 'reservation_unused_amortized_upfront_fee_for_billing_period': 'double', 'reservation_unused_normalized_unit_quantity': 'double', 'reservation_unused_quantity': 'double', 'reservation_unused_recurring_fee': 'double', 'reservation_upfront_value': 'double', 'resource_tags': 'map<string,string>', 'savings_plan_amortized_upfront_commitment_for_billing_period': 'double', 'savings_plan_end_time': 'string', 'savings_plan_instance_type_family': 'string', 'savings_plan_net_amortized_upfront_commitment_for_billing_period': 'double', 'savings_plan_net_recurring_commitment_for_billing_period': 'double', 'savings_plan_net_savings_plan_effective_cost': 'double', 'savings_plan_offering_type': 'string', 'savings_plan_payment_option': 'string', 'savings_plan_purchase_term': 'string', 'savings_plan_recurring_commitment_for_billing_period': 'double', 'savings_plan_region': 'string', 'savings_plan_savings_plan_a_r_n': 'string', 'savings_plan_savings_plan_effective_cost': 'double', 'savings_plan_savings_plan_rate': 'double', 'savings_plan_start_time': 'string', 'savings_plan_total_commitment_to_date': 'double', 'savings_plan_used_commitment': 'double', 'split_line_item_actual_usage': 'double', 'split_line_item_net_split_cost': 'double', 'split_line_item_net_unused_cost': 'double', 'split_line_item_parent_resource_id': 'string', 'split_line_item_public_on_demand_split_cost': 'double', 'split_line_item_public_on_demand_unused_cost': 'double', 'split_line_item_reserved_usage': 'double', 'split_line_item_split_cost': 'double', 'split_line_item_split_usage': 'double', 'split_line_item_split_usage_ratio': 'double', 'split_line_item_unused_cost': 'double'}
# partitions_types = {'BILLING_PERIOD': 'string'}
# # partitions_types = {'billing_period': 'string'}

# # 4. Write a table from one billing period
# database_name = "cloud_optimisation_and_accountability"
# table_name = "mojap_cur_data_dev_test_1"
# billing_period = "BILLING_PERIOD=2025-08"

# wr.catalog.create_parquet_table(
#     database = database_name,
#     table = table_name,
#     path = path_to_single_partition,
#     columns_types = columns_types,
#     partitions_types = partitions_types,
#     compression = 'snappy', # pass the existing file compression type
#     # table_type = table_type,
#     # mode='append' # 'overwrite' to recreate any possibly existing table
# )

# ---------------------------------------------------
# # 5. Write entire table; point at the .../data/ folder under which are all the partitions 
# database_name = "cloud_optimisation_and_accountability"
# table_name = "mojap_cur_data_dev_test_2"
# wr.catalog.create_parquet_table(
#     database = database_name,
#     table = table_name,
#     path = path_to_partitions,
#     columns_types = columns_types,
#     partitions_types = partitions_types,
#     compression = "snappy", # pass the existing file compression type
#     table_type = "EXTERNAL_TABLE",
#     mode= "overwrite" # "append" or "overwrite" to recreate any possibly existing table
# )

# # Get list of objects in s3, to loop through and add partitions
# s3_objects = wr.s3.list_objects(path_to_partitions)
# prefix = 's3://mojap-data-production-coat-cur-reports-v2-hourly/moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/'
# billing_period_list = list(set([object.removeprefix(prefix).split('/')[0] for object in s3_objects]))
# billing_period_list.sort()
# print(billing_period_list) # returns ['BILLING_PERIOD=2022-04',...,'BILLING_PERIOD=2025-08']

# # Create dictionary of partitions to add
# partitions_values = {}
# for billing_period in billing_period_list:
#     key = prefix + billing_period + "/"
#     value = [billing_period.split("=")[1]]
#     partitions_values[key] = value

# print(partitions_values)

# # Add partitions

# wr.catalog.add_parquet_partitions(
#     database=database_name,
#     table=table_name,
#     partitions_values=partitions_values
# )

# # Get partitions to check
# partitions_list = wr.catalog.get_partitions(
#     database=database_name,
#     table=table_name,
# )
# print(partitions_list) # returns 0

# Our partition prefixes are perhaps not correct as we have multiple 'subfolders' before we arrive at the partition
# Something is not right with how the table is configured
# Try wr.catalog.add_parquet_partitions

# # Repair table idea: this writes to the source s3 bucket though - do NOT want to do that!!!
# update_partitions = wr.athena.repair_table(
#     table = table_name,
#     database = database_name,
#     s3_output = path_to_single_partition
# )
# print(update_partitions)


# -----------------------------------
# # Delete table if exists
# database_name = "cloud_optimisation_and_accountability"
# table_name = "mojap_cur_data_dev_test_2"
# wr.catalog.delete_table_if_exists(database=database_name, table=table_name)

# Notes ------------------------------------------
# # Parquet files are organised into separate 'folders' per billing period, each billing period location holds many parquet files.
# s3_path_to_parquet = "s3://mojap-data-production-coat-cur-reports-v2-hourly/moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/BILLING_PERIOD=2022-04/"
# database_name = "cloud_optimisation_and_accountability"
# table_name = "mojap_cur_data_dev"
# table_type = "EXTERNAL_TABLE"
# partition_col = "billing_period"
# use_partition_projection = False

# meta = wr.s3.read_parquet_metadata(path=s3_path_to_parquet, dataset=True)
# column_types = {}
# column_types = meta.column_types
# partition_types = {partition_col: "string"}

# wr.catalog.create_parquet_table(
#     database = database_name,
#     table = table_name,
#     path = s3_path_to_parquet,
#     column_types = column_types,
#     partitions_types = {partition_col: "string"},
#     table_type = table_type,
#     mode='append' # 'overwrite' to recreate any possibly existing table
# )
