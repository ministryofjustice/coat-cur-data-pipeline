import os

mode = os.environ.get('MODE')

bucket = "s3://mojap-data-production-coat-cur-reports-v2-hourly-enriched/"
prefix_to_billing_periods = ""
path_to_partitions = bucket + prefix_to_billing_periods

# NOTE; AI usage data is delivered in a separate nested folder alongside the standard
# billing-period data, with the same schema and the same BILLING_PERIOD=YYYY-MM
# sub-partitioning. 
# 
# It will register as its own Athena table so the standard
# table is never touched.

ai_prefix = "moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/_ai_data/"
path_to_ai_partitions = bucket + ai_prefix

database_name = "cloud_optimisation_and_accountability"

if mode == "dev":
    table_name = "mojap_cur_enriched_data_dev"
    ai_table_name = "mojap_cur_enriched_data_ai_dev"
elif mode == "prod":
    table_name = "mojap_cur_enriched_data"
    ai_table_name = "mojap_cur_enriched_data_ai"
else:
    table_name = "mojap_cur_enriched_data_dev"
    ai_table_name = "mojap_cur_enriched_data_ai_dev"