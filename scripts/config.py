import os

mode = os.environ.get('MODE')

bucket = "s3://mojap-data-production-coat-cur-reports-v2-hourly/"
prefix_to_billing_periods = "moj-cost-and-usage-reports/MOJ-CUR-V2-HOURLY/data/"
path_to_partitions = bucket + prefix_to_billing_periods
database_name = "cloud_optimisation_and_accountability"

if mode == "dev":
    table_name = "mojap_cur_data_dev"
elif mode == "prod":
    table_name = "mojap_cur_data"
else:
    table_name = "mojap_cur_data_dev"
