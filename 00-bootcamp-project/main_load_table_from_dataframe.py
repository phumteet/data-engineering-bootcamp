# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

# import json
# import os
# from datetime import datetime

# import pandas as pd
# from google.cloud import bigquery
# from google.oauth2 import service_account


# keyfile = os.environ.get("KEYFILE_PATH")
# service_account_info = json.load(open(keyfile))
# credentials = service_account.Credentials.from_service_account_info(service_account_info)
# project_id = "braided-destiny-384416"
# client = bigquery.Client(
#     project=project_id,
#     credentials=credentials,
# )

# job_1_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
#     ],
#     time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="created_at",
#     ),
#     clustering_fields=["first_name", "last_name"],
# )

# file_path1 = "./data/users.csv"
# df1 = pd.read_csv(file_path1, parse_dates=["created_at", "updated_at"])
# df1.info()

# table_id1 = f"{project_id}.deb_bootcamp.users"
# job1 = client.load_table_from_dataframe(df1, table_id1, job_config=job_1_config)
# job1.result()

# job_2_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
#     ],
#     time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="created_at",
#     ),
#     # clustering_fields=["first_name", "last_name"],
# )

# file_path2 = "./data/events.csv"
# df2 = pd.read_csv(file_path2, parse_dates=["created_at"])
# df2.info()

# table_id2 = f"{project_id}.deb_bootcamp.events"
# job2 = client.load_table_from_dataframe(df2, table_id2, job_config=job_2_config)
# job2.result()

# job_3_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT64),
#         bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT64),
#         bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT64),
#         bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
#     ],
#     time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="created_at",
#     ),
#     # clustering_fields=["first_name", "last_name"],
# )

# file_path3 = "./data/orders.csv"
# df3 = pd.read_csv(file_path3, parse_dates=["created_at", "estimated_delivery_at", "delivered_at"])
# df3.info()

# table_id3 = f"{project_id}.deb_bootcamp.orders"
# job3 = client.load_table_from_dataframe(df3, table_id3, job_config=job_3_config)
# job3.result()

# # ----------------- No partotioning --------------- 

# job_4_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.INT64),
#         bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
#     ],
# )

# file_path4 = "./data/addresses.csv"
# df4 = pd.read_csv(file_path4, dtype={'zipcode': 'Int64'})
# df4.info()

# table_id4 = f"{project_id}.deb_bootcamp.addresses"
# job4 = client.load_table_from_dataframe(df4, table_id4, job_config=job_4_config)
# job4.result()

# job_5_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("quantity", bigquery.SqlTypeNames.INT64),
#     ],
# )

# file_path5 = "./data/order_items.csv"
# df5 = pd.read_csv(file_path5, dtype={'zipcode': 'Int64'})
# df5.info()

# table_id5 = f"{project_id}.deb_bootcamp.order_items"
# job5 = client.load_table_from_dataframe(df5, table_id5, job_config=job_5_config)
# job5.result()

# job_6_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT64),
#         bigquery.SchemaField("inventory", bigquery.SqlTypeNames.INT64),
#     ],
# )

# file_path6 = "./data/products.csv"
# df6 = pd.read_csv(file_path6, dtype={'price': 'Float64', 'inventory': 'Int64'})
# df6.info()

# table_id6 = f"{project_id}.deb_bootcamp.products"
# job6 = client.load_table_from_dataframe(df6, table_id6, job_config=job_6_config)
# job6.result()

# job_7_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("discount", bigquery.SqlTypeNames.INT64),
#         bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
#     ],
# )

# file_path7 = "./data/promos.csv"
# df7 = pd.read_csv(file_path7, dtype={'discount': 'Int64'})
# df7.info()

# table_id7 = f"{project_id}.deb_bootcamp.promos"
# job7 = client.load_table_from_dataframe(df7, table_id7, job_config=job_7_config)
# job7.result()

# table1 = client.get_table(table_id1)
# table2 = client.get_table(table_id2)
# table3 = client.get_table(table_id3)
# table4 = client.get_table(table_id4)
# table5 = client.get_table(table_id5)
# table6 = client.get_table(table_id6)
# table7 = client.get_table(table_id7)
# print(f"Loaded {table1.num_rows} rows and {len(table1.schema)} columns to {table_id1}")
# print(f"Loaded {table2.num_rows} rows and {len(table2.schema)} columns to {table_id2}")
# print(f"Loaded {table3.num_rows} rows and {len(table3.schema)} columns to {table_id3}")
# print(f"Loaded {table4.num_rows} rows and {len(table4.schema)} columns to {table_id4}")
# print(f"Loaded {table5.num_rows} rows and {len(table5.schema)} columns to {table_id5}")
# print(f"Loaded {table6.num_rows} rows and {len(table6.schema)} columns to {table_id6}")
# print(f"Loaded {table7.num_rows} rows and {len(table7.schema)} columns to {table_id7}")

import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"


# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "braided-destiny-384416-c5a422c4df1a.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "braided-destiny-384416"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

# def load_data_without_partition(data):
#     job_config = bigquery.LoadJobConfig(
#         skip_leading_rows=1,
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         source_format=bigquery.SourceFormat.CSV,
#         autodetect=True,
#     )

#     file_path = f"{DATA_FOLDER}/{data}.csv"
#     with open(file_path, "rb") as f:
#         table_id = f"{project_id}.deb_bootcamp.{data}2"
#         job = client.load_table_from_file(f, table_id, job_config=job_config)
#         job.result()

#     table = client.get_table(table_id)
#     print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


# data = [
#     "addresses",
#     "promos",
#     "products",
#     "order_items",
# ]
# for each in data:
#     load_data_without_partition(each)


def load_data_with_partition(data, dt, clustering_fields=[]):
    if clustering_fields:
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=clustering_fields,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
        )
    partition = dt.replace("-", "")
    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.deb_bootcamp.{data}2${partition}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

load_data_with_partition("events", "2021-02-10")
load_data_with_partition("orders", "2021-02-10")
load_data_with_partition("users", "2020-10-23", ["first_name", "last_name"])