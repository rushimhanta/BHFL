from datetime import datetime, timedelta
import pandas as pd
import boto3
import json
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
def get_mysql_secret(secret_name, region="ap-south-1"):
    secret_name = "mangesh-mysql-secrets"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)
secret = get_mysql_secret("my-mysql-secret")

username = secret["username"]
password = secret["password"]
host     = secret["host"]
port     = secret["port"]
database = secret["database"]

engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
)
df_cust = pd.read_sql_table("customers", engine)
df_customers = list(df_cust["customer_id"])
filter_customer_str = ",".join([f"'{c}'" for c in df_customers])
query = f"""
SELECT *
FROM transactions
WHERE customer_id IN ({filter_customer_str})
  AND transaction_date >= %(start)s
  AND transaction_date <= %(end)s
"""
def get_last_month_range():
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return first_day_last_month, last_day_last_month
start_date, end_date = get_last_month_range()
def mask_aadhaar(adhar):
    adhar = str(adhar)
    if len(adhar) >= 8:
        return adhar[:4] + "XXXX" + adhar[-4:]
    return "XXXX"
def mask_phone(phone):
    phone = str(phone)
    if len(phone) >= 4:
        return "XXXXX" + phone[-4:]
    return "XXXXX"
def mask_pan(pan):
    pan = str(pan)
    if len(pan) >= 4:
        return "XXXXX" + pan[-2:]
    return "XXXXX"
df_txn = pd.read_sql(query, engine, params={"start": start_date, "end": end_date})
df = df_txn.merge(df_cust, on="customer_id", how="left")
df["aadhaar"] = df["aadhaar"].apply(mask_aadhaar)
df["phone_no"] = df["phone_no"].apply(mask_phone)
df["panid"] = df["panid"].apply(mask_pan)
df.drop(columns=["status","txn_type"],inplace=True)
s3 = boto3.client("s3")
bucket = "bhfl-bank-transformed"
for cust_id, cust_df in df.groupby("customer_id"):
    table = pa.Table.from_pandas(cust_df)
    key = f"transactions/month={start_date.strftime('%Y-%m')}/cust_id={cust_id}/data.parquet"

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().to_pybytes()
    )
