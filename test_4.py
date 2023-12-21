"""address = "s3://data-handling-public/products.csv"
s3_address = address.replace("s3://",  "")
parts = s3_address.split("/")
bucket_name = parts[0]
object_key = "/".join(parts[1:])
print(parts)
print(f"bucket: {bucket_name}")
print(f"object key: {object_key}")"""
import database_utils
import data_extraction
rds_df = data_extraction.DataExtractor.reads_rds_table()
rds_df.drop(rds_df['first_name'])