# Databricks notebook source
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
file_name = "rows.csv"
s3_bucket_path = f"s3://uc-external-locations-us-west-2/uc-migration/{file_name}"

# COMMAND ----------

dbutils.fs.cp(f"{download_url}", f"{s3_bucket_path}")

# COMMAND ----------

data = [(2021, "test", "Albany", "M", 42)]
columns = ["Year", "First_Name", "County", "Sex", "Count"]

df = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
display(df)

# COMMAND ----------

df_csv = spark.read.csv(f"{s3_bucket_path}",
    header=True,
    inferSchema=True,
    sep=",")
display(df_csv)

# COMMAND ----------


