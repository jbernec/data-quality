# Databricks notebook source
# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls04.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls04.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls04.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

# File path on DBFS
source_path = "dbfs/files/data_sample.csv"

dbutils.fs.put(
    "file:/dbfs/files/data_sample.csv",
    """
id,name,age,profession
1, Alice, 25, Engineer
2, Bob, 30, Doctor
3, Charlie, , Artist
4, David, 45, Chef
5, Eve, 28, Data Scientist
6, Frank, 33, Lawyer

""",
    True,
)

# COMMAND ----------

source_path = "file:/dbfs/files/data_sample.csv"
dbutils.fs.ls(source_path)

# COMMAND ----------

source_file = "dbfs:/files/data_sample.csv"
df = spark.read.format("csv").options(inferschema=True,header=True).load(source_file)
df.display()

# COMMAND ----------

# write to delta lake
df.write.mode("overwrite").format("delta").save("abfss://experiments@adls04.dfs.core.windows.net/pytest")

# COMMAND ----------

df_lake = spark.read.format("delta").load("abfss://experiments@adls04.dfs.core.windows.net/pytest")
df_lake.display()

# COMMAND ----------

source_file = "dbfs:/files/data_sample.csv"
df = spark.read.format("csv").options(inferschema=True,header=True).load(source_file)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema =StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

# COMMAND ----------

df_csv = spark.read.format("csv").options(inferschema=True,header=True).schema(schema).load(source_file)

# COMMAND ----------

df_csv.printSchema()
