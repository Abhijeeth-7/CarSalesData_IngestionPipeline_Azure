# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

silver_table_name = "carsales_catalog.silver.car_sales_silver"
silver_data_path = "abfss://silver@carsalesdatalake04ajaz.dfs.core.windows.net/transformed_data"
bronze_data_path = "abfss://bronze@carsalesdatalake04ajaz.dfs.core.windows.net/raw_data"

is_incremental = dbutils.widgets.get("is_incremental").lower() == "true"
if is_incremental and spark.catalog.tableExists(silver_table_name): 
    sink_df = spark.read.format("delta").load(path)

raw_data_bronze = spark.read.parquet(bronze_data_path).filter(col("bronze_load_timestamp") > silver_tabl)


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

incremental_data_df = raw_data_bronze.withColumn(
    "Revenue", col("Revenue").cast("decimal(19,2)")
).withColumn(
    "UnitsSold", col("UnitsSold").cast("int")
).withColumn(
    "Date", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-"), col("Day")))
).withColumn(
    "silver_load_timestamp", current_timestamp()
).drop("bronze_load_timestamp")



# COMMAND ----------

# MAGIC %md
# MAGIC # Handling NULL Values

# COMMAND ----------


incremental_data_df = incremental_data_df.dropna(how="any", subset=["Date", "DateId"])
incremental_data_df = incremental_data_df.fillna(0, subset=["Revenue", "UnitsSold"])
incremental_data_df = incremental_data_df.fillna("NA", subset=["BranchName", "DealerName", "ProductName", "BranchId", "DealerId", "ModelId"])


# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a surrogate key

# COMMAND ----------

silver_layer_schema = StructType([
    StructField("BranchId", StringType(), False), # Non-nullable in silver
    StructField("DealerId", StringType(), False), # Non-nullable in silver
    StructField("ModelId", StringType(), False), # Non-nullable in silver
    StructField("Revenue", DecimalType(19,2), True),
    StructField("UnitsSold", IntegerType(), False),  # Non-nullable in silver
    StructField("DateId", StringType(), False),
    StructField("Day", IntegerType(), False),
    StructField("Month", IntegerType(), False),
    StructField("Year", IntegerType(), False),
    StructField("BranchName", StringType(), False), # Non-nullable in silver
    StructField("DealerName", StringType(), False), # Non-nullable in silver
    StructField("ProductName", StringType(), False), # Non-nullable in silver
    StructField("Date", DateType(), False),
    StructField("silver_load_timestamp", TimestampType(), False),  
    StructField("silver_sales_key", LongType(), False)
])

sink_df = incremental_data_df.withColumn("silver_sales_key", lit(None))
src_df = incremental_data_df
max_sales_id = 1

if is_incremental and spark.catalog.tableExists(silver_table_name): 
    sink_df = spark.read.format("delta").load(silver_data_path)
    max_sales_id = sink_df.agg(max("silver_sales_key")).collect()[0][0]
else:
    sink_df = spark.createDataFrame([], schema=silver_layer_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join upserted records with existing data in sink 

# COMMAND ----------

join_condition =  (src_df['ModelId'] == sink_df['ModelId'])\
                    & (src_df['DateId'] == sink_df['DateId'])\
                    & (src_df['BranchId'] == sink_df['BranchId'])\
                    & (src_df['DealerId'] == sink_df['DealerId'])

upserted_records_df = src_df.join(sink_df, join_condition, "left")\
                            .select(src_df['*'], sink_df['silver_sales_key'])



# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter new and upserted records, add surrogate key to upserted records

# COMMAND ----------


#filter old records
updated_records = upserted_records_df.filter(~upserted_records_df["silver_sales_key"].isNull())

#filter new records and add surrogate key for newly inserted records
new_records = upserted_records_df.filter(upserted_records_df["silver_sales_key"].isNull())
new_records = new_records.withColumn("silver_sales_key", max_sales_id + monotonically_increasing_id())

#combine new and updated records
upserted_records_df = updated_records.union(new_records)
upserted_records_df[upserted_records_df.Date.isNull()]


# COMMAND ----------

# MAGIC %md 
# MAGIC # Write the upserted records to the sink using Delta lake

# COMMAND ----------

if is_incremental and spark.catalog.tableExists("carsales_catalog.silver.car_sales_silver"):
    src_delta = DeltaTable.forPath(spark, silver_data_path)
    src_delta\
        .merge(upserted_records_df, src_delta['silver_sales_key'] == upserted_records_df['silver_sales_key'])\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    upserted_records_df.write.format('delta')\
        .mode('overwrite')\
        .option("path", silver_data_path)\
        .saveAsTable("carsales_catalog.silver.car_sales_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC # Testing

# COMMAND ----------

spark.sql("SELECT * FROM carsales_catalog.silver.car_sales_silver LIMIT 10").display()
