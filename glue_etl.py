# ETL script

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data
sales_df = spark.read.option("header", "true").csv("s3://supply-chain-data-lake/raw/sales/")
shipments_df = spark.read.option("header", "true").csv("s3://supply-chain-data-lake/raw/shipment/")
warehouse_df = spark.read.option("header", "true").csv("s3://supply-chain-data-lake/raw/warehouse_master/")

# Cleaning: Dedup, null fill, cast
sales_df = sales_df.dropDuplicates(["Order_ID"]).na.fill({"Discount": 0})
sales_df = sales_df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))
# Validate: Quantity >=0, Unit_Price >0 (filter if needed)
sales_df = sales_df.filter(col("Quantity") >= 0).filter(col("Unit_Price") > 0)

# Normalization: Uppercase region, join warehouse
sales_df = sales_df.withColumn("Region", trim(upper(col("Region"))))
# Map synonyms e.g., when(col("Region") == "N", lit("NORTH")).otherwise(col("Region"))
enriched_df = sales_df.join(warehouse_df, "Product_ID", "left")

# Aggregation: Daily sales
daily_df = enriched_df.groupBy("Region", "Product_ID", date_trunc("DAY", "Order_Date").alias("date")) \
    .agg(sum("Quantity").alias("daily_qty"), sum("Total_Sales").alias("daily_sales"))
# Lag features for ML
daily_df = daily_df.withColumn("lag_7d_qty", lag("daily_qty", 7).over(Window.partitionBy("Product_ID", "Region").orderBy("date")))

# Enrich with shipments (merge on Order_ID, adjust for delays >3 days: -5% demand)
daily_df = daily_df.join(shipments_df.select("Order_ID", "Delay_Days").alias("delays"), "Order_ID", "left")
daily_df = daily_df.withColumn("adjusted_qty", when(col("Delay_Days") > 3, col("daily_qty") * 0.95).otherwise(col("daily_qty")))

# Write Parquet partitioned
daily_df.write.mode("overwrite").partitionBy("Region", "year", "month", "day").parquet("s3://supply-chain-data-lake/processed/sales/")
# Schema evolution enabled via Glue Catalog

# Quality checks
row_count = daily_df.count()
null_check = daily_df.filter(col("daily_qty").isNull()).count()
if null_check / row_count > 0.05:
    raise Exception("High null rate detected")
# Min/max: daily_df.agg(min("daily_qty"), max("daily_sales")).show()

job.commit()