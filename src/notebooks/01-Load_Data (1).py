# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load Data from Databricks Datasets Power Utilities

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Load in data from databricks datasets
# MAGIC 
# MAGIC Power Utilities Dataset : /databricks-datasets/power-plant/data/
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC source_df : DataFrame = spark.read.format("csv")\
# MAGIC                              .option("header", True)\
# MAGIC                              .option("inferSchema", True)\
# MAGIC                              .option("delimiter", "\t")\
# MAGIC                              .load("/databricks-datasets/power-plant/data/*.tsv")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Add a random UUID to the data
# MAGIC """
# MAGIC 
# MAGIC import random, string, uuid
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC 
# MAGIC uuid_udf = udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write the data to a delta table
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC 
# MAGIC ## retrieve a multi task value to get the database name
# MAGIC database : str = dbutils.jobs.taskValues.get(taskKey    = "00-Setup", \
# MAGIC                                              key        = "database", \
# MAGIC                                              default    = 'default', \
# MAGIC                                              debugValue = 'default')
# MAGIC 
# MAGIC ## set the table name
# MAGIC table_name : str = f"{database}.demo_sensor_data"
# MAGIC 
# MAGIC try:
# MAGIC   # write out the data to a table
# MAGIC   source_df.withColumn("UUID", uuid_udf())\
# MAGIC            .withColumn("processed_time", current_timestamp())\
# MAGIC            .write.format("delta")\
# MAGIC            .option("mergeSchema", True)\
# MAGIC            .mode("overwrite")\
# MAGIC            .saveAsTable(table_name)
# MAGIC   
# MAGIC   # set a databricks task value to ensure subsequent tasks will use the write table
# MAGIC   dbutils.jobs.taskValues.set("table", table_name)
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"Failed to table, {err}")
