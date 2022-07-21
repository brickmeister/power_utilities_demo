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
# MAGIC                              .option("delimiter", "\t")\
# MAGIC                              .load("/databricks-datasets/power-plant/data/*.tsv")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write the data to a delta table
# MAGIC """
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
# MAGIC source_df.write.format("delta")\
# MAGIC          .option("mode", "append")\
# MAGIC          .saveAsTable(table_name)
