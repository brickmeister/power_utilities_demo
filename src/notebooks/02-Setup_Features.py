# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # This module generates ML features

# COMMAND ----------

# DBTITLE 1,Retrieve Data from Delta Lake
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC """
# MAGIC   Setup a dataframe to read in data from
# MAGIC   a gold level table  
# MAGIC """
# MAGIC 
# MAGIC table : str = dbutils.jobs.taskValues.get(taskKey    = "01-Load_Data", \
# MAGIC                                           key        = "table", \
# MAGIC                                           default    = 'default.demo_sensor_data', \
# MAGIC                                           debugValue = 'default.demo_sensor_data')
# MAGIC 
# MAGIC bronze_df : DataFrame = spark.read\
# MAGIC                              .format("delta")\
# MAGIC                              .table(table);
# MAGIC 
# MAGIC bronze_df.createOrReplaceTempView("delta_power_stream")
# MAGIC 
# MAGIC display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get the Schema of the Data
# MAGIC 
# MAGIC Retrieve the schema of the data to get an idea as to what data types we are working with. This is used for featurizing the dataset.

# COMMAND ----------

# DBTITLE 1,Get the Schema of the Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get the schema of the columns
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE delta_power_stream;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Sanity Checks

# COMMAND ----------

# DBTITLE 1,Amount of Records Per UUID
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- look at the amount of records added to the data set over time
# MAGIC --
# MAGIC 
# MAGIC SELECT UUID,
# MAGIC        count(*) as record_counts
# MAGIC FROM delta_power_stream
# MAGIC GROUP BY UUID
# MAGIC ORDER BY UUID asc;

# COMMAND ----------

# DBTITLE 1,Look at Average Metrics Streaming In
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- look at average metrics streaming in
# MAGIC --
# MAGIC 
# MAGIC SELECT avg(V) as avg_v,
# MAGIC        avg(AP) as avg_ap,
# MAGIC        avg(RH) as avg_rh,
# MAGIC        avg(PE) as avg_pe
# MAGIC FROM delta_power_stream;

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Dataframes for ML
# MAGIC 
# MAGIC Dataframes need to be featurized and split into train and test partitions for machine learning.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Clean the data

# COMMAND ----------

# DBTITLE 1,Remove Data That Doesn't Contains Nulls
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Remove censored data and cast data to proper data types
# MAGIC """
# MAGIC 
# MAGIC cleaned_df : DataFrame = spark.read.format("delta")\
# MAGIC                               .table(table)\
# MAGIC                               .na.drop();
# MAGIC 
# MAGIC cleaned_df.createOrReplaceTempView("cleaned_power_sensors")
# MAGIC 
# MAGIC display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Featurize the Dataset

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from databricks.feature_store import FeatureStoreClient
# MAGIC 
# MAGIC """
# MAGIC   Write to feature store
# MAGIC """
# MAGIC 
# MAGIC ## start up the feature store client
# MAGIC fs = FeatureStoreClient()
# MAGIC 
# MAGIC ## retrieve the database 
# MAGIC database : str = dbutils.jobs.taskValues.get(taskKey    = "00-SETUP", \
# MAGIC                                              key        = "database", \
# MAGIC                                              default    = 'default', \
# MAGIC                                              debugValue = 'default')
# MAGIC 
# MAGIC ## set the feature table name
# MAGIC feature_table : str = f"{database}.feature_sensor_data"
# MAGIC 
# MAGIC try:
# MAGIC   # setup the feature store table
# MAGIC   fs.create_table(
# MAGIC     name=feature_table,
# MAGIC     primary_keys='UUID',
# MAGIC     schema=cleaned_df.schema,
# MAGIC     description='Sensor Data Features')
# MAGIC except Exception as err:
# MAGIC   ## if table is registered, just leave it
# MAGIC   pass
# MAGIC   
# MAGIC try:
# MAGIC   # write to feature store
# MAGIC   fs.write_table(
# MAGIC   name=feature_table,
# MAGIC   df = cleaned_df,
# MAGIC   mode = 'overwrite')
# MAGIC   
# MAGIC   # set the feature store value for the multi-task workflow
# MAGIC   dbutils.jobs.taskValues.set("feature_store", feature_table)
# MAGIC   
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"Failed to write out to feature store, {err}")
