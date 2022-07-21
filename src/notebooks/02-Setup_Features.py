# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,Retrieve Data from Delta Lake
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC 
# MAGIC /*
# MAGIC   Setup a dataframe to read in data from
# MAGIC   a gold level table  
# MAGIC */
# MAGIC 
# MAGIC val df : DataFrame = spark.readStream
# MAGIC                           .format("delta")
# MAGIC                           .table("power_utilities_demo_data");
# MAGIC 
# MAGIC df.createOrReplaceTempView("delta_power_stream")
# MAGIC 
# MAGIC display(df)

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

# DBTITLE 1,Amount of Records Per EventID
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- look at the amount of records added to the data set over time
# MAGIC --
# MAGIC 
# MAGIC SELECT eventID,
# MAGIC        count(*) as record_counts
# MAGIC FROM delta_power_stream
# MAGIC GROUP BY eventID
# MAGIC ORDER BY eventID asc;

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
# MAGIC ## Setup Dataframes for ML
# MAGIC 
# MAGIC Dataframes need to be featurized and split into train and test partitions for machine learning.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Clean the data

# COMMAND ----------

# DBTITLE 1,Remove Data That Doesn't Contains Nulls
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Remove censored data and cast data to proper data types
# MAGIC */
# MAGIC 
# MAGIC val cleaned_df : DataFrame = spark.read
# MAGIC                                   .format("delta")
# MAGIC                                   .table("power_utilities_demo_data")
# MAGIC                                   .na.drop();
# MAGIC 
# MAGIC cleaned_df.createOrReplaceTempView("cleaned_power_sensors")
# MAGIC 
# MAGIC display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Featurize the Dataset

# COMMAND ----------

# DBTITLE 1,Create a ML Dataset
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC """
# MAGIC 
# MAGIC   Setup the datasets that will be used for this training example
# MAGIC 
# MAGIC """
# MAGIC 
# MAGIC # Convert strings to floats
# MAGIC df_sensors = spark.sql("SELECT * FROM cleaned_power_sensors")\
# MAGIC                   .select(col("eventID"),
# MAGIC                           col("AT").cast("float"),
# MAGIC                           col("V").cast("float"),
# MAGIC                           col("AP").cast("float"),
# MAGIC                           col("RH").cast("float"),
# MAGIC                           col("PE").cast("float"))
# MAGIC 
# MAGIC features = [x for x in df_sensors.columns if x not in ["eventID"]] ## use all features except metadata or those string indexed
# MAGIC 
# MAGIC assembler = VectorAssembler(inputCols = features,
# MAGIC                             outputCol = "features")