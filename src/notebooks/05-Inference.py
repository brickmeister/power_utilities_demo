# Databricks notebook source
# MAGIC %md
# MAGIC ## Forecast the tier/bracket a sensor signal will belong to

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from databricks.feature_store import FeatureStoreClient
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC """
# MAGIC Get data from the feature store
# MAGIC """
# MAGIC 
# MAGIC ## initialize the feature store client
# MAGIC fs = FeatureStoreClient()
# MAGIC 
# MAGIC ## get the feature table name from multi task workflow
# MAGIC feature_table_name = dbutils.jobs.taskValues.get(taskKey    = "02-Setup_Features", \
# MAGIC                                                  key        = "feature_table", \
# MAGIC                                                  default    = 'default.feature_sensor_data', \
# MAGIC                                                  debugValue = 'default.feature_sensor_data')
# MAGIC df_sensors : DataFrame = fs.read_table(feature_table_name)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC """
# MAGIC 
# MAGIC   Setup the datasets for featurization
# MAGIC 
# MAGIC """
# MAGIC 
# MAGIC # Convert strings to floats
# MAGIC 
# MAGIC features = [x for x in df_sensors.columns if x.upper() not in ["UUID", "PROCESSED_TIME"]] ## use all features except metadata or those string indexed
# MAGIC 
# MAGIC assembler = VectorAssembler(inputCols = features,
# MAGIC                             outputCol = "features")
# MAGIC 
# MAGIC sensor_features = assembler.transform(df_sensors)
# MAGIC 
# MAGIC display(sensor_features)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Batch Inferencing

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Retrieve the model path
# MAGIC """
# MAGIC 
# MAGIC logged_model = dbutils.jobs.taskValues.get(taskKey    = "04-Decision_Tree_Training", \
# MAGIC                                            key        = "best_dtc_model", \
# MAGIC                                            default    = 'dbfs:/databricks/mlflow-tracking/d9058f27daac417b87b103f869afe14f/a664a4a837bf4b04a85f990af611d06b/artifacts/Decision_tree_4',
# MAGIC                                            debugValue = 'dbfs:/databricks/mlflow-tracking/d9058f27daac417b87b103f869afe14f/a664a4a837bf4b04a85f990af611d06b/artifacts/Decision_tree_4')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Inference on some data
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC import mlflow
# MAGIC 
# MAGIC ## get the logged model
# MAGIC model = mlflow.spark.load_model(logged_model)
# MAGIC 
# MAGIC ## do some batch inferencing
# MAGIC inferences : DataFrame = model.transform(sensor_features)
