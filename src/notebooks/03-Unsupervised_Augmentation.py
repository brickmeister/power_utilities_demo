# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # This module show how to execute an unsupervised learning model

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Setup

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
# MAGIC   Setup the datasets that will be used for this training example
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
# MAGIC # K-Means Model Training

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import mlflow
# MAGIC 
# MAGIC """
# MAGIC Setup MLFlow Experiment ID to allow usage in Job Batches
# MAGIC """
# MAGIC 
# MAGIC current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC 
# MAGIC mlflow.set_experiment(current_notebook_path+"_experiment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Function

# COMMAND ----------

# DBTITLE 1,K-Means Training Function
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel
# MAGIC from pyspark.ml.evaluation import ClusteringEvaluator
# MAGIC from pyspark.sql import DataFrame
# MAGIC import mlflow
# MAGIC from typing import Tuple
# MAGIC 
# MAGIC """
# MAGIC   Setup K-Means modeling
# MAGIC """
# MAGIC 
# MAGIC def kMeansTrain(nCentroids : int,
# MAGIC                 seed : int,
# MAGIC                 dataset : DataFrame,
# MAGIC                 featuresCol : str = "features") -> Tuple[BisectingKMeansModel,float]:
# MAGIC   """
# MAGIC     Setup K-Means modeling
# MAGIC     
# MAGIC     @return Trained model
# MAGIC     @return Silhouete with squared euclidean distance 
# MAGIC     
# MAGIC     @param nCentroids   | number of centroids to cluster around
# MAGIC     @param seed          | random number seed
# MAGIC     @param dataset       | Spark DataFrame containing features
# MAGIC     @param featuresCol   | Name of the vectorized column
# MAGIC   """
# MAGIC   
# MAGIC   with mlflow.start_run() as run:
# MAGIC   
# MAGIC     mlflow.log_param("Number_Centroids", str(nCentroids))
# MAGIC     mlflow.log_metric("Training Data Rows", dataset.count())
# MAGIC     mlflow.log_param("seed", str(seed))
# MAGIC 
# MAGIC     ## Start up the bisecting k-means model
# MAGIC     bkm = BisectingKMeans()\
# MAGIC                       .setFeaturesCol(featuresCol)\
# MAGIC                       .setK(nCentroids)\
# MAGIC                       .setSeed(seed)\
# MAGIC                       .setPredictionCol("predictions")
# MAGIC 
# MAGIC     ## Start up the evaluator
# MAGIC     evaluator = ClusteringEvaluator()\
# MAGIC                       .setPredictionCol("predictions")
# MAGIC 
# MAGIC     ## Train a model
# MAGIC     model = bkm.fit(dataset)
# MAGIC 
# MAGIC     ## Make some predictions
# MAGIC     predictions = model.transform(dataset)
# MAGIC 
# MAGIC     ## Evaluate the clusters
# MAGIC     silhouette = evaluator.evaluate(predictions)
# MAGIC 
# MAGIC     ## Log some modeling metrics
# MAGIC     mlflow.log_metric("Silhouette", silhouette)
# MAGIC     mlflow.spark.log_model(model, f"K-Means_{nCentroids}")
# MAGIC 
# MAGIC   
# MAGIC     ## Return the class and silhouette
# MAGIC     return (model, silhouette)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train the K-Means Model

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Hyperparameter Tuning for Number of Centroids

# COMMAND ----------

# DBTITLE 1,Tune the Number of Centroids
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Tune a K-Means Model
# MAGIC """
# MAGIC 
# MAGIC ## Tune the K Means model by optimizing the number of centroids (hyperparameter tuning)
# MAGIC kMeansTuning = [(i, kMeansTrain(nCentroids = i, dataset = sensor_features, featuresCol = "features", seed = 1)) for i in range(2 ,15, 1)]
# MAGIC 
# MAGIC ## Return the results into a series of arrays
# MAGIC kMeansCosts = [(a[0], a[1][1]) for a in kMeansTuning]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Elbow Plot

# COMMAND ----------

# DBTITLE 1,Hierarchical Bisecting K-Means Cluster Tuning
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Show the efffect of increasing the number of centroids
# MAGIC   for a K Means cluster
# MAGIC """
# MAGIC 
# MAGIC kMeansCostsDF = sc.parallelize(kMeansCosts)\
# MAGIC                       .toDF()\
# MAGIC                       .withColumnRenamed("_1", "Number of Centroids")\
# MAGIC                       .withColumnRenamed("_2", "Loss")
# MAGIC 
# MAGIC display(kMeansCostsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Label Dataset with Clusters

# COMMAND ----------

# DBTITLE 1,Setup Data Train, Test Split
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame;
# MAGIC 
# MAGIC """
# MAGIC   Separate the training and testing dataset into two dataframes
# MAGIC """
# MAGIC 
# MAGIC trainingDF, testingDF = sensor_features.randomSplit([0.7, 0.3])

# COMMAND ----------

# DBTITLE 1,Augment Data with Cluster Label
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Label the testing and training dataframes with the optimal clustering model
# MAGIC """
# MAGIC 
# MAGIC 
# MAGIC optimalClusterModel = kMeansTuning[4][1][0]
# MAGIC 
# MAGIC ## label the training and testing dataframes
# MAGIC clusteredtrainingDF = optimalClusterModel.transform(trainingDF)\
# MAGIC                             .withColumnRenamed("predictions", "cluster")
# MAGIC clusteredTestingDF = optimalClusterModel.transform(testingDF)\
# MAGIC                             .withColumnRenamed("predictions", "cluster")
# MAGIC 
# MAGIC clusteredtrainingDF.createOrReplaceTempView("clustered_training_df")
# MAGIC clusteredTestingDF.createOrReplaceTempView("clustered_testing_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Class Balance Between Training and Test Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Check for class imbalance
# MAGIC --
# MAGIC 
# MAGIC SELECT "TRAINING" AS LABEL,
# MAGIC        CLUSTER,
# MAGIC        LOG(COUNT(*)) AS LOG_COUNT
# MAGIC FROM clustered_training_df
# MAGIC GROUP BY CLUSTER
# MAGIC UNION ALL
# MAGIC SELECT "TESTING" AS LABEL,
# MAGIC        CLUSTER,
# MAGIC        LOG(COUNT(*)) AS LOG_COUNT
# MAGIC FROM clustered_testing_df
# MAGIC GROUP BY CLUSTER
# MAGIC ORDER BY CLUSTER ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write to Feature Store

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write to feature store
# MAGIC """
# MAGIC 
# MAGIC augmented_df : DataFrame = optimalClusterModel.transform(sensor_features)\
# MAGIC                                               .drop("Features")\
# MAGIC                                               .withColumnRenamed("predictions", "Cluster")

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
# MAGIC database : str = dbutils.jobs.taskValues.get(taskKey    = "00-Setup", \
# MAGIC                                              key        = "database", \
# MAGIC                                              default    = 'default', \
# MAGIC                                              debugValue = 'default')
# MAGIC 
# MAGIC ## set the feature table name
# MAGIC feature_table : str = f"{database}.augmented_feature_sensor_data"
# MAGIC 
# MAGIC try:
# MAGIC   # setup the feature store table
# MAGIC   fs.create_table(
# MAGIC     name=feature_table,
# MAGIC     primary_keys='UUID',
# MAGIC     schema=augmented_df.schema,
# MAGIC     description='Augmented Sensor Data Features')
# MAGIC except Exception as err:
# MAGIC   ## if table is registered, just leave it
# MAGIC   pass
# MAGIC   
# MAGIC try:
# MAGIC   # write to feature store
# MAGIC   fs.write_table(
# MAGIC   name=feature_table,
# MAGIC   df = augmented_df,
# MAGIC   mode = 'overwrite')
# MAGIC   
# MAGIC   # set the feature store value for the multi-task workflow
# MAGIC   dbutils.jobs.taskValues.set("augmented_feature_store", feature_table)
# MAGIC   
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"Failed to write out to feature store, {err}")
