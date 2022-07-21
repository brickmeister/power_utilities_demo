# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## K-Means Model Training

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
# MAGIC kMeansTuning = [(i, kMeansTrain(nCentroids = i, dataset = dfDataset, featuresCol = "features", seed = 1)) for i in range(2 ,15, 1)]
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

# DBTITLE 1,Validation DataFrame Class Balance
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