# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Decision Tree Model Training

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prepare Data

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
# MAGIC feature_table_name = dbutils.jobs.taskValues.get(taskKey    = "03-Unsupervised_Augmentation", \
# MAGIC                                                  key        = "augmented_feature_table", \
# MAGIC                                                  default    = 'default.augmented_feature_sensor_data', \
# MAGIC                                                  debugValue = 'default.augmented_feature_sensor_data')
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

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame;
# MAGIC 
# MAGIC """
# MAGIC   Separate the training and testing dataset into two dataframes
# MAGIC """
# MAGIC 
# MAGIC trainingDF, testingDF = sensor_features.randomSplit([0.7, 0.3])
# MAGIC trainingDF.createOrReplaceTempView("clustered_training_df")
# MAGIC testingDF.createOrReplaceTempView("clustered_testing_df")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Visualize class imbalance

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

# MAGIC %python
# MAGIC 
# MAGIC import mlflow
# MAGIC 
# MAGIC """
# MAGIC Setup MLFlow Experiment ID to allow usage in Job Batches
# MAGIC """
# MAGIC 
# MAGIC mlflow.set_experiment("/Users/mark.lee@databricks.com/power_utilities_sensor_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Function

# COMMAND ----------

# DBTITLE 1,Decision Tree Training Function
import mlflow
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from typing import Tuple

def dtcTrain(p_max_depth : int,
             training_data : DataFrame,
             test_data : DataFrame,
             seed : int,
             featuresCol : str,
             labelCol : str) -> Tuple[int, float]:
  with mlflow.start_run() as run:
    # log some parameters
    mlflow.log_param("Maximum_depth", p_max_depth)
    mlflow.log_metric("Training Data Rows", training_data.count())
    mlflow.log_metric("Test Data Rows", test_data.count())
    
    # start the decision tree classifier
    dtc = DecisionTreeClassifier()\
                          .setFeaturesCol(featuresCol)\
                          .setLabelCol(labelCol)\
                          .setMaxDepth(p_max_depth)\
                          .setSeed(seed)\
                          .setPredictionCol("predictions")\
                          .setMaxBins(4000)
    
    # Start up the evaluator
    evaluator = MulticlassClassificationEvaluator()\
                      .setLabelCol("Cluster")\
                      .setPredictionCol("predictions")

    # Train a model
    model = dtc.fit(training_data)

    # Make some predictions
    predictions = model.transform(test_data)

    # Evaluate the tree
    silhouette = evaluator.evaluate(predictions)
    
    # Log the accuracy
    mlflow.log_metric("F1", silhouette)
    
    # Log the feature importances
    mlflow.log_param("Feature Importances", model.featureImportances)
    
    # Log the model
    mlflow.spark.log_model(model, f"Decision_tree_{p_max_depth}")
    
    ## Return the class and silhouette
    return (model, silhouette)

# COMMAND ----------

# MAGIC %md
# MAGIC # Train the Decision Tree

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameter tuning for Max Depth

# COMMAND ----------

# DBTITLE 1,Tune Tree Depth
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Tune the max depth of the Decision tree
# MAGIC """
# MAGIC 
# MAGIC dtcTuning = [(i, dtcTrain(p_max_depth = i,
# MAGIC                           training_data = trainingDF,
# MAGIC                           test_data = testingDF,
# MAGIC                           seed = 1,
# MAGIC                           featuresCol = "features",
# MAGIC                           labelCol = "Cluster"))
# MAGIC               for i in range(2, 15, 1)]
# MAGIC 
# MAGIC ## Return the results into a series of arrays
# MAGIC dtcF1 = [(a[0], a[1][1]) for a in dtcTuning]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elbow Plot

# COMMAND ----------

# DBTITLE 1,Decision Tree Tuning
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Show the effect of increasing the max depth
# MAGIC   for a Decision Tree
# MAGIC """
# MAGIC 
# MAGIC dtcF1DF = sc.parallelize(dtcF1)\
# MAGIC                       .toDF()\
# MAGIC                       .withColumnRenamed("_1", "Max Depth")\
# MAGIC                       .withColumnRenamed("_2", "F1")
# MAGIC 
# MAGIC display(dtcF1DF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Set the optimal model for the next workflow
# MAGIC """
# MAGIC 
# MAGIC dbutils.jobs.taskValues.set("best_dtc_model", "dbfs:/databricks/mlflow-tracking/d9058f27daac417b87b103f869afe14f/a664a4a837bf4b04a85f990af611d06b/artifacts/Decision_tree_4")
