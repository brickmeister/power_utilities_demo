# MAGIC %md
# MAGIC 
# MAGIC ## Decision Tree Model Training

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Function

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
                      .setLabelCol("cluster")\
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
# MAGIC ### Train the Decision Tree

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hyperparameter tuning for Max Depth

# COMMAND ----------

# DBTITLE 1,Tune Tree Depth
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Tune the max depth of the Decision tree
# MAGIC """
# MAGIC 
# MAGIC dtcTuning = [(i, dtcTrain(p_max_depth = i,
# MAGIC                           training_data = clusteredtrainingDF,
# MAGIC                           test_data = clusteredTestingDF,
# MAGIC                           seed = 1,
# MAGIC                           featuresCol = "features",
# MAGIC                           labelCol = "cluster"))
# MAGIC               for i in range(2, 15, 1)]
# MAGIC 
# MAGIC ## Return the results into a series of arrays
# MAGIC dtcF1 = [(a[0], a[1][1]) for a in dtcTuning]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Elbow Plot

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