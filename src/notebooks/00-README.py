# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Abstract
# MAGIC   Power utilities are heavily reliant on sensor data that resemble the internet of things (IOT). Due to increasing power demands globally, forecasting sensor failure can greatly reduce power disruptions and revenue loss. We showcase an auto-segmentation model that classifies sensor data in real-time utilizing Databricks and the SparkML platform. We believe the demographic segmentation will help power utilities monitors better power stability to customers.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dataset
# MAGIC 
# MAGIC The example data is provided by UCI at UCI Machine Learning Repository Combined Cycle Power Plant Data Set
# MAGIC * Source : http://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Schema
# MAGIC 
# MAGIC | Field | Data Type | Description |
# MAGIC | ------------ | ------------ | ------------ |
# MAGIC |AT|StringType|Ambient Temperature|
# MAGIC |V|StringType|Exhaust Vacuum|
# MAGIC |AP|StringType|Ambient Pressure|
# MAGIC |RH|StringType|Relative Humdity|
# MAGIC |PE|StringType|Energy Output|

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Methods
# MAGIC 
# MAGIC Data is first cleaned and featurized using Scala SparkML libraries. Resultant data is passed through two machine learning models (hierarchical bisecting-kmeans, decision tree classifier) in order to segment a dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Architecture
# MAGIC <img src="https://raw.githubusercontent.com/brickmeister/power_utilities_demo/master/img/Power%20Utilities%20Demographic%20Segmentation.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Libraries Used
# MAGIC * [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC * [Delta Lake](https://docs.delta.io/latest/delta-intro.html)
# MAGIC * [SparkML](http://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC * [MLFlow](https://www.mlflow.org/docs/latest/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Models Used
# MAGIC * [Hierarchical Bisecting-K-Means](https://medium.com/@afrizalfir/bisecting-kmeans-clustering-5bc17603b8a2)
# MAGIC * [Decision Tree Classifier](https://medium.com/swlh/decision-tree-classification-de64fc4d5aac)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion/Results
# MAGIC   Auto segmentation of streaming sensor data suggested there were 5 distinct categories of power plant running statuses. Further classification of these statuses would allow better targeted post mortem analyses as well as on-line training for power utilities monitors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Follow up sources
# MAGIC 1. [Prediction of full load electrical power output of a base load operated combined cycle power plant using machine learning methods](https://www.sciencedirect.com/science/article/abs/pii/S0142061514000908)
# MAGIC 2. [Scaling Advanced Analytics at Shell](https://databricks.com/session/scaling-advanced-analytics-at-shell)

# COMMAND ----------