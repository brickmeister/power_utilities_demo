# Power Utilities Demo

Determining points of failure for IOT devices is a common use case for power utilities. The following code reads streaming data from Kafka and utilizes unsupervised clustering for failure segmentation.

# Table of Contents
- [Power Utilities Demo](#power-utilities-demo)
- [Table of Contents](#table-of-contents)
- [Architecture](#architecture)
- [Dataset](#dataset)
  - [Schema](#schema)
- [Libraries used](#libraries-used)
- [Models used](#models-used)

# Architecture
![architecture](https://raw.githubusercontent.com/brickmeister/power_utilities_demo/master/img/Power%20Utilities%20Demographic%20Segmentation.png)

# Dataset

The example data is provided by UCI at UCI Machine Learning Repository Combined Cycle Power Plant Data Set
 * Source : http://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant

## Schema

 | Field | Data Type | Description |
 | ------------ | ------------ | ------------ |
 |MAGIC |AT|StringType|Ambient Temperature|
 |V|StringType|Exhaust Vacuum|
 |AP|StringType|Ambient Pressure|
 |RH|StringType|Relative Humdity|
 |PE|StringType|Energy Output|

# Libraries used
* [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html)
* [Delta Lake](https://docs.delta.io/latest/delta-intro.html)
* [SparkML](http://spark.apache.org/docs/latest/ml-guide.html)
* [MLFlow](https://www.mlflow.org/docs/latest/index.html)
* [SparkXGBoost](https://github.com/sllynn/spark-xgboost.git)

# Models used
* [Hierarchical Bisecting-K-Means](https://medium.com/@afrizalfir/bisecting-kmeans-clustering-5bc17603b8a2)
* [Decision Tree Classifier](https://medium.com/swlh/decision-tree-classification-de64fc4d5aac)
