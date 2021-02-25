# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Abstract
# MAGIC   The COVID-19 pandemic has been an ongoing global health crisis that has threatened many nations. In order to combat the pandemic, numerous approaches such as social distancing, contact tracing, and ventilation equipment have been used to track the spread. The large amount of data and ongoing crisis also suggest that there currently isn't an unanimous treatment for the global demographic. Hence, we developed a segmentation model to better categorize COVID-19 cases into nine distinct topics as well as determined the features associated with each of these topics. We believe the demographic segmentation will help first responders and epidemiologists in better targeting COVID-19 treatment and messaging.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dataset
# MAGIC 
# MAGIC =========================================
# MAGIC Combined Cycle Power Plant Data Set
# MAGIC =========================================
# MAGIC Power Plant Sensor Readings Data Set
# MAGIC 
# MAGIC ### Source
# MAGIC http://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant
# MAGIC 
# MAGIC ### Summary
# MAGIC 
# MAGIC The example data is provided by UCI at UCI Machine Learning Repository Combined Cycle Power Plant Data Set
# MAGIC You can read the background on the UCI page, but in summary we have collected a number of readings from sensors at a Gas Fired Power Plant (also called a Peaker Plant) and now we want to use those sensor readings to predict how much power the plant will generate.
# MAGIC 
# MAGIC ### Usage License
# MAGIC 
# MAGIC If you publish material based on databases obtained from this repository, then, in your acknowledgements, please note the assistance you received by using this repository. This will help others to obtain the same data sets and replicate your experiments. We suggest the following reference format for referring to this repository:
# MAGIC Pınar Tüfekci, Prediction of full load electrical power output of a base load operated combined cycle power plant using machine learning methods, International Journal of Electrical Power & Energy Systems, Volume 60, September 2014, Pages 126-140, ISSN 0142-0615, [Link](http://www.sciencedirect.com/science/article/pii/S0142061514000908)
# MAGIC 
# MAGIC 
# MAGIC Heysem Kaya, Pınar Tüfekci , Sadık Fikret Gürgen: Local and Global Learning Methods for Predicting Power of a Combined Gas & Steam Turbine, Proceedings of the International Conference on Emerging Trends in Computer and Electronics Engineering ICETCEE 2012, pp. 13-18 (Mar. 2012, Dubai)

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
# MAGIC <img src="https://raw.githubusercontent.com/brickmeister/power_utilities_demo/master/img/Power%20Utilities%20Demographic%20Segmentation.png" width = 100% /img>

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
# MAGIC Six categories were derived from the unsupervised clustering which were classified according to a decision tree. Average accuracy for the decision tree model was 97% [CMD 77], and the average precision was 94% [CMD 79].
# MAGIC 
# MAGIC Among the features that strongly divided groups, the top features
# MAGIC were unsurprisingly # of patients recovered, # of active cases. The surprising factor was that the longitude of a record was also a strong separator into categories. This suggests that COVID-19 may have more stratification across timezones than seasons (heat doesn't strongly affect the virus).
# MAGIC 
# MAGIC Obviously due to the preliminary nature of this analysis, any conclusions drawn from this analysis should be further verified.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Follow up sources
# MAGIC 1. [Prediction of full load electrical power output of a base load operated combined cycle power plant using machine learning methods](https://www.sciencedirect.com/science/article/abs/pii/S0142061514000908)
# MAGIC 2. [Scaling Advanced Analytics at Shell](https://databricks.com/session/scaling-advanced-analytics-at-shell)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve Power Utilities Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve Data from Kafka

# COMMAND ----------

# DBTITLE 1,Setup some Kafka Credentials
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# MAGIC   Just choose the set of corresponding endpoints to use.
# MAGIC   If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
# MAGIC */
# MAGIC 
# MAGIC val kafka_bootstrap_servers_tls : String = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-tls"       )
# MAGIC val kafka_bootstrap_servers_plaintext : String = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-plaintext" )
# MAGIC 
# MAGIC // Full username, e.g. "aaron.binns@databricks.com"
# MAGIC val username : String = dbutils.notebook.getContext.tags("user")
# MAGIC 
# MAGIC // Short form of username, suitable for use as part of a topic name.
# MAGIC val user : String = username.split("@")(0).replace(".","_")+"_2"
# MAGIC 
# MAGIC // DBFS directory for this project, we will store the Kafka checkpoint in there
# MAGIC val project_dir : String = s"/home/${username}/oetrta/kafka_test"
# MAGIC 
# MAGIC val checkpoint_location : String = s"${project_dir}/kafka_checkpoint"
# MAGIC 
# MAGIC val topic : String = s"${user}_oetrta_kafka_test"

# COMMAND ----------

# DBTITLE 1,Read Kafka Stream
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Read Kafka Topic
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.from_json;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC 
# MAGIC // setup the input schema to decode the kafka topic
# MAGIC val input_schema : StructType = StructType(List(StructField("AT",StringType,true),
# MAGIC                                                 StructField("V",StringType,true),
# MAGIC                                                 StructField("AP",StringType,true),
# MAGIC                                                 StructField("RH",StringType,true),
# MAGIC                                                 StructField("PE",StringType,true)));
# MAGIC 
# MAGIC // setup the starting offset
# MAGIC val startingOffsets : String = "earliest";
# MAGIC 
# MAGIC val df_sensors : DataFrame = spark.readStream
# MAGIC                                   .format("kafka")
# MAGIC                                   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
# MAGIC                                   .option("subscribe", topic)
# MAGIC                                   .option("startingOffsets", startingOffsets)
# MAGIC                                   .load()
# MAGIC                                   .select($"key".cast("string").alias("eventId"),
# MAGIC                                           from_json($"value".cast("string"), 
# MAGIC                                                     input_schema)
# MAGIC                                             .alias("json"));
# MAGIC 
# MAGIC // display the dataframe
# MAGIC display(df_sensors)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Data into Delta

# COMMAND ----------

# DBTITLE 1,Ingest Data into Delta
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.current_timestamp;
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC 
# MAGIC /*
# MAGIC 
# MAGIC   Load the Power Sensor data into a Dataframe
# MAGIC 
# MAGIC */
# MAGIC 
# MAGIC // Flatten the JSON object from Kafka into a DataFrame
# MAGIC val df_raw = df_sensors
# MAGIC                 .select($"eventID", $"json.*") // Flatten the JSON object
# MAGIC                 .withColumn("ingested_time", current_timestamp) // append the ingested time
# MAGIC 
# MAGIC // Write to delta lae
# MAGIC df_raw.writeStream
# MAGIC       .format("delta")
# MAGIC       .option("checkpointLocation", s"dbfs:/tmp/${user}/power_utilities_checkpoint")
# MAGIC       .trigger(Trigger.ProcessingTime("1 second"))
# MAGIC       .outputMode("append")
# MAGIC       .table("power_utilities_demo_data")
# MAGIC 
# MAGIC display(df_raw); // display the dataframe

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup the 70:30 train:test split

# COMMAND ----------

# DBTITLE 1,Create the train and test datasets
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame;
# MAGIC 
# MAGIC """
# MAGIC   Separate the training and testing dataset into two dataframes
# MAGIC """
# MAGIC 
# MAGIC dfDataset = assembler.transform(df_sensors)
# MAGIC trainingDF, testingDF = dfDataset.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Training
# MAGIC 
# MAGIC For the purposes of this experiment, we will use MLFLOW to persist results and save models

# COMMAND ----------

# MAGIC %pip install mlflow

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
# MAGIC   Show the efffect of increasing the max depth
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

# MAGIC %md
# MAGIC 
# MAGIC #### Multi-Dimensional Cross Validation

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml import Pipeline
# MAGIC from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# MAGIC 
# MAGIC """
# MAGIC Do a cross validation of the decision tree model
# MAGIC """
# MAGIC 
# MAGIC # Set the decision tree that will be optimized
# MAGIC dt = DecisionTreeClassifier()\
# MAGIC             .setFeaturesCol("features")\
# MAGIC             .setLabelCol("cluster")\
# MAGIC             .setMaxDepth(5)\
# MAGIC             .setSeed(1)\
# MAGIC             .setPredictionCol("predictions")\
# MAGIC             .setMaxBins(4000)
# MAGIC 
# MAGIC # Build the grid of different parameters
# MAGIC paramGrid = ParamGridBuilder() \
# MAGIC     .addGrid(dt.maxDepth, [5, 10, 15]) \
# MAGIC     .addGrid(dt.maxBins, [4000, 5000, 6000]) \
# MAGIC     .build()
# MAGIC 
# MAGIC # Generate an average F1 score for each prediction
# MAGIC evaluator = MulticlassClassificationEvaluator()\
# MAGIC                   .setLabelCol("cluster")\
# MAGIC                   .setPredictionCol("predictions")
# MAGIC 
# MAGIC # Build out the cross validation
# MAGIC crossval = CrossValidator(estimator = dt,
# MAGIC                           estimatorParamMaps = paramGrid,
# MAGIC                           evaluator = evaluator,
# MAGIC                           numFolds = 3)  
# MAGIC pipelineCV = Pipeline(stages=[crossval])
# MAGIC 
# MAGIC # Train the model using the pipeline, parameter grid, and preceding BinaryClassificationEvaluator
# MAGIC cvModel_u = pipelineCV.fit(clusteredtrainingDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize the Optimal Decision Tree

# COMMAND ----------

# DBTITLE 1,Visualize Optimal Decision Tree
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Visualize the optimal decision tree
# MAGIC """
# MAGIC 
# MAGIC display(dtcTuning[7][1][0])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Forecasts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast the tier/bracket a covid case will belong to

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Label the clustered predictions 
# MAGIC """
# MAGIC 
# MAGIC # choose the third decision tree model
# MAGIC optimalDTCModel = dtcTuning[5][1][0]
# MAGIC 
# MAGIC # create the segmentation DF
# MAGIC segmentationDF = optimalDTCModel.transform(testingDF)
# MAGIC 
# MAGIC display(segmentationDF);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Deploy Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup a pipeline with an assembler and decision tree

# COMMAND ----------

# DBTITLE 1,Create Deployment Model Pipeline
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml import Pipeline, PipelineModel
# MAGIC 
# MAGIC """
# MAGIC   Combine the dataframe indexer and assembler with the decision tree
# MAGIC """
# MAGIC 
# MAGIC # Combine the existing models into a pipeline
# MAGIC deployment_ml_pipeline : Pipeline = Pipeline(stages = [assembler, optimalDTCModel])
# MAGIC 
# MAGIC # Create the pipeline transformer for the model by combining existing transformers
# MAGIC deployment_ml_pipeline_model : PipelineModel = deployment_ml_pipeline.fit(df_sensors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use model to predict on an example dataframe

# COMMAND ----------

# DBTITLE 1,Forecast Using Deployment Model Pipeline
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC """
# MAGIC Forecast some results on the Power Sensors dataset
# MAGIC """
# MAGIC 
# MAGIC # pull in streaming sensor data
# MAGIC df_sensors_stream : DataFrame = spark.sql("SELECT * FROM delta_power_stream")\
# MAGIC                                      .na\
# MAGIC                                      .drop()\
# MAGIC                                      .select(col("eventID"),
# MAGIC                                              col("AT").cast("float"),
# MAGIC                                              col("V").cast("float"),
# MAGIC                                              col("AP").cast("float"),
# MAGIC                                              col("RH").cast("float"),
# MAGIC                                              col("PE").cast("float"))
# MAGIC 
# MAGIC # classify the raw dataframe
# MAGIC df_deployed_pipeline_classification : DataFrame = deployment_ml_pipeline_model.transform(df_sensors_stream)\
# MAGIC                                                                               .select(col("eventID"),
# MAGIC                                                                                       col("AT"),
# MAGIC                                                                                       col("AP"),
# MAGIC                                                                                       col("RH"),
# MAGIC                                                                                       col("PE"),
# MAGIC                                                                                       col("predictions"))
# MAGIC   
# MAGIC df_deployed_pipeline_classification.createOrReplaceTempView("delta_power_stream_segmented")
# MAGIC 
# MAGIC # visualize the results
# MAGIC display(df_deployed_pipeline_classification)

# COMMAND ----------

# DBTITLE 1,Real Time Streaming Sensor Classification
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look at the live classification counts for streaming sensor data
# MAGIC --
# MAGIC 
# MAGIC SELECT predictions,
# MAGIC        count(eventID) as counts
# MAGIC FROM delta_power_stream_segmented
# MAGIC GROUP BY predictions;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Register the model with MLFlow registry

# COMMAND ----------

# DBTITLE 1,Register Deployment Model with MLFlow
# MAGIC %python
# MAGIC 
# MAGIC import mlflow
# MAGIC from mlflow.models import ModelSignature
# MAGIC 
# MAGIC """
# MAGIC Log the mlflow model to the registry
# MAGIC """
# MAGIC 
# MAGIC model_version_major = 1
# MAGIC model_version_minor = 2
# MAGIC 
# MAGIC with mlflow.start_run() as run:
# MAGIC   # get the dataframe signature for the model
# MAGIC   _signature = mlflow.models.infer_signature(df_sensors\
# MAGIC                                               .drop("ingested_time")\
# MAGIC                                               .drop("date")\
# MAGIC                                               .drop("Last_Update"),
# MAGIC                                             df_deployed_pipeline_classification\
# MAGIC                                               .drop("features")\
# MAGIC                                               .drop("ingested_time")\
# MAGIC                                               .drop("probability")\
# MAGIC                                               .drop("rawPrediction"))
# MAGIC   # Log the model
# MAGIC   mlflow.spark.log_model(spark_model = deployment_ml_pipeline_model,
# MAGIC                          signature = _signature,
# MAGIC                          registered_model_name = "power_utilities_segmenter",
# MAGIC                          artifact_path = f"pipeline_model_v{model_version_major}.{model_version_minor}"
# MAGIC                         )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access the model via MLFlow Python API

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Prepare data

# COMMAND ----------

# DBTITLE 1,Create JSON Data
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Generate JSON records
# MAGIC """
# MAGIC 
# MAGIC json_records = df_sensors.limit(100).toPandas().to_json(orient='split')
# MAGIC print(json_records)

# COMMAND ----------

# DBTITLE 1,Read Data as Pandas Dataframe
import pandas as pd

"""
Read JSON Data as a Pandas dataframe
"""

pd.read_json(json_records, orient = 'split')

# COMMAND ----------

# DBTITLE 1,Access Deployed Model using Python API
# MAGIC %python
# MAGIC 
# MAGIC import os
# MAGIC import requests
# MAGIC import pandas as pd
# MAGIC 
# MAGIC """
# MAGIC Example scoring model from MLFlow UI
# MAGIC Need to specify bearer token to connect to current MLFlow deployment
# MAGIC """
# MAGIC 
# MAGIC # define the model scoring function
# MAGIC def score_model(dataset: pd.DataFrame):
# MAGIC   url = 'https://field-eng.cloud.databricks.com/model/power_utilities_segmenter/3/invocations'
# MAGIC   headers = {'Authorization': f'Bearer {dbutils.secrets.get("ml-ml", "TOKEN")}'}
# MAGIC   data_json = json_records
# MAGIC   response = requests.request(method='POST', headers=headers, url=url, json=data_json)
# MAGIC   if response.status_code != 200:
# MAGIC     raise Exception(f'Request failed with status {response.status_code}, {response.text}')
# MAGIC   return response.json()
# MAGIC 
# MAGIC # score the model
# MAGIC score_model(df_dataset)
