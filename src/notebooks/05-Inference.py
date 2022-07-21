# Databricks notebook source
# MAGIC %md
# MAGIC ## Forecast the tier/bracket a sensor signal will belong to

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