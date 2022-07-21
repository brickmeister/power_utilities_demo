# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Setup some environmental variables

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Setup the database for loading data
# MAGIC """
# MAGIC 
# MAGIC user : str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0].replace(".", "_")
# MAGIC database : str = '_'.join(["power_utilities", user])
# MAGIC 
# MAGIC try:
# MAGIC   spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
# MAGIC   ## set the database as a databricks task value for multitask workflows
# MAGIC   ## this can be read by other tasks in a workflow
# MAGIC   dbutils.jobs.taskValues.set("database", database)
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"Couldn't set database, {err}")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Print some values
# MAGIC """
# MAGIC 
# MAGIC print(database)
