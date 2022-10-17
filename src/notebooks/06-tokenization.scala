// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC --
// MAGIC -- Uncensored data
// MAGIC --
// MAGIC 
// MAGIC select *
// MAGIC from hive_metastore.power_utilities_mark_lee.feature_sensor_data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --
// MAGIC -- Create a secure view to censor data
// MAGIC --
// MAGIC 
// MAGIC create or replace temporary view secure_feature_sensor_data as
// MAGIC   select AT,
// MAGIC          V,
// MAGIC          AP,
// MAGIC          RH,
// MAGIC          PE,
// MAGIC          case when IS_MEMBER('group1')
// MAGIC            then UUID
// MAGIC            ELSE sha2(uuid, 256)
// MAGIC          end as uuid
// MAGIC  from hive_metastore.power_utilities_mark_lee.feature_sensor_data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --
// MAGIC -- Select from the secure view
// MAGIC --
// MAGIC 
// MAGIC Select *
// MAGIC From secure_feature_sensor_data;
