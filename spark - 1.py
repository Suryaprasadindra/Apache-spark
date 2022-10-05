# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC This notebook will show you 
# MAGIC 1. how to create and query a table or DataFrame
# MAGIC 2. how to read schema
# MAGIC 3. how to convert date format(dd-MM-yyyy) to standard spark date format(yyyy-MM-dd)
# MAGIC 4. filling null values 
# MAGIC 5. how to create new column , extract year & month from date column
# MAGIC 5. partitioning table by year and table
# MAGIC 6. how to save table
# MAGIC 7. checking the partition files using linux command

# COMMAND ----------

# reading data
df = spark.read.csv('/FileStore/tables/emp-2.txt',header=True)
display(df)


# COMMAND ----------

# reading data and infering schema
df = spark.read.csv('/FileStore/tables/emp-2.txt',header=True,inferSchema=True)
display(df)
df.schema.fields
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date
df = df.withColumn("HIREDATE",to_date("HIREDATE",'dd-MM-yyy')).fillna({"HIREDATE":'9999-12-31'}) # converting data format to standard spark date format
df.show()
df.printSchema()
df.schema

# COMMAND ----------

from pyspark.sql.functions import date_format
df = df.withColumn("year",date_format('HIREDATE','yyyy'))\   # creating year and month columns
       .withColumn("month",date_format('HIREDATE','MM'))
df.show()

# COMMAND ----------

df.write.format("delta").partitionBy("year","month").mode("overwrite").saveAsTable('partition_dataframe')

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/partition_dataframe/year=1981         # checking files localtion

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from partition_df where year = 1981       
