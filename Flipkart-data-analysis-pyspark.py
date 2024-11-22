# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

file_path = '/FileStore/tables/Flipkart-1.csv'

# COMMAND ----------

fk_df=spark.read.csv(file_path,header = "true",inferSchema = "true",mode = "permissive")
fk_df.show(10)

# COMMAND ----------

fk_df.printSchema()

# COMMAND ----------

fk_df.count()

# COMMAND ----------

#renaming collumn
raname_fk_df = fk_df.select("*",col("maincateg").alias("gender")).show()

# COMMAND ----------

#handling the missing data
fk_df.select([count(when(col(c).isNull(), c)).alias(c) for c in fk_df.columns]).display()

#drop the rows that is missing 
fk_df_clean=fk_df.dropna()

#filling specific values to the nan columns or missing columns
fk_df_filled=fk_df.fillna({"Rating":0,"maincateg":"Men"})

# COMMAND ----------


#hight rated products
high_rated_products = fk_df.filter((col("Rating") > 4))
high_rated_products.display(5)

# COMMAND ----------

#group by the category and calculte the average rating 
avg_rating_by_category=high_rated_products.groupBy("maincateg").avg("Rating")
avg_rating_by_category.display()

# COMMAND ----------

output_table='Flipkart_Data_Analysis_table'
fk_df.write.mode("overwrite").saveAsTable(output_table)
