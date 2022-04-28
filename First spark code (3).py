# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum,avg,min,max,count,mean
spark = SparkSession.builder.appName("Mini Project-2").getOrCreate()


# COMMAND ----------

df = spark.read.options(header="True").csv("/FileStore/tables/OfficeDataProject.csv")                                    
df.show()

# COMMAND ----------

df.select("employee_id").count()

# COMMAND ----------

df.select(col("department")).distinct().count()


# COMMAND ----------

df2=df.select(col("department").alias("Department Names")).distinct()
df2.show()

# COMMAND ----------

df3=df.groupBy(col("department")).count()
df3.show()

# COMMAND ----------

df4=df.groupBy(col("state")).count()
df4.show()

# COMMAND ----------

df5=df.groupBy(col("state"), col("department")).count()
df5.show()

# COMMAND ----------

df6=df.groupBy(col("department")).agg(min("salary").alias("Minimum Salary") , max("salary").alias("Maximum Salary"))
df6.sort(col("Minimum Salary").asc(), col("Maximum Salary").asc()).show()

# COMMAND ----------

average_bonus = df.filter(df.state == "NY").groupBy("state").agg(avg("bonus").alias("avg_bonus")).select("avg_bonus").collect()[0]["avg_bonus"]
df.filter((df.state=="NY") & (df.department == "Finance") & (df.bonus > average_bonus)).show()

# COMMAND ----------

def raise_salary(age,salary):
    if age > 45 :
        return salary+500
    else:
        return salary
TotalSalary = udf(lambda x,y : raise_salary(x,y), IntegerType())
df.withColumn("Increment", TotalSalary(col("age").cast("Integer"), col("salary").cast("Integer"))).show()


# COMMAND ----------

df=df.filter((col("age").cast("Integer")) > 45)
df.write.options(header="True").csv("/FileStore/tables/OfficeDataProject/sub_file2")
df = spark.read.options(header="True").csv("/FileStore/tables/OfficeDataProject/sub_file2")
df.show()

# COMMAND ----------


