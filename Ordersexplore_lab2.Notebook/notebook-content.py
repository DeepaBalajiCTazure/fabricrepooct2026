# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8c38520b-e35a-4e64-967e-b7b5c78a7acc",
# META       "default_lakehouse_name": "lh1_cap",
# META       "default_lakehouse_workspace_id": "7907f786-2ea6-4931-8673-3671cc769255",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c38520b-e35a-4e64-967e-b7b5c78a7acc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # # Sales order data exploration
# Use this notebook to explore sales order data

# MARKDOWN ********************

# /lakehouse/default/Files/orders/2019.csv
# abfss://ws1techM@onelake.dfs.fabric.microsoft.com/techmLH.Lakehouse/Files/orders/2021.csv   
# Files/orders/2021.csv

# CELL ********************

 df = spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")
 # df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
 display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dftemp= spark.read.format("csv").option("header","false").option("inferSchema",True).load("Files/orders/2019.csv")
display(dftemp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.types import *

 orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
 ])

 df = spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")

 display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Filter a DataFrame

# CELL ********************

customers = df['CustomerName', 'Email']

print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# SELECT COLUMNS

# CELL ********************

 customers = df.select("CustomerName", "Email")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
 print(customers.count())
 print(customers.distinct().count())

 display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# AGGREGATE OPERATION

# CELL ********************

 productSales = df.select("Item", "Quantity").groupBy("Item").sum()

 display(productSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.functions import *

 yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

 display(yearlySales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **transform data files**

# CELL ********************

 from pyspark.sql.functions import *

 # Create Year and Month columns
 transformed_df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

 # Create the new FirstName and LastName fields
 transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

 # Filter and reorder columns
 transformed_df = transformed_df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]

 # Display the first five orders
 display(transformed_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')

 print ("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 orders_df = spark.read.format("parquet").load("Files/transformed_data/orders")
 display(orders_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **APPLY PARTITION BY TO SAVE IN FOLDERS**

# CELL ********************

 orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")

 print ("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 orders_2021_df = spark.read.format("parquet").load("Files/partitioned_data/Year=2019/Month=*")

 display(orders_2021_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Work with tables and SQL**

# CELL ********************

# Create a new table
df.write.format("delta").saveAsTable("salesorders")

# Get the table description
spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from salesorders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 df = spark.sql("SELECT * FROM salesorders LIMIT 1000")

 display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 SELECT YEAR(OrderDate) AS OrderYear,
        SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
 FROM salesorders
 GROUP BY YEAR(OrderDate)
 ORDER BY OrderYear;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Visualize data with Spark**

# CELL ********************

 %%sql
 SELECT * FROM salesorders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, 
# MAGIC                  SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
# MAGIC              FROM salesorders 
# MAGIC              GROUP BY CAST(YEAR(OrderDate) AS CHAR(4))
# MAGIC              ORDER BY OrderYear

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                 SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
             FROM salesorders \
             GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
             ORDER BY OrderYear"
 df_spark = spark.sql(sqlQuery)
 df_spark.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("view1sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from view1sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
