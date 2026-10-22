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
# META         },
# META         {
# META           "id": "a3e6a648-1c66-41a1-aba4-da96b1d17eaa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **sample1**

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh1_cap.dbo.orders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/data/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/data/products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sparkr


# METADATA ********************

# META {
# META   "language": "r",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from lh1_cap.dbo.orders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
