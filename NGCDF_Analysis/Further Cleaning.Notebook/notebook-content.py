# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c42b8b73-06d1-4c88-ab23-0c50920eb4b9",
# META       "default_lakehouse_name": "Further_Cleaning",
# META       "default_lakehouse_workspace_id": "6288070a-c8f5-41e3-8903-921d7faf8199",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee5e29f5-d673-4ec0-993f-0cd1af6d9e00"
# META         },
# META         {
# META           "id": "c42b8b73-06d1-4c88-ab23-0c50920eb4b9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *

# Load the Constituency Silver data (replace with the actual path)
silver_constituency_df = spark.read.format("Delta") .option("header", "true").option("inferSchema", "true").load("path_to_silver_constituency_data.csv")

silver_constituency_df.show()

# Define a window specification (no partitioning for a single list of IDs)
window_spec = Window.orderBy("Constituency")

# Add a sequential ID column starting from 1
dim_constituency_df = silver_constituency_df \
    .withColumn("Constituency_ID", row_number().over(window_spec))

dim_constituency_df.show()



# Save the Dim_Constituency table to the Gold layer
dim_constituency_df.write.format("parquet") \
    .mode("overwrite") \
    .save("path_to_gold_layer/Dim_Constituency")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
