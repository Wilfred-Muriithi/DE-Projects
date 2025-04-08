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
# META           "id": "c42b8b73-06d1-4c88-ab23-0c50920eb4b9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import Window
# Load the Constituency Silver data (replace with the actual path)
silver_constituency_df = spark.read.format("Delta").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/Constituencies")

# Define a window specification (no partitioning for a single list of IDs)
window_spec = Window.orderBy("Constituency")

# Add a sequential ID column starting from 1
dim_constituency_df = silver_constituency_df \
    .withColumn("Constituency_ID", row_number().over(window_spec))
dim_constituency_df = dim_constituency_df['Constituency_ID', 'Constituency']
# Save the Dim_Constituency table to the Gold layer
dim_constituency_df.write.format("delta").mode("overwrite").saveAsTable("dim_constituency")
display(dim_constituency_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the Silver layer County data (replace with actual file path)
silver_county_df = spark.read.format("delta").option("header", "true").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/Constituencies")

# Select only the required columns and rename them for clarity
# Change datatype of 'County_Number' to int
dim_county_df = silver_county_df.withColumn("County_Number", col("County_Number").cast("int"))
dim_county_df = dim_county_df['County_Number', 'County'].distinct().orderBy('County_Number')


# Save the Dim_County table to the Gold layer
dim_county_df.write.format("delta").mode("overwrite").saveAsTable("dim_county")
display(dim_county_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Load the Dim_Constituency data
dim_constituency_df = spark.read.format("delta").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/dim_constituency")

# Load Allocations data
allocations_df = spark.read.format("delta").option("header", "true").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/allocation_cleant")
dim_allocation = allocations_df.withColumnRenamed('County_Number', 'County_Numbera').withColumnRenamed('Amount', 'Allocated_Amount').withColumnRenamed('AmountInMillions', 'Allocated_AmountInMillions')
dim_allocation = dim_allocation.withColumn("Start_Year", split(dim_allocation["FinancialYear"], "/")[0]) \
       .withColumn("End_Year", split(dim_allocation["FinancialYear"], "/")[1])

# Create Start_Date and End_Date columns
dim_allocation = dim_allocation.withColumn("AllocationStart_Date", to_date(concat(dim_allocation["Start_Year"], lit("-07-01")))) \
       .withColumn("AllocationEnd_Date", to_date(concat(dim_allocation["End_Year"], lit("-06-30"))))

dim_allocation.write.format('delta').mode('overwrite').saveAsTable('dim_allocation')
#display (dim_allocation)




# Load Disbursements data
disbursements_df = spark.read.format("delta").option("header", "true").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/distribution_cleaned")
dim_disbursement= disbursements_df.withColumnRenamed('Amount', 'Disbursed_Amount').withColumnRenamed('AmountInMillions', 'Disbursed_AmountInMillions')
# Split the Financial_Year column into Start_Year and End_Year
dim_disbursement = dim_disbursement.withColumn("Start_Year", split(dim_disbursement["FinancialYear"], "/")[0]) \
       .withColumn("End_Year", split(dim_disbursement["FinancialYear"], "/")[1])

# Create Start_Date and End_Date columns
dim_disbursement = dim_disbursement.withColumn("Start_Date", to_date(concat(dim_disbursement["Start_Year"], lit("-07-01")))) \
       .withColumn("End_Date", to_date(concat(dim_disbursement["End_Year"], lit("-06-30"))))
dim_disbursement = dim_disbursement.withColumn("Disbursement_Date", to_date(dim_disbursement["Date"], "MMM dd, yyyy"))
dim_disbursement.write.format('delta').mode('overwrite').saveAsTable('dim_disbursement')
#display(dim_disbursement)

# Join allocations and disbursements on 'Constituency' and 'Financial_Year'
merged_df = dim_allocation.join(
    dim_disbursement,
    on=["Constituency", "FinancialYear"],
    how="inner"
).select(
    col("Constituency"),
    col("FinancialYear"),
    col("Allocated_Amount"),
    col("Disbursed_Amount"),
    col("County_Numbera"),
    col('Disbursement_Date')
      # Assuming this column is in one of the tables
)
#display(merged_df)


# Load Dim_Constituency data
dim_constituency_df = spark.read.format("delta").load("abfss://NGCDF@onelake.dfs.fabric.microsoft.com/Further_Cleaning.Lakehouse/Tables/dim_constituency")

# Add Constituency_ID to the merged data
fact_table_df = merged_df.join(
    dim_constituency_df,
    on=["Constituency"],  # Joining on Constituency
    how="inner"
).select(
    col("Constituency_ID"),
    col('County_Numbera'),
    col("Allocated_Amount"),
    col("Disbursed_Amount"),
    col("FinancialYear"),
    col('Disbursement_Date')
).orderBy('Constituency_ID')

display(fact_table_df)
fact_table_df.write.format('delta').saveAsTable('Fact_Table')
'''
# Add Disbursement_Ratio column
fact_table_df = fact_table_df.withColumn(
    "Disbursement_Ratio",
    (col("Disbursed_Amount") / col("Allocated_Amount")).alias("Disbursement_Ratio")
)
'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
