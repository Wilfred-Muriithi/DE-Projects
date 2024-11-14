# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a29dff8c-4ee2-4a3c-9e0f-b2a0ab37b3e0",
# META       "default_lakehouse_name": "Analysis_lakehouse",
# META       "default_lakehouse_workspace_id": "e557fe1e-2312-4415-90f8-886e6fdb28a3"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "6fba9a6f-b6fc-4b44-a84b-a9e424470458",
# META       "known_warehouses": [
# META         {
# META           "id": "6fba9a6f-b6fc-4b44-a84b-a9e424470458",
# META           "type": "Lakewarehouse"
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

# CELL ********************

import pandas as pd
import numpy as np

# Set seed for reproducibility
np.random.seed(42)

# Sample data generation parameters
num_samples = 1000
ages = np.random.randint(18, 70, num_samples)
genders = np.random.choice(['Male', 'Female', 'Other'], num_samples)
locations = np.random.choice(['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret', 'Thika', 'Naivasha', 'Nyeri', 'Meru', 'Garissa'], num_samples)
frequencies = np.random.choice(['Daily', 'Weekly', 'Monthly', 'Rarely'], num_samples)
satisfaction_ratings = np.random.randint(1, 11, num_samples)
product_quality_ratings = np.random.randint(1, 11, num_samples)
customer_service_ratings = np.random.randint(1, 11, num_samples)
recommend_to_others = np.random.randint(1, 11, num_samples)
comments = np.random.choice([
    'Great product, very satisfied.', 
    'Good value for money.', 
    'Average experience.', 
    'Not very satisfied.', 
    'Excellent product and service.',
    'Needs improvement.',
    'Will not recommend.',
    'Customer service was helpful.',
    'Product quality exceeded expectations.',
    'Too expensive for the value provided.'], num_samples)

# Create DataFrame
survey_data = pd.DataFrame({
    'RespondentID': range(1, num_samples + 1),
    'Age': ages,
    'Gender': genders,
    'Location': locations,
    'ProductUsageFrequency': frequencies,
    'SatisfactionRating': satisfaction_ratings,
    'ProductQualityRating': product_quality_ratings,
    'CustomerServiceRating': customer_service_ratings,
    'RecommendToOthers': recommend_to_others,
    'Comments': comments
})

# Save to CSV
survey_data.to_csv("survey_data.csv")



#To a spark data flame
spark_df = spark.createDataFrame(survey_data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_data.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SurveyDataProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save DataFrame to Delta Lake
delta_table_path = "abfss://e557fe1e-2312-4415-90f8-886e6fdb28a3@onelake.dfs.fabric.microsoft.com/a29dff8c-4ee2-4a3c-9e0f-b2a0ab37b3e0/Files/survey_data_delta"
spark_df.write.format("delta").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load Delta table
survey_df = spark.read.format("delta").load(delta_table_path)

# Perform basic data analysis
import pyspark.sql.functions as F

# Calculate summary statistics
summary_stats = survey_df.describe().show()

# Example of data transformation
transformed_df = survey_df.withColumn("age_group", F.when(survey_df.Age < 18, "Child")
                                              .when((survey_df.Age >= 18) & (survey_df.Age < 65), "Adult")
                                              .otherwise("Senior"))

# Example of aggregation
age_group_counts = transformed_df.groupBy("age_group").count()

# Collect data for visualization
age_group_counts_pd = age_group_counts.toPandas()

# Plot
import matplotlib.pyplot as plt

plt.bar(age_group_counts_pd['age_group'], age_group_counts_pd['count'])
plt.xlabel('Age Group')
plt.ylabel('Count')
plt.title('Survey Respondents by Age Group')
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
