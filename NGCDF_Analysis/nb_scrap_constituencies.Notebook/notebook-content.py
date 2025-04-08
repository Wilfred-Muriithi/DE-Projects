# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1b8c60cb-f48f-4385-82e5-8b80b6a6b934",
# META       "default_lakehouse_name": "lh_cdf_bronze",
# META       "default_lakehouse_workspace_id": "6288070a-c8f5-41e3-8903-921d7faf8199",
# META       "known_lakehouses": [
# META         {
# META           "id": "1b8c60cb-f48f-4385-82e5-8b80b6a6b934"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session (already running in Fabric notebooks)
spark = SparkSession.builder.getOrCreate()

# Define the target URL
url = "https://ngcdf.go.ke/constituency/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Request the page
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"Failed to fetch page. Status code: {response.status_code}")

# Parse HTML with BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Locate the specific table by its class
table = soup.find("table", {"class": "table table-striped table-borderless constituencies-counties-table table-responsive"})
if not table:
    raise Exception("Could not find the constituencies-counties-table in the page HTML.")

# 6. Extract the rows
rows = table.find_all("tr")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare a container for data
scraped_data = []

# Loop through each row and extract cell text
for row in rows:
    cells = row.find_all("td")
    # Skip the header row (which might be <th> or a row with fewer columns)
    if len(cells) < 3:
        continue

    no_cell = cells[0].get_text(strip=True)
    county_cell = cells[1].get_text(strip=True)
    constituencies_cell = cells[2].get_text(strip=True)

    # Build a dictionary for each row
    scraped_data.append({
        "No": no_cell,
        "County": county_cell,
        "Constituencies": constituencies_cell
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Create a Spark DataFrame
df = spark.createDataFrame(scraped_data)

# 9. Show the DataFrame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Split on commas to create an array, then explode into multiple rows
df_exploded = (
    df.withColumn(
        "ConstituencyList",
        F.split(F.col("Constituencies"), ",")  # splits on commas
    )
    .withColumn(
        "Constituency",
        F.explode("ConstituencyList")  # creates a new row for each item in the array
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean up each constituency
#    - Remove leading numbers + dot (e.g. "1." or "10.") using regex
#    - Trim whitespace
df_cleaned = (
    df_exploded.withColumn(
        "Constituency",
        F.trim(F.regexp_replace(F.col("Constituency"), r"^[0-9]+\.", ""))
    )
    # Optionally drop original columns or rename them as needed
    .drop("Constituencies", "ConstituencyList")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Filter out any empty rows that might occur if there's a trailing comma
df_final = df_cleaned.filter(F.col("Constituency") != "")

# 4. Show the final result
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rename the 'No' column to 'County Number'
df_final = df_final.withColumnRenamed("No", "County_Number")

# Select columns in the desired order
df_final = df_final.select("Constituency", "County", "County_Number")

# Show the results
display(df_final)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_path = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_bronze.Lakehouse/Tables/Constituencies"

# Write the DataFrame as Delta, overwriting any existing data
df_final.write.format("delta").mode("overwrite").save(output_path)

print("DataFrame saved as Delta to:", output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
