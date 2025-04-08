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
import pyspark.sql.functions as F
import time

# Initialize Spark (in your Fabric notebook)
spark = SparkSession.builder.getOrCreate()

def fetch_allocations(constid, max_retries=5):
    url = f"https://crm.ngcdf.go.ke/api/bootstrap?n=mobile-allocations&constid={constid}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/110.0.5481.178 Safari/537.36"
        )
    }
    attempts = 0
    while attempts < max_retries:
        try:
            print(f"Fetching allocations for constid={constid}, attempt {attempts+1}")
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                print(f"Success: constid={constid}")
                return r.json()
            elif r.status_code == 429:
                wait_time = 5 * (attempts + 1)
                print(f"Warning: constid={constid} returned status 429. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Warning: constid={constid} returned status {r.status_code}. Skipping.")
                return None
        except Exception as e:
            print(f"Error fetching constid={constid}: {e}")
            return None
        attempts += 1
    print(f"Exceeded retries for constid={constid}. Skipping.")
    return None

all_allocations = []

# Loop through constituency IDs from 1 to 290
for i in range(1, 291):
    print(f"\nProcessing constituency ID: {i}")
    data = fetch_allocations(i)
    if not data:
        print(f"No data returned for constid={i}.")
        continue

    payload = data.get("PAYLOAD", [])
    if len(payload) < 2:
        print(f"Payload for constid={i} is too short.")
        continue

    # Extract the constituency name from the first dictionary with key "Allocations"
    constituency_name = payload[0].get("Allocations", f"Unknown_{i}")
    print(f"Constituency: {constituency_name}")

    count_rows = 0
    # Process allocation rows (each subsequent dictionary in the payload)
    for row in payload[1:]:
        amount_val = row.get("Amount", None)
        fy_val = row.get("FY", None)

        all_allocations.append({
            "Constituency": constituency_name,
            "Amount": amount_val,
            "FinancialYear": fy_val
        })
        count_rows += 1
    print(f"Collected {count_rows} allocation rows for constituency: {constituency_name}")
    
    # Wait a bit between requests to be polite
    time.sleep(1)

print(f"\nTotal allocation rows collected: {len(all_allocations)}")

if not all_allocations:
    raise Exception("No allocation data collected from the API. Please check your code or API limits.")

# Create a Spark DataFrame from the collected allocation data.
df_allocations = spark.createDataFrame(all_allocations)
print("\nSample allocation data:")
df_allocations.show(30, truncate=False)

# Optional: Clean the Amount field (remove commas and cast to double)
df_allocations_clean = (
    df_allocations.withColumn("AmountCleaned", F.regexp_replace(F.col("Amount"), ",", "").cast("double"))
                   .drop("Amount")
                   .withColumnRenamed("AmountCleaned", "Amount")
)
print("\nAllocation data after cleaning the Amount field:")
df_allocations_clean.show(10, truncate=False)

# Write the cleaned DataFrame to Delta in your Lakehouse.
output_path_alloc = "Tables/All_NGCDF_Allocations"
df_allocations_clean.write.format("delta").mode("overwrite").save(output_path_alloc)
print(f"\nAllocation data saved to Delta at: {output_path_alloc}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
