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

# Initialize Spark (Fabric notebook)
spark = SparkSession.builder.getOrCreate()

def fetch_disbursements(constid, max_retries=5):
    url = f"https://crm.ngcdf.go.ke/api/bootstrap?n=mobile-disbursements&constid={constid}"
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
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                # Too Many Requests - wait and then retry
                wait_time = 5 * (attempts + 1)
                print(f"Warning: constid={constid} returned status 429, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Warning: constid={constid} returned status {r.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching constid={constid}: {e}")
            return None
        attempts += 1
    # If all attempts fail, return None
    return None

all_data = []

# Loop through constituency IDs from 1 to 290.
for i in range(1, 291):
    data = fetch_disbursements(i)
    if not data:
        continue

    payload = data.get("PAYLOAD", [])
    if len(payload) < 2:
        continue

    # The first element contains the constituency name.
    constituency_name = payload[0].get("Disbursement", f"Unknown_{i}")

    # Remaining elements are disbursement entries.
    for row in payload[1:]:
        date_val = row.get("Date", None)
        amount_val = row.get("Amount", None)
        fy_val = row.get("FY", None)

        all_data.append({
            "Constituency": constituency_name,
            "Date": date_val,
            "Amount": amount_val,
            "FinancialYear": fy_val
        })
    
    # Be polite to the API by waiting a little between requests.
    time.sleep(1)

print(f"Total disbursement rows collected: {len(all_data)}")

if not all_data:
    raise Exception("No data collected from the API. Please check your code or API limits.")

df = spark.createDataFrame(all_data)
df.show(30, truncate=False)

# Optional: Clean the Amount field (remove commas and cast to double)
df_cleaned = (
    df.withColumn("AmountCleaned", F.regexp_replace(F.col("Amount"), ",", "").cast("double"))
      .drop("Amount")
      .withColumnRenamed("AmountCleaned", "Amount")
)
df_cleaned.show(10, truncate=False)

# Write to Delta table in your Lakehouse
output_path = "Tables/All_NGCDF_Disbursements"
df_cleaned.write.format("delta").mode("overwrite").save(output_path)
print("Disbursement data saved to Delta at:", output_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
