# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fda80a9a-fd49-484b-b1f2-317d3912abea",
# META       "default_lakehouse_name": "lh_malaria_silver",
# META       "default_lakehouse_workspace_id": "815e9f81-df95-4e8e-a776-a6188b9e4758",
# META       "known_lakehouses": [
# META         {
# META           "id": "fda80a9a-fd49-484b-b1f2-317d3912abea"
# META         },
# META         {
# META           "id": "dbb2b297-f723-400f-adbb-e7d56d7feb89"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

print("🚀 SILVER LAYER STARTED (FINAL FIXED VERSION)")

# ─────────────────────────────────────────────
# LOAD BRONZE (UNCHANGED)
# ─────────────────────────────────────────────

who_bronze = spark.read.format("delta").load(
    "abfss://815e9f81-df95-4e8e-a776-a6188b9e4758@onelake.dfs.fabric.microsoft.com/dbb2b297-f723-400f-adbb-e7d56d7feb89/Tables/dbo/bronze_malaria_who_gho"
)

wb_bronze = spark.read.format("delta").load(
    "abfss://815e9f81-df95-4e8e-a776-a6188b9e4758@onelake.dfs.fabric.microsoft.com/dbb2b297-f723-400f-adbb-e7d56d7feb89/Tables/dbo/bronze_worldbank_context"
)

psi_bronze = spark.read.format("delta").load(
    "abfss://815e9f81-df95-4e8e-a776-a6188b9e4758@onelake.dfs.fabric.microsoft.com/dbb2b297-f723-400f-adbb-e7d56d7feb89/Tables/dbo/bronze_psi_program_data"
)

# ─────────────────────────────────────────────
# INCREMENTAL RUN ID (CORRECT COLUMN)
# ─────────────────────────────────────────────

latest_run = who_bronze.agg(F.max("_run_id")).collect()[0][0]

who_inc = who_bronze.filter(F.col("_run_id") == latest_run)
wb_inc  = wb_bronze.filter(F.col("_run_id") == latest_run)
psi_inc = psi_bronze.filter(F.col("_run_id") == latest_run)

print(f"📌 Run ID: {latest_run}")

# ─────────────────────────────────────────────
# COUNTRY DIM
# ─────────────────────────────────────────────

def hash_country(x):
    return hashlib.sha256(x.encode()).hexdigest()[:16]

hash_udf = F.udf(hash_country, StringType())

country_map_df = spark.createDataFrame([
    ("KEN","Kenya"),("UGA","Uganda"),("TZA","Tanzania"),("ETH","Ethiopia"),
    ("NGA","Nigeria"),("GHA","Ghana"),("MOZ","Mozambique"),("ZMB","Zambia"),
    ("MLI","Mali"),("CMR","Cameroon"),("SEN","Senegal"),("BFA","Burkina Faso"),
    ("MDG","Madagascar"),("MWI","Malawi"),("ZWE","Zimbabwe"),
    ("CIV","Cote d'Ivoire"),("COD","DR Congo"),("TCD","Chad")
], ["country_code","country_name"])

all_countries = (
    who_inc.select("country_code")
    .union(wb_inc.select("country_code"))
    .union(psi_inc.select("country_code"))
    .distinct()
)

dim_country = all_countries.join(country_map_df, "country_code", "left") \
    .fillna({"country_name": "Unknown"}) \
    .withColumn("country_key", hash_udf(F.col("country_code")))

dim_country.write.format("delta").mode("overwrite").save("Tables/silver/dim_country")

country_dim = dim_country.select("country_code", "country_key")

print("✅ Country dimension created")

# ─────────────────────────────────────────────
# WHO FACT (FIXED)
# ─────────────────────────────────────────────

fact_who = who_inc.join(country_dim, "country_code", "left") \
    .withColumn(
        "IndicatorName",
        F.expr("""
            CASE indicator_name
            WHEN 'malaria_incidence' THEN 'Malaria Incidence'
            WHEN 'malaria_deaths' THEN 'Malaria Deaths'
            ELSE indicator_name
            END
        """)
    ) \
    .select(
        F.col("country_key").alias("CountryKey"),
        F.col("IndicatorName"),
        F.col("year").alias("Year"),
        F.col("indicator_value").alias("Value"),
        F.col("low_value").alias("LowValue"),
        F.col("high_value").alias("HighValue"),
        F.col("_run_id").alias("RunId"),          # ✅ FIXED
        F.col("_ingested_at").alias("IngestedAt")
    )

fact_who.write.format("delta").mode("overwrite").save("Tables/silver/fact_malaria_burden")

print("✅ WHO fact written")

# ─────────────────────────────────────────────
# WORLD BANK FACT (FIXED COLUMN NAME)
# ─────────────────────────────────────────────

fact_wb = wb_inc.join(country_dim, "country_code", "left") \
    .withColumn(
        "IndicatorName",
        F.expr("""
            CASE indicator_name
            WHEN 'health_expenditure_pct_gdp' THEN 'Health Expenditure Percent to GDP'
            WHEN 'population_total' THEN 'Total Population'
            WHEN 'gdp_per_capita' THEN 'GDP Per Capita'
            ELSE indicator_name
            END
        """)
    ) \
    .select(
        F.col("country_key").alias("CountryKey"),
        F.col("IndicatorName"),
        F.col("year").alias("Year"),
        F.col("indicator_value").alias("Value"),
        F.col("_run_id").alias("RunId"),          # ✅ FIXED HERE TOO
        F.col("_ingested_at").alias("IngestedAt")
    )

fact_wb.write.format("delta").mode("overwrite").save("Tables/silver/fact_health_context")

print("✅ WB fact written")

# ─────────────────────────────────────────────
# PSI FACT (UNCHANGED LOGIC)
# ─────────────────────────────────────────────

fact_psi = psi_inc.join(country_dim, "country_code", "left") \
    .withColumn("EfficiencyScore", F.col("coverage_pct") / F.col("quality_score")) \
    .withColumn("UnitCost", F.col("expenditure_usd") / F.col("beneficiaries_reached")) \
    .select(
        F.col("country_key").alias("CountryKey"),
        F.col("program_name").alias("ProgramName"),
        F.col("year").alias("Year"),
        F.col("_run_id").alias("RunId"),
        F.col("_ingested_at").alias("IngestedAt"),
        F.col("EfficiencyScore"),
        F.col("UnitCost")
    )

fact_psi.write.format("delta").mode("overwrite").save("Tables/silver/fact_psi_programs")

print("✅ PSI fact written")

print("\n🎯 SILVER LAYER COMPLETE (FULLY FIXED)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
