# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dbb2b297-f723-400f-adbb-e7d56d7feb89",
# META       "default_lakehouse_name": "lh_malaria_bronze",
# META       "default_lakehouse_workspace_id": "815e9f81-df95-4e8e-a776-a6188b9e4758",
# META       "known_lakehouses": [
# META         {
# META           "id": "dbb2b297-f723-400f-adbb-e7d56d7feb89"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1bbea9c7-9401-81ab-4a1c-51ea51c6a013",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# =============================================================
# Notebook: 01_bronze_ingestion
# Layer   : BRONZE — Raw API Ingestion → Delta Tables
# Fabric   : Microsoft Fabric Lakehouse (NO schema switching)
# =============================================================

# ── Cell 1: Parameters ───────────────────────────────────────
START_YEAR = 2015
END_YEAR   = 2023
RUN_ID     = None

TARGET_COUNTRIES = [
    "KEN","TZA","UGA","ETH","NGA","GHA",
    "MOZ","ZMB","MLI","CMR","SEN","BFA",
    "MDG","MWI","ZWE","CIV","COD","TCD"
]

# ── Cell 2: Imports ──────────────────────────────────────────
import uuid, logging, random, requests, time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze")

RUN_ID = RUN_ID or f"bronze_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

print(f"🚀 Bronze Ingestion | {RUN_ID}")

# ── Cell 3: HTTP Helper ──────────────────────────────────────
def http_get(url, params=None):
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            time.sleep(2)
    return {}

def write_table(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# ── Cell 4: WHO INDICATORS (FIXED MODEL) ─────────────────────
WHO_INDICATORS = {
    "malaria_incidence": "MALARIA_EST_INCIDENCE",
    "malaria_deaths": "MALARIA_EST_DEATHS",
    "itn_coverage": "MALARIA_ITNUSE",
    "irs_coverage": "MALARIA_IRSPROTECTED",
    "act_treatment": "MALARIA_ACT_TREATMENT",
    "antenatal_ipt": "MALARIA_IPT2"
}

who_records = []

# ── Cell 5: WHO INGESTION (CORRECT API) ──────────────────────
for name, code in WHO_INDICATORS.items():
    print(f"📡 WHO: {name}")

    url = f"https://ghoapi.azureedge.net/api/{code}"
    data = http_get(url)

    for r in data.get("value", []):
        try:
            year = int(r.get("TimeDim"))
        except:
            continue

        country = r.get("SpatialDim")

        if (
            country in TARGET_COUNTRIES and
            START_YEAR <= year <= END_YEAR
        ):
            who_records.append({
                "indicator_name": name,
                "indicator_code": code,
                "country_code": country,
                "year": year,
                "indicator_value": r.get("NumericValue"),
                "low_value": r.get("Low"),
                "high_value": r.get("High"),
                "value_type": r.get("DataTypeName"),
                "comments": r.get("Comments"),
                "source_system": "WHO_GHO",
                "_run_id": RUN_ID,
                "_ingested_at": datetime.utcnow()
            })

schema_who = StructType([
    StructField("indicator_name",StringType()),
    StructField("indicator_code",StringType()),
    StructField("country_code",StringType()),
    StructField("year",IntegerType()),
    StructField("indicator_value",DoubleType()),
    StructField("low_value",DoubleType()),
    StructField("high_value",DoubleType()),
    StructField("value_type",StringType()),
    StructField("comments",StringType()),
    StructField("source_system",StringType()),
    StructField("_run_id",StringType()),
    StructField("_ingested_at",TimestampType())
])

df_who = spark.createDataFrame(who_records, schema_who)

write_table(df_who, "bronze_malaria_who_gho")

print(f"✅ WHO rows: {df_who.count()}")

# ── Cell 6: WORLD BANK ───────────────────────────────────────
WB_INDICATORS = {
    "health_expenditure_pct_gdp":"SH.XPD.CHEX.GD.ZS",
    "gdp_per_capita":"NY.GDP.PCAP.CD",
    "population_total":"SP.POP.TOTL"
}

wb_records = []

for name, code in WB_INDICATORS.items():
    print(f"📡 WB: {name}")

    for country in TARGET_COUNTRIES:
        url = f"https://api.worldbank.org/v2/country/{country}/indicator/{code}"
        data = http_get(url, {"format":"json","per_page":2000})

        if isinstance(data, list) and len(data) > 1:
            for r in data[1]:
                if r.get("value") is None:
                    continue

                try:
                    year = int(r.get("date"))
                except:
                    continue

                if START_YEAR <= year <= END_YEAR:
                    wb_records.append({
                        "indicator_name": name,
                        "indicator_code": code,
                        "country_code": country,
                        "country_name": r["country"]["value"],
                        "year": year,
                        "indicator_value": float(r["value"]),
                        "source_system": "WORLD_BANK",
                        "_run_id": RUN_ID,
                        "_ingested_at": datetime.utcnow()
                    })

schema_wb = StructType([
    StructField("indicator_name",StringType()),
    StructField("indicator_code",StringType()),
    StructField("country_code",StringType()),
    StructField("country_name",StringType()),
    StructField("year",IntegerType()),
    StructField("indicator_value",DoubleType()),
    StructField("source_system",StringType()),
    StructField("_run_id",StringType()),
    StructField("_ingested_at",TimestampType())
])

df_wb = spark.createDataFrame(wb_records, schema_wb)

write_table(df_wb, "bronze_worldbank_context")

print(f"✅ WB rows: {df_wb.count()}")

# ── Cell 7: PSI SYNTHETIC ────────────────────────────────────
random.seed(42)

psi_records = []

for c in TARGET_COUNTRIES:
    for y in range(START_YEAR, END_YEAR+1):
        psi_records.append({
            "program_name":"Net Distribution",
            "country_code":c,
            "year":y,
            "quarter":random.randint(1,4),
            "delivery_channel":"Community",
            "beneficiaries_reached":random.randint(5000,200000),
            "budget_usd":random.uniform(10000,2000000),
            "expenditure_usd":random.uniform(10000,2000000),
            "coverage_pct":random.uniform(40,95),
            "quality_score":random.uniform(60,100),
            "source_system":"PSI_INTERNAL",
            "_run_id":RUN_ID,
            "_ingested_at":datetime.utcnow()
        })

schema_psi = StructType([
    StructField("program_name",StringType()),
    StructField("country_code",StringType()),
    StructField("year",IntegerType()),
    StructField("quarter",IntegerType()),
    StructField("delivery_channel",StringType()),
    StructField("beneficiaries_reached",IntegerType()),
    StructField("budget_usd",DoubleType()),
    StructField("expenditure_usd",DoubleType()),
    StructField("coverage_pct",DoubleType()),
    StructField("quality_score",DoubleType()),
    StructField("source_system",StringType()),
    StructField("_run_id",StringType()),
    StructField("_ingested_at",TimestampType())
])

df_psi = spark.createDataFrame(psi_records, schema_psi)

write_table(df_psi, "bronze_psi_program_data")

print(f"✅ PSI rows: {df_psi.count()}")

# ── Cell 8: BASIC DQ ─────────────────────────────────────────
dq_results = [
    ("WHO_null_country", df_who.filter("country_code IS NULL").count()),
    ("WB_null_country", df_wb.filter("country_code IS NULL").count()),
    ("PSI_invalid_coverage", df_psi.filter("coverage_pct > 100").count())
]

df_dq = spark.createDataFrame(dq_results, ["check","issue_count"])

write_table(df_dq, "dq_audit_log")

print("✅ DQ complete")

# ── Cell 9: SUMMARY ──────────────────────────────────────────
print("\n==============================")
print("BRONZE COMPLETE")
print("==============================")

for t in [
    "bronze_malaria_who_gho",
    "bronze_worldbank_context",
    "bronze_psi_program_data",
    "dq_audit_log"
]:
    print(t, spark.table(t).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in [
    "bronze_malaria_who_gho",
    "bronze_worldbank_context",
    "bronze_psi_program_data",
    "dq_audit_log"
]:
    spark.sql(f"REFRESH TABLE {t}")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in [
    "bronze_malaria_who_gho",
    "bronze_worldbank_context",
    "bronze_psi_program_data",
    "dq_audit_log"
]:
    try:
        df = spark.table(t)
        print(t, "✔", df.count())
    except Exception as e:
        print(t, "❌ NOT FOUND:", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
