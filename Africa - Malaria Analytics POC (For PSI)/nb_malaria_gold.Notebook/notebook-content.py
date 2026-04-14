# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "75cc645c-e3e3-4c26-8e7c-a567f9125ad6",
# META       "default_lakehouse_name": "lh_malaria_gold",
# META       "default_lakehouse_workspace_id": "815e9f81-df95-4e8e-a776-a6188b9e4758",
# META       "known_lakehouses": [
# META         {
# META           "id": "75cc645c-e3e3-4c26-8e7c-a567f9125ad6"
# META         },
# META         {
# META           "id": "fda80a9a-fd49-484b-b1f2-317d3912abea"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

print("🏆 GOLD ELITE PIPELINE STARTED")

# ─── CONFIG ──────────────────────────────────────────────────
BASE_PATH = "abfss://815e9f81-df95-4e8e-a776-a6188b9e4758@onelake.dfs.fabric.microsoft.com/fda80a9a-fd49-484b-b1f2-317d3912abea/Tables/silver"
GOLD_PATH = "Tables/gold"
RUN_TS = datetime.utcnow()

# ─── LOAD SILVER ─────────────────────────────────────────────
fact_malaria       = spark.read.format("delta").load(f"{BASE_PATH}/fact_malaria_burden")
fact_health        = spark.read.format("delta").load(f"{BASE_PATH}/fact_health_context")
fact_psi           = spark.read.format("delta").load(f"{BASE_PATH}/fact_psi_programs")
dim_country_silver = spark.read.format("delta").load(f"{BASE_PATH}/dim_country")

dim_country = dim_country_silver.select(
    F.col("country_key").alias("CountryKey"),
    F.col("country_code").alias("CountryCode"),
    F.col("country_name").alias("CountryName")
)

print("✅ Silver loaded")

# ─── INCREMENTAL FILTER ───────────────────────────────────────
latest_run   = fact_malaria.agg(F.max("RunId")).collect()[0][0]
fact_malaria = fact_malaria.filter(F.col("RunId") == latest_run)
fact_health  = fact_health.filter(F.col("RunId") == latest_run)
fact_psi     = fact_psi.filter(F.col("RunId") == latest_run)
print(f"📌 Incremental RunId: {latest_run}")

# ─── NORMALISATION HELPER ─────────────────────────────────────
# Scales any column to 0–100 using min-max within the dataset.
# Required when combining metrics with incompatible units
# (rates vs absolute counts vs dollar values).
def minmax_norm(df, col_in, col_out):
    stats = df.select(
        F.min(col_in).alias("mn"),
        F.max(col_in).alias("mx")
    ).collect()[0]
    mn, mx = stats["mn"], stats["mx"]
    if mx is None or mx == mn:
        # No variance across dataset — assign neutral midpoint
        return df.withColumn(col_out, F.lit(50.0))
    return df.withColumn(
        col_out,
        F.round(
            ((F.col(col_in) - F.lit(mn)) / F.lit(mx - mn)) * 100,
        4)
    )

# ─── MALARIA RISK SCORE (0–100) ───────────────────────────────
# Incidence (rate per 1,000) and Deaths (absolute count) are on
# completely different scales — normalise both before weighting.
# Higher score = higher burden = worse outcome.

malaria_wide = (
    fact_malaria
    .groupBy("CountryKey", "Year")
    .pivot("IndicatorName", ["Malaria Incidence", "Malaria Deaths"])
    .agg(F.avg("Value"))
)

malaria_wide = minmax_norm(malaria_wide, "Malaria Incidence", "incidence_norm")
malaria_wide = minmax_norm(malaria_wide, "Malaria Deaths",    "deaths_norm")

malaria = malaria_wide.withColumn(
    "MalariaRiskScore",
    F.round(
        F.col("incidence_norm") * 0.6 +
        F.col("deaths_norm")    * 0.4,
    2)
)

# ─── HEALTH SYSTEM SCORE (0–100) ─────────────────────────────
# Health spend % GDP (~3–8) and GDP per capita (~$400–$2,200)
# cannot be combined raw — GDP dominates by 2–3 orders of magnitude.
# Normalise both first. Higher score = stronger health system.

health_wide = (
    fact_health
    .groupBy("CountryKey", "Year")
    .pivot("IndicatorName", [
        "Health Expenditure Percent to GDP",
        "GDP Per Capita"
    ])
    .agg(F.avg("Value"))
)

health_wide = minmax_norm(health_wide, "Health Expenditure Percent to GDP", "health_exp_norm")
health_wide = minmax_norm(health_wide, "GDP Per Capita",                     "gdp_norm")

health = health_wide.withColumn(
    "HealthSystemScore",
    F.round(
        F.col("health_exp_norm") * 0.5 +
        F.col("gdp_norm")        * 0.5,
    2)
)

# ─── PSI PROGRAM SCORE (0–100) ────────────────────────────────
# Raw efficiency ratio (coverage/quality) runs ~0.4–1.5.
# Normalise to 0–100 for comparability with other scores.
# Higher score = more efficient PSI programme delivery.

psi_raw = (
    fact_psi
    .groupBy("CountryKey", "Year")
    .agg(
        F.avg("EfficiencyScore").alias("psi_raw_score"),
        F.avg("UnitCost").alias("AvgUnitCost")
    )
)

psi = minmax_norm(psi_raw, "psi_raw_score", "PSIProgramScore")

# ─── OVERALL PSI SCORE ───────────────────────────────────────
# All three inputs are now 0–100. Weights sum to 1.0.
#
# Burden   × −0.5 → High burden PENALISES (flags as priority)
# Health   × +0.3 → Strong health system partially offsets
# PSI Pgm  × +0.2 → Efficient PSI delivery partially offsets
#
# Interpretation:
#   < −25  → Priority 1: high burden, weak system
#   −25–−10 → Priority 2: accelerate coverage
#   −10–+5 → Priority 3: monitor
#   > +5   → On Track

country_kpi = (
    malaria.select("CountryKey", "Year", "MalariaRiskScore")
    .join(
        health.select("CountryKey", "Year", "HealthSystemScore"),
        ["CountryKey", "Year"], "left"
    )
    .join(
        psi.select("CountryKey", "Year", "PSIProgramScore", "AvgUnitCost"),
        ["CountryKey", "Year"], "left"
    )
    .withColumn(
        "OverallPSIScore",
        F.round(
            (F.col("MalariaRiskScore")  * -0.5) +
            (F.col("HealthSystemScore") *  0.3) +
            (F.col("PSIProgramScore")   *  0.2),
        2)
    )
    .withColumn(
        "PriorityTier",
        F.when(F.col("OverallPSIScore") < -15, "Priority 1 — Immediate Action")
         .when(F.col("OverallPSIScore") <   0, "Priority 2 — Accelerate")
         .when(F.col("OverallPSIScore") <  13, "Priority 3 — Monitor")
         .otherwise("On Track")
    )
)

# ─── SANITY CHECK BEFORE WRITING ─────────────────────────────
print("\n📊 Score range validation (all should be 0–100 except Overall):")
country_kpi.select(
    F.min("MalariaRiskScore").alias("Burden_Min"),
    F.max("MalariaRiskScore").alias("Burden_Max"),
    F.min("HealthSystemScore").alias("Health_Min"),
    F.max("HealthSystemScore").alias("Health_Max"),
    F.min("PSIProgramScore").alias("PSI_Min"),
    F.max("PSIProgramScore").alias("PSI_Max"),
    F.min("OverallPSIScore").alias("Overall_Min"),
    F.max("OverallPSIScore").alias("Overall_Max"),
).show()

print("Priority tier distribution:")
country_kpi.groupBy("PriorityTier").count().orderBy("count", ascending=False).show()

# ─── SCD TYPE 2 — DIM COUNTRY ────────────────────────────────
dim_country = dim_country \
    .withColumn("EffectiveFrom", F.lit(RUN_TS)) \
    .withColumn("EffectiveTo", F.lit(None).cast("timestamp")) \
    .withColumn("IsCurrent", F.lit(True))

dim_path = f"{GOLD_PATH}/dim_country"

try:
    delta_dim = DeltaTable.forPath(spark, dim_path)

    (
        delta_dim.alias("t")
        .merge(dim_country.alias("s"), "t.CountryKey = s.CountryKey AND t.IsCurrent = true")
        .whenMatchedUpdate(
            condition="t.CountryName <> s.CountryName OR t.CountryCode <> s.CountryCode",
            set={
                "EffectiveTo": "current_timestamp()",
                "IsCurrent":   "false"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("🔁 SCD Type 2 applied")

except:
    dim_country.write.format("delta").mode("overwrite").save(dim_path)
    print("🆕 dim_country created")

# ─── GENERIC UPSERT HELPER ───────────────────────────────────
def merge_delta(df, path, keys):
    try:
        delta_table = DeltaTable.forPath(spark, path)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        (
            delta_table.alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except:
        df.write.format("delta").mode("overwrite").save(path)

# ─── WRITE GOLD FACT ─────────────────────────────────────────
merge_delta(
    country_kpi,
    f"{GOLD_PATH}/gold_fact_country_kpi",
    ["CountryKey", "Year"]
)
print("✅ gold_fact_country_kpi written")

# ─── DQ AUDIT ────────────────────────────────────────────────
dq_results = []

def dq_check(df, name):
    total  = df.count()
    nulls  = df.filter("CountryKey IS NULL OR Year IS NULL").count()
    # Additional check: flag if any composite score is null
    score_nulls = df.filter(
        F.col("MalariaRiskScore").isNull() |
        F.col("HealthSystemScore").isNull() |
        F.col("PSIProgramScore").isNull() |
        F.col("OverallPSIScore").isNull()
    ).count()
    dq_results.append((name, total, nulls, score_nulls, RUN_TS))

dq_check(country_kpi, "gold_fact_country_kpi")

dq_df = spark.createDataFrame(
    dq_results,
    ["TableName", "TotalRows", "NullKeyRows", "NullScoreRows", "RunTimestamp"]
)

merge_delta(dq_df, f"{GOLD_PATH}/dq_gold_audit", ["TableName", "RunTimestamp"])
print("🔍 Gold DQ logged")

# ─── FINAL VALIDATION ────────────────────────────────────────
print("\n✅ Final row counts:")
for t in ["gold_fact_country_kpi", "dim_country", "dq_gold_audit"]:
    count = spark.read.format("delta").load(f"{GOLD_PATH}/{t}").count()
    print(f"   {t}: {count} rows")

print("\n🏆 GOLD ELITE PIPELINE COMPLETE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load(f"abfss://815e9f81-df95-4e8e-a776-a6188b9e4758@onelake.dfs.fabric.microsoft.com/75cc645c-e3e3-4c26-8e7c-a567f9125ad6/Tables/gold/gold_fact_country_kpi")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
