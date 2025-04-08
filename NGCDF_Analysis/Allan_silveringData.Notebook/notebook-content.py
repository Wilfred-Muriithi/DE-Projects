# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0041a51-f310-4320-92ce-6c99d8816544",
# META       "default_lakehouse_name": "lh_cdf_silver",
# META       "default_lakehouse_workspace_id": "6288070a-c8f5-41e3-8903-921d7faf8199",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0041a51-f310-4320-92ce-6c99d8816544"
# META         },
# META         {
# META           "id": "1b8c60cb-f48f-4385-82e5-8b80b6a6b934"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
allocation_df = spark.sql("SELECT * FROM lh_cdf_silver.All_NGCDF_Allocations LIMIT 1000")
allocation_df = allocation_df.withColumn('Constituency', initcap('Constituency'))
allocation_df = allocation_df.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
allocation_df = allocation_df.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)
display(allocation_df.limit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1617 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2016-2017')
df_1617  = df_1617.withColumn('FinancialYear', lit('2016/2017'))
df_1617 = df_1617.withColumn('Constituency', initcap('Constituency'))
df_1617 = df_1617.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_1617 = df_1617.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)
display(df_1617)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constituencies_df = spark.sql("SELECT * FROM lh_cdf_silver.Constituencies LIMIT 1000")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
mergedac_df = df_1617.join(constituencies_df,  on='Constituency', how='left')
mergedac_df = mergedac_df[mergedac_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac_df.write.format('delta').saveAsTable('Allocation_Cleant')
display(mergedac_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_silver.Audit_Reports LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Allocations

# CELL ********************

df_1718 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2017-2018')
df_1718  = df_1718.withColumn('FinancialYear', lit('2017/2018'))
df_1718 = df_1718.withColumn('Constituency', initcap('Constituency'))
df_1718 = df_1718.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_1718 = df_1718.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)
display(df_1718)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
mergedac1_df = df_1718.join(constituencies_df,  on='Constituency', how='left')
mergedac1_df = mergedac1_df[mergedac1_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac1_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac1_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac1_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_1819 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2018-2019')
df_1819  = df_1819.withColumn('FinancialYear', lit('2018/2019'))
df_1819 = df_1819.withColumn('Constituency', initcap('Constituency'))
df_1819 = df_1819.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_1819 = df_1819.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac2_df = df_1819.join(constituencies_df,  on='Constituency', how='left')
mergedac2_df = mergedac2_df[mergedac2_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac2_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac2_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac2_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_1920 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2019-2020')
df_1920  = df_1920.withColumn('FinancialYear', lit('2019/2020'))
df_1920 = df_1920.withColumn('Constituency', initcap('Constituency'))
df_1920 = df_1920.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_1920 = df_1920.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac3_df = df_1920.join(constituencies_df,  on='Constituency', how='left')
mergedac3_df = mergedac3_df[mergedac3_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac3_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac3_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac3_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_1920 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2019-2020')
df_1920  = df_1920.withColumn('FinancialYear', lit('2019/2020'))
df_1920 = df_1920.withColumn('Constituency', initcap('Constituency'))
df_1920 = df_1920.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_1920 = df_1920.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac4_df = df_1920.join(constituencies_df,  on='Constituency', how='left')
mergedac4_df = mergedac4_df[mergedac4_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac4_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac4_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac4_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_2021 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2020-2021')
df_2021  = df_2021.withColumn('FinancialYear', lit('2020/2021'))
df_2021 = df_2021.withColumn('Constituency', initcap('Constituency'))
df_2021 = df_2021.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_2021 = df_2021.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac5_df = df_2021.join(constituencies_df,  on='Constituency', how='left')
mergedac5_df = mergedac5_df[mergedac5_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac5_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac5_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac5_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_2122 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2021-2022')
df_2122  = df_2122.withColumn('FinancialYear', lit('2021/2022'))
df_2122 = df_2122.withColumn('Constituency', initcap('Constituency'))
df_2122 = df_2122.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_2122 = df_2122.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac6_df = df_2122.join(constituencies_df,  on='Constituency', how='left')
mergedac6_df = mergedac6_df[mergedac6_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac6_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac6_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac6_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_2223 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2022-2023')
df_2223  = df_2223.withColumn('FinancialYear', lit('2022/2023'))
df_2223 = df_2223.withColumn('Constituency', initcap('Constituency'))
df_2223 = df_2223.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_2223 = df_2223.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac7_df = df_2223.join(constituencies_df,  on='Constituency', how='left')
mergedac7_df = mergedac7_df[mergedac7_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac7_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac7_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac7_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_2324 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2023-2024')
df_2324  = df_2324.withColumn('FinancialYear', lit('2023/2024'))
df_2324 = df_2324.withColumn('Constituency', initcap('Constituency'))
df_2324 = df_2324.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_2324 = df_2324.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac8_df = df_2324.join(constituencies_df,  on='Constituency', how='left')
mergedac8_df = mergedac8_df[mergedac8_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac8_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac8_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac8_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_2425 = spark.read.format('parquet').load('Files/allocationByYear/FinancialYear=2024-2025')
df_2425  = df_2425.withColumn('FinancialYear', lit('2024/2025'))
df_2425 = df_2425.withColumn('Constituency', initcap('Constituency'))
df_2425 = df_2425.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
df_2425 = df_2425.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

mergedac9_df = df_2425.join(constituencies_df,  on='Constituency', how='left')
mergedac9_df = mergedac9_df[mergedac9_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', mergedac9_df['Formated Amount'].alias('Amount'), 'AmountInMillions'].orderBy('County_Number')
mergedac9_df.write.format('delta').mode('append').saveAsTable('Allocation_Cleant')
display(mergedac9_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_silver.allocation_cleant")
display(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
disbursement_df = spark.read.format('delta').load('abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_silver.Lakehouse/Tables/All_NGCDF_Disbursements')
disbursement_df = disbursement_df.filter(col('FinancialYear') >= '2016/2017')
disbursement_df = disbursement_df.withColumn('Formated Amount', format_number(col('Amount'), 2))
disbursement_df = disbursement_df.withColumn('AmountInMillions', format_number(col('Amount') / 1E6, 2))
disbursement_df = disbursement_df.withColumn('Constituency', initcap('Constituency'))
disbursement_df = disbursement_df.withColumn('Constituency', regexp_replace(col('Constituency'), " ", "-"))
disbursement_df = disbursement_df.withColumn("Constituency", 
    when(col("Constituency") == "Chuka---Igambang’ombe", "Chuka")
    .when(col("Constituency") == "Emurua-Dikirr", "Emuria-Dikirr")
    .when(col("Constituency") == "Kiambu-Town", "Kiambu")
    .when(col("Constituency") == "Kilifi-South---Bahari", "Kilifi-South")
    .when(col("Constituency") == "Kitutu-Chache-North", "KitutuChache-North")
    .when(col("Constituency") == "Kitutu-Chache-South", "KitutuChache-South")
    .when(col("Constituency") == "Lunga-Lunga", "LungaLunga")
    .when(col("Constituency") == "Maragwa", "Maragua")
    .when(col("Constituency") == "Homa-Bay-Town", "Homabay-Town")
    .when(col("Constituency") == "Mt.-Elgon", "Mt-Elgon")
    .when(col("Constituency") == "Nakuru-Town-East", "NakuruTown-East")
    .when(col("Constituency") == "Nakuru-Town-West", "NakuruTown-West")
    .when(col("Constituency") == "Ol-Jororok", "OlJoro-Orok")
    .when(col("Constituency") == "Ol-Kalou", "OlKalou")
    .when(col("Constituency") == "Sigowet---Soin", "Soin")
    .when(col("Constituency") == "Suba-North---Mbita", "Suba-North")
    .otherwise(col("Constituency"))  # Keep original value if no match
)

disbursement_df.write.partitionBy('FinancialYear').mode('overwrite').parquet('Files/DisbursementByYear')
display(disbursement_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
dsdf_1617 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2016-2017')
dsdf_1617  = dsdf_1617.withColumn('FinancialYear', lit('2016/2017'))
mergedds_df = dsdf_1617.join(constituencies_df,  on='Constituency', how='left')
mergedds_df = mergedds_df.select(mergedds_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds_df.write.format('delta').mode('overwrite').saveAsTable('Distribution_Cleaned')
display(mergedds_df)

   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_1718 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2017-2018')
dsdf_1718  = dsdf_1718.withColumn('FinancialYear', lit('2017/2018'))
mergedds1_df = dsdf_1718.join(constituencies_df,  on='Constituency', how='left')
mergedds1_df = mergedds1_df.select(mergedds1_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds1_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds1_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds1_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_1819 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2018-2019')
dsdf_1819  = dsdf_1819.withColumn('FinancialYear', lit('2018/2019'))
mergedds2_df = dsdf_1819.join(constituencies_df,  on='Constituency', how='left')
mergedds2_df = mergedds2_df.select(mergedds2_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds2_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds2_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds2_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_1920 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2019-2020')
dsdf_1920  = dsdf_1920.withColumn('FinancialYear', lit('2019/2020'))
mergedds3_df = dsdf_1920.join(constituencies_df,  on='Constituency', how='left')
mergedds3_df = mergedds3_df.select(mergedds3_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds3_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds3_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds3_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_2021 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2020-2021')
dsdf_2021  = dsdf_2021.withColumn('FinancialYear', lit('2020/2021'))
mergedds4_df = dsdf_2021.join(constituencies_df,  on='Constituency', how='left')
mergedds4_df = mergedds4_df.select(mergedds4_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds4_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds4_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds4_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_2122 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2021-2022')
dsdf_2122  = dsdf_2122.withColumn('FinancialYear', lit('2021/2022'))
mergedds5_df = dsdf_2122.join(constituencies_df,  on='Constituency', how='left')
mergedds5_df = mergedds5_df.select(mergedds5_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds5_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds5_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds5_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_2223 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2022-2023')
dsdf_2223  = dsdf_2223.withColumn('FinancialYear', lit('2022/2023'))
mergedds6_df = dsdf_2223.join(constituencies_df,  on='Constituency', how='left')
mergedds6_df = mergedds6_df.select(mergedds6_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds6_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds6_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds6_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_2324 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2023-2024')
dsdf_2324  = dsdf_2324.withColumn('FinancialYear', lit('2023/2024'))
mergedds7_df = dsdf_2324.join(constituencies_df,  on='Constituency', how='left')
mergedds7_df = mergedds7_df.select(mergedds7_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds7_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds7_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds7_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
dsdf_2425 = spark.read.format('parquet').load('Files/DisbursementByYear/FinancialYear=2024-2025')
dsdf_2425  = dsdf_2425.withColumn('FinancialYear', lit('2024/2025'))
mergedds8_df = dsdf_2425.join(constituencies_df,  on='Constituency', how='left')
mergedds8_df = mergedds8_df.select(mergedds8_df['County_Number'].cast(IntegerType()), 'County', 'Constituency', 'FinancialYear', 'Date', mergedds8_df['Formated Amount'].alias('Amount'), 'AmountInMillions').orderBy('County_Number', 'Constituency')
mergedds8_df.write.format('delta').mode('append').saveAsTable('Distribution_Cleaned')
#display(mergedds8_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format('delta').load('abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_silver.Lakehouse/Tables/distribution_cleaned')
display(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format('delta').load('abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_silver.Lakehouse/Tables/All_NGCDF_Disbursements').where(col('FinancialYear') >= '2016/2017')
display(df.count())

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
