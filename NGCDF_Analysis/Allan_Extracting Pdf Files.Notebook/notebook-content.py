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
# META       "default_lakehouse_workspace_id": "6288070a-c8f5-41e3-8903-921d7faf8199"
# META     }
# META   }
# META }

# CELL ********************

!pip install pdfplumber

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = "/lh_cdf_bronze/Files/Audit-Reports/2016-2017/Ainabkoi-NGCDF-2016-2017.pdf"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install PyPDF2
from io import BytesIO
import PyPDF2
binary_df = spark.read.format("binaryFile").load("Files/Audit-Reports/2017-2018/Balambala-CDF-2017-2018.pdf").limit(1)
pdf_bytes = binary_df.collect()[0].content


file_stream = BytesIO(pdf_bytes)
reader = PyPDF2.PdfReader(file_stream)

extracted_text = ""
for page in reader.pages:
 extracted_text += page.extract_text()

 

data_rows = [{"content": extracted_text}]
df = spark.createDataFrame(data_rows)

df.write.mode("overwrite").format("delta").saveAsTable("ExtractedPdfData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.extractedpdfdata LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
