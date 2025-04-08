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
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "451f5dad-0a75-884d-4cfc-34f19a4e53ba",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import os
import time
import re
import json
import io
import PyPDF2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from openai import AzureOpenAI, RateLimitError
from delta.tables import DeltaTable

# Set up SSL certificate bundle
ca_bundle_path = "/etc/ssl/certs/ca-certificates.crt"
if os.path.exists(ca_bundle_path):
    os.environ["SSL_CERT_FILE"] = ca_bundle_path
    print(f"Using CA bundle at: {ca_bundle_path}")
else:
    print("CA bundle not found. Proceeding without custom SSL_CERT_FILE setting.")

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Set up AzureOpenAI client parameters
ENDPOINT = "https://mango-bush-0a9e12903.5.azurestaticapps.net/api/v1"
API_KEY = "Replace with your API key"   
API_VERSION = "2024-02-01"
MODEL_NAME = "gpt-4o-kenya-hack"

client = AzureOpenAI(
    azure_endpoint=ENDPOINT,
    api_key=API_KEY,
    api_version=API_VERSION,
)

# Define the schema for the Delta table
schema = StructType([
    StructField("Constituency_Name", StringType(), True),
    StructField("Financial_Year", StringType(), True),
    StructField("Auditors_Opinion", StringType(), True),
    StructField("Key_Audit_Matters", StringType(), True),
    StructField("Budgetary_Control_and_Performance", StringType(), True),
    StructField("Lawfulness_and_Effectiveness", StringType(), True),
    StructField("Internal_Controls_Risk_Management_Governance", StringType(), True),
    StructField("Recommendations", StringType(), True)
])

# Define full ABFSS URIs for your resources.
audit_reports_path = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_bronze.Lakehouse/Files/Audit-Reports"
delta_table_path   = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_silver.Lakehouse/Tables/Audit_Reports"
checkpoint_path    = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_silver.Lakehouse/Files/Checkpoints/Audit_Reports_Processed.txt"

# ---- Utility functions using Hadoop FileSystem API via Spark ----

def get_hadoop_fs(path):
    jPath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jPath.getFileSystem(spark._jsc.hadoopConfiguration())

def file_exists(path):
    jPath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = get_hadoop_fs(path)
    return fs.exists(jPath)

def normalize_path(path):
    # Remove the common prefix so that both checkpoint and current listing match.
    prefix = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/"
    if path.startswith(prefix):
        return path[len(prefix):]
    return path

def read_checkpoint(path):
    if file_exists(path):
        # Use Spark to read the text file into a DataFrame
        df = spark.read.text(path)
        # Normalize each path before storing it in the set.
        return set(normalize_path(row.value) for row in df.collect())
    else:
        return set()

def write_checkpoint(path, content):
    fs = get_hadoop_fs(path)
    jPath = spark._jvm.org.apache.hadoop.fs.Path(path)
    # Create (or overwrite) the checkpoint file
    output = fs.create(jPath, True)
    output.write(bytearray(content, "utf-8"))
    output.close()

def list_directory(path):
    fs_hadoop = get_hadoop_fs(path)
    jPath = spark._jvm.org.apache.hadoop.fs.Path(path)
    statuses = fs_hadoop.listStatus(jPath)
    items = []
    for status in statuses:
        items.append({
            "path": status.getPath().toString(),
            "isDirectory": status.isDirectory()
        })
    return items

def read_pdf_bytes(path):
    # Use Spark's binaryFile reader to load the file as binary data.
    df = spark.read.format("binaryFile").load(path)
    # Assumes that the path returns one row.
    row = df.collect()[0]
    return row.content

# ---- PDF Text Extraction Function ----

def extract_text_from_pdf(path):
    """Extract text from a PDF file using PyPDF2."""
    pdf_bytes = read_pdf_bytes(path)
    reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    pdf_text = ""
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pdf_text += text + "\n"
    return pdf_text

# ---- Structured Data Extraction via AzureOpenAI ----

def extract_structured_data(pdf_text, max_retries=5):
    """Use AzureOpenAI to extract specific fields from PDF text and return as a dictionary."""
    for attempt in range(max_retries):
        try:
            messages = [
                {"role": "system", "content": """You are a data normalization expert working with audit reports. Convert unstructured data into clean structured JSON format with the following requirements:

    constituency name NVARCHAR(MAX),
    financial_year should all be in the same format (like 2016/2017. If 2019, then 2018/2019, if year ended 30th june 2019, then 2018/2019),
    qualified_opinion_audit_scope NVARCHAR(MAX),
    qualified_opinion_conclusion NVARCHAR(MAX),
    qualified_opinion_lawfulness_and_effectiveness_conclusion NVARCHAR(MAX),
    budgetary_control_performance_approved_budget NVARCHAR(50),
    budgetary_control_performance_actual_receipts NVARCHAR(50),
    budgetary_control_performance_under_expenditure_amount NVARCHAR(50),
    budgetary_control_performance_under_expenditure_percentage NVARCHAR(10),
    budgetary_control_performance_impact_analysis NVARCHAR(MAX)

Return ONLY valid JSON without markdown formatting."""},
    
                {"role": "user", "content": f"Clean this data:\n{pdf_text}"}
            ]
            
            completion = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
            )
            
            content = completion.choices[0].message.content
            try:
                structured_data = json.loads(content)
                return structured_data
            except json.JSONDecodeError:
                print(f"Failed to parse content as JSON: {content}")
                return {}
        except RateLimitError as e:
            if attempt < max_retries - 1:
                message = str(e)
                match = re.search(r'retry after (\d+) seconds', message, re.IGNORECASE)
                wait_time = int(match.group(1)) if match else 60
                print(f"Rate limit exceeded on attempt {attempt + 1}. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print(f"Max retries ({max_retries}) reached for this PDF. Skipping.")
                return {}

# ---- Checkpoint Handling ----

processed_files = read_checkpoint(checkpoint_path)
print(f"Loaded checkpoint. {len(processed_files)} files already processed.")

# ---- List Audit Report Subfolders ----

all_items = list_directory(audit_reports_path)
print("all_items:", all_items)
# Filter for directories (each expected to represent a financial year)
subfolders = [item["path"] for item in all_items if item["isDirectory"]]

# ---- Build List of Files to Process ----

all_files_to_process = []
for subfolder in subfolders:
    # Assume the financial year is the basename of the subfolder
    financial_year = os.path.basename(subfolder.rstrip("/"))
    items = list_directory(subfolder)
    # Filter for PDF files
    pdf_files = [item["path"] for item in items if (not item["isDirectory"]) and item["path"].endswith(".pdf")]
    for pdf_file in pdf_files:
        normalized_pdf_file = normalize_path(pdf_file)
        if normalized_pdf_file not in processed_files:
            all_files_to_process.append((pdf_file, financial_year))

total_files = len(all_files_to_process)
print(f"Total files to be processed: {total_files}")

# ---- Delta Upsert and Checkpoint Update Function ----

def merge_and_checkpoint(new_records, batch_files, processed_files_set):
    if new_records:
        new_df = spark.createDataFrame(new_records, schema)
        try:
            deltaTable = DeltaTable.forPath(spark, delta_table_path)
            (deltaTable.alias("t")
             .merge(new_df.alias("s"),
                    "t.Financial_Year = s.Financial_Year AND t.Constituency_Name = s.Constituency_Name")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            print("Merged new records into existing Delta table.")
        except Exception as e:
            new_df.write.format("delta").mode("overwrite").save(delta_table_path)
            print("Delta table not found. Created new Delta table with new records.")

        # Update processed_files_set with normalized paths.
        processed_files_set.update(normalize_path(p) for p in batch_files)
        checkpoint_content = "\n".join(list(processed_files_set))
        try:
            write_checkpoint(checkpoint_path, checkpoint_content)
            print(f"Checkpoint updated with {len(batch_files)} new files.")
        except Exception as e:
            print(f"Failed to update checkpoint: {e}")

# ---- Process Each File ----

new_data_list = []
batch_processed_files = []
BATCH_SIZE = 20  # Save checkpoint every 20 files
global_index = 0

for pdf_file, financial_year in all_files_to_process:
    global_index += 1
    print(f"Processing file {global_index} of {total_files}: {pdf_file}")
    
    constituency_name = os.path.basename(pdf_file).replace(".pdf", "")
    
    try:
        pdf_text = extract_text_from_pdf(pdf_file)
    except Exception as e:
        print(f"Error extracting text from {pdf_file}: {str(e)}")
        continue
    
    extracted_data = extract_structured_data(pdf_text)
    
    new_data_list.append((
        extracted_data.get("constituency_name", constituency_name),
        extracted_data.get("financial_year", financial_year),
        extracted_data.get("Auditors_Opinion", ""),
        extracted_data.get("Key_Audit_Matters", ""),
        extracted_data.get("Budgetary_Control_and_Performance", ""),
        extracted_data.get("Lawfulness_and_Effectiveness", ""),
        extracted_data.get("Internal_Controls_Risk_Management_Governance", ""),
        extracted_data.get("Recommendations", "")
    ))
    
    batch_processed_files.append(pdf_file)
    
    if len(batch_processed_files) >= BATCH_SIZE:
        merge_and_checkpoint(new_data_list, batch_processed_files, processed_files)
        new_data_list = []
        batch_processed_files = []

if batch_processed_files:
    merge_and_checkpoint(new_data_list, batch_processed_files, processed_files)

print(f"Data successfully upserted into Delta table at {delta_table_path}")


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

# CELL ********************



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
