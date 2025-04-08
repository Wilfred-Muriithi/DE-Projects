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
# META     },
# META     "environment": {
# META       "environmentId": "451f5dad-0a75-884d-4cfc-34f19a4e53ba",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import os
import re
import json
import time
import datetime
import textwrap
import pandas as pd
from IPython.display import display, HTML
from notebookutils import mssparkutils
from tenacity import retry, wait_random_exponential, stop_after_attempt
from openai import AzureOpenAI
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders import PyPDFLoader
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

# -------------------------- 1) CONFIGURATION -------------------------- #
print("Step 1: Configuring environment...")
OPENAI_GPT4_DEPLOYMENT_NAME = "gpt-4o-kenya-hack"
OPENAI_DEPLOYMENT_ENDPOINT = "https://mango-bush-0a9e12903.5.azurestaticapps.net/api/v1"
OPENAI_API_KEY = "API KEY"
OPENAI_ADA_EMBEDDING_DEPLOYMENT_NAME = "text-embedding-ada-002-kenya-hack"

KUSTO_URI = "https://trd-jjrr7x98d4mwcxrz4w.z9.kusto.fabric.microsoft.com"
KUSTO_DATABASE = "eh_cdf_audit"
KUSTO_TABLE = "AuditEmbeddings"
accessToken = mssparkutils.credentials.getToken(KUSTO_URI)

checkpoint_path = "/lakehouse/default/Files/checkpoints/audit_report_embeddings_delta"
print("Configuration complete.\n")

# -------------------------- 2) INITIALIZE OPENAI CLIENT -------------------------- #
print("Step 2: Initializing OpenAI client...")
client = AzureOpenAI(
    azure_endpoint=OPENAI_DEPLOYMENT_ENDPOINT,
    api_key=OPENAI_API_KEY,
    api_version="2024-02-01"
)
print("OpenAI client initialized.\n")

# -------------------------- 3) ENHANCED EMBEDDING GENERATION WITH EXPONENTIAL BACKOFF -------------------------- #
print("Step 3: Setting up batch embedding generation...")

@retry(wait=wait_random_exponential(min=1, max=30), stop=stop_after_attempt(10))
def generate_embeddings_batch(text_list):
    """Generates embeddings with enhanced retry logic"""
    cleaned_texts = [txt.replace("\n", " ") for txt in text_list]
    response = client.embeddings.create(
        input=cleaned_texts,
        model=OPENAI_ADA_EMBEDDING_DEPLOYMENT_NAME
    )
    return [item.embedding for item in response.data]

def safe_generate_embeddings_batch(text_list):
    """Handles rate limits with smart backoff and midnight reset"""
    try:
        return generate_embeddings_batch(text_list), False
    except Exception as e:
        if "429" in str(e) or "TooManyRequests" in str(e):
            match = re.search(r"retry after (\d+) seconds", str(e), re.IGNORECASE)
            if match:
                sleep_time = int(match.group(1)) + 2
                print(f"Rate limit hit. Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
                return generate_embeddings_batch(text_list), True
            else:
                now = datetime.datetime.utcnow()
                tomorrow = now.date() + datetime.timedelta(days=1)
                midnight = datetime.datetime.combine(tomorrow, datetime.time.min)
                sleep_seconds = (midnight - now).total_seconds()
                print(f"Critical rate limit. Sleeping until midnight UTC ({sleep_seconds}s)...")
                time.sleep(sleep_seconds)
                return generate_embeddings_batch(text_list), True
        else:
            raise

print("Batch embedding function ready.\n")

# -------------------------- 4) TEXT SPLITTING CONFIG -------------------------- #
print("Step 4: Configuring text splitter...")
splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=30,
)
print("Text splitter configured.\n")

# -------------------------- 5) PDF LOADING HELPER -------------------------- #
print("Step 5: Defining PDF loading helper...")

def load_pdf_locally(pdf_path):
    """Handles ABFSS to local file conversion for PDF processing"""
    if pdf_path.startswith("abfss://"):
        local_pdf_path = "/tmp/" + os.path.basename(pdf_path)
        print(f"Copying {pdf_path} to {local_pdf_path}...")
        binary_df = spark.read.format("binaryFile").load(pdf_path)
        file_content = binary_df.collect()[0]["content"]
        with open(local_pdf_path, "wb") as f:
            f.write(bytearray(file_content))
        return local_pdf_path
    return pdf_path

print("Helper function defined.\n")

# -------------------------- 6) CHECKPOINT HANDLING -------------------------- #
try:
    print("Step 6: Checking for embedding checkpoint...")
    df_checkpoint = spark.read.format("delta").load(checkpoint_path)
    print("Checkpoint found. Resuming from checkpoint...\n")
    df = df_checkpoint.toPandas()
except Exception as e:
    print("No checkpoint found. Processing PDFs from scratch...\n")
    # -------------------------- 7) PDF DISCOVERY -------------------------- #
    print("Step 7: Discovering PDF files...")
    audit_reports_root = "abfss://NGCDF@onelake.dfs.fabric.microsoft.com/lh_cdf_bronze.Lakehouse/Files/Audit-Reports"
    pdf_file_paths = []
    for folder_info in mssparkutils.fs.ls(audit_reports_root):
        if folder_info.isDir:
            print(f"Scanning {folder_info.path}")
            for file_info in mssparkutils.fs.ls(folder_info.path):
                if file_info.name.endswith(".pdf"):
                    pdf_file_paths.append(file_info.path)
    print(f"Found {len(pdf_file_paths)} PDFs\n")
    
    # -------------------------- 8) PDF PROCESSING -------------------------- #
    print("Step 8: Processing PDFs...")
    all_data = []
    for pdf_path in pdf_file_paths:
        document_name = os.path.splitext(os.path.basename(pdf_path))[0]
        print(f"\nProcessing {document_name}")
        local_path = load_pdf_locally(pdf_path)
        print(f"Splitting {local_path}...")
        loader = PyPDFLoader(local_path)
        pages = loader.load_and_split(text_splitter=splitter)
        for page in pages:
            all_data.append({
                "document_name": document_name,
                "content": page.page_content,
                "embedding": None
            })
    
    df = pd.DataFrame(all_data)
    print(f"\nTotal chunks: {len(df)}")
    print("Sample data:")
    print(df.head(2), "\n")

# -------------------------- 9) EMBEDDING GENERATION WITH INCREMENTAL CHECKPOINTING -------------------------- #
print("Step 9: Generating embeddings with exponential backoff and checkpointing...")
batch_size = 16
base_interval = 15
backoff_factor = 2
max_interval = 300
current_interval = base_interval

num_chunks = len(df)
if 'embedding' in df.columns:
    incomplete = df['embedding'].isnull()
    start_index = incomplete.idxmax() if incomplete.any() else num_chunks
else:
    start_index = 0

batch_counter = 0

for i in range(start_index, num_chunks, batch_size):
    batch_counter += 1
    batch_texts = df["content"].iloc[i:i+batch_size].tolist()
    print(f"\nProcessing batch {((i)//batch_size) + 1} ({i}-{min(i+batch_size, num_chunks)})")
    
    try:
        embeddings, rate_limit_hit = safe_generate_embeddings_batch(batch_texts)
        df.loc[i:i+batch_size-1, "embedding"] = embeddings
        
        if rate_limit_hit:
            current_interval = min(current_interval * backoff_factor, max_interval)
            print(f"Rate limit encountered. Interval increased to {current_interval}s")
        else:
            current_interval = max(base_interval, current_interval / backoff_factor)
            print(f"Batch successful. Interval decreased to {current_interval}s")
        
        print(f"Completed batch {((i)//batch_size) + 1}. Sleeping {current_interval}s...")
        time.sleep(current_interval)
        
        # Save a checkpoint every 200 batches
        if batch_counter % 2 == 0:
            print(f"\nCheckpointing after {batch_counter} batches processed...")
            df_sp = spark.createDataFrame(df)
            df_sp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(checkpoint_path)
            print("Checkpoint saved.\n")
            
    except Exception as e:
        print(f"Critical error processing batch {((i)//batch_size) + 1}: {str(e)}")
        raise

df_sp = spark.createDataFrame(df)
df_sp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(checkpoint_path)
print("Final checkpoint saved. Embeddings generated successfully.\n")

# -------------------------- 10) KUSTO DATA INGESTION -------------------------- #
print("Step 10: Writing to Eventhouse...")
df_sp = spark.createDataFrame(df)
df_sp.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", KUSTO_URI) \
    .option("kustoDatabase", KUSTO_DATABASE) \
    .option("kustoTable", KUSTO_TABLE) \
    .option("accessToken", accessToken) \
    .mode("Append") \
    .save()
print(f"Data written to {KUSTO_TABLE}\n")

# -------------------------- 11) RAG QUERY FUNCTIONS -------------------------- #
print("Step 11: Configuring RAG query...")

def call_openAI(messages):
    response = client.chat.completions.create(
        model=OPENAI_GPT4_DEPLOYMENT_NAME,
        messages=messages,
        temperature=0
    )
    return response.choices[0].message.content

def get_answer_from_eventhouse(question, nr_of_answers=3):
    print(f"Embedding question: {question[:50]}...")
    searchedEmbedding = safe_generate_embeddings_batch([question])[0][0]
    
    kusto_query = f"""
    {KUSTO_TABLE}
    | extend similarity = series_cosine_similarity(dynamic({searchedEmbedding}), embedding)
    | top {nr_of_answers} by similarity desc
    """
    return spark.read \
        .format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("kustoCluster", KUSTO_URI) \
        .option("kustoDatabase", KUSTO_DATABASE) \
        .option("accessToken", accessToken) \
        .option("kustoQuery", kusto_query) \
        .load()

print("RAG functions ready.\n")

# -------------------------- 12) TEST RAG PIPELINE -------------------------- #
print("Step 12: Testing RAG pipeline...")
question = "What does the 2016-2017 audit report say about Bahati-NGCDF?"
print(f"Question: {question}")

answers_df = get_answer_from_eventhouse(question)
context = " ".join([row.content for row in answers_df.rdd.toLocalIterator()])

prompt = f"""Answer the question using only the provided context.
Question: {question}
Context: {context[:3000]}... [truncated]
Answer:"""

messages = [
    {"role": "system", "content": "You are an audit report assistant. Answer concisely."},
    {"role": "user", "content": prompt}
]

print("Generating answer...")
final_answer = call_openAI(messages)
display(HTML(f"<b>Final Answer:</b><br>{final_answer}"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
