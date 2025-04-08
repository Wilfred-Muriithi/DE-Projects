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
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# Import necessary libraries
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlretrieve

# Fetch the HTML content of the webpage
url = "https://www.oagkenya.go.ke/2016-2017-constituency-development-fund-audit-reports/"
response = requests.get(url)
html = response.text

# Parse the HTML to extract all PDF download URLs
soup = BeautifulSoup(html, "html.parser")
links = soup.find_all("a", class_="dlp-download-link")
urls = [link["href"] for link in links]

# Define the output directory in the lakehouse
output_dir = "/lakehouse/default/Files/Audit-Reports/2016-2017"
os.makedirs(output_dir, exist_ok=True)

# Define the download function
def download_file(url):
    try:
        filename = url.split("/")[-1]
        save_path = os.path.join(output_dir, filename)
        urlretrieve(url, save_path)
        print(f"Downloaded {filename}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

# Downloading files in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(download_file, urls)

# Verifying the number of files downloaded
print(f"Total URLs found: {len(urls)}")
print(f"Files in output directory: {len(os.listdir(output_dir))}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.All_NGCDF_Allocations LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.All_NGCDF_Disbursements LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.Constituencies LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.All_NGCDF_Allocations LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.All_NGCDF_Disbursements LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_cdf_bronze.Constituencies LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install pdfplumber

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pdfplumber
import pandas as pd

with pdfplumber.open("Files") as pdf:
    text = ""
    for page in pdf.pages:
        text += page.extract_text()

# Process the text and convert it into a DataFrame
data = {"Column1": [text]}  # Example structure
df = pd.DataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
