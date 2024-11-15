# Medallion Structure in Data Factory

In this data factory, we have implemented a medallion architecture to efficiently manage and process our data using lakehouses, dataflows, and pipelines. The medallion structure consists of two main layers: Bronze and Gold lakehouses, each serving a specific purpose in our data processing workflow.

## Bronze Lakehouse

The Bronze Lakehouse acts as the raw data storage layer. It is designed to ingest data from various sources with minimal transformation, allowing for the preservation of the original data. This layer serves as the foundation for further data processing and transformation activities. The data is stored in its native format and is ingested through a pipeline using copy activities. This ensures that all incoming data is captured and stored securely for future processing.

- **Purpose:** Raw data ingestion and storage.
- **Data Sources:** Various external data sources.
- **Processing:** Minimal transformation, primarily data ingestion.
- **Tools:** Azure Data Factory Pipelines, Copy Activity.

## Gold Lakehouse

The Gold Lakehouse represents the curated, highly refined data layer. Data in this layer has undergone significant transformation and enrichment processes to ensure it is ready for business intelligence (BI) reporting and analytics. The transformation processes are managed through dataflows and additional pipeline activities, ensuring data quality and consistency. This layer is optimized for performance and is structured to support efficient querying and reporting.

- **Purpose:** Refined and enriched data ready for BI and analytics.
- **Data Processing:** Data transformation, cleansing, and enrichment.
- **Tools:** Azure Data Factory Dataflows, Pipelines.

## BI Report

The BI report utilizes data from the Gold Lakehouse, providing insights and analytics to support business decision-making. By leveraging the structured and refined data in the Gold Lakehouse, the BI report delivers accurate and actionable information to stakeholders.

- **Purpose:** Deliver insights and analytics.
- **Data Source:** Gold Lakehouse.
- **Outcome:** Actionable business intelligence.

This medallion structure ensures that data is systematically processed, transformed, and made available for business use, providing a scalable and efficient data management solution.
