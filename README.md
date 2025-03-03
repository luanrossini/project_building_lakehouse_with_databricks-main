# Building a Lakehouse project with Databricks platform

![{9F170516-175F-4683-92B5-826A91724946}](https://github.com/user-attachments/assets/dd69b21b-de0c-4d0c-a21a-bb31f71c6efb)

## Business case:
A telecommunications company is a high-speed internet provider, serving a broad base of residential and business customers. The company's network infrastructure consists of multiple servers distributed across different locations to ensure service quality.

The person responsible for monitoring the company's network and servers faces challenges in ensuring that the infrastructure operates efficiently and that customers receive the best possible connection experience. Recently, he noticed that due to increased demand, some servers are overloaded and need to be upgraded to meet the demand. He needs to monitor in real-time the number of devices connected to each server as well as the connection usage rate. Additionally, he must identify whether customers are consuming a significant amount of bandwidth to ensure they receive the service they subscribed to.

Upon analyzing the data, he discovered that the company is experiencing an increase in the number of connected devices, causing network slowdowns and impacting the customer experience. He identified some customers who are using a disproportionate amount of bandwidth, which affects the connection quality for others. The responsible person proposed upgrading the servers to increase connection capacity and improve service quality. Additionally, he suggested implementing measures to identify and limit excessive bandwidth usage by some customers.

## Project Architecture
![{92B86AA3-AF68-4B15-9D9F-EEE4973C7482}](https://github.com/user-attachments/assets/53120c20-7be3-41a8-ac53-e3cae77dc97f)

The project will use the medallion architecture as its main concept, leveraging Databricks. The medallion architecture is a data design model that organizes and structures data into different layers to optimize data management and analysis. It is divided into three main layers:

Bronze Layer (Raw Data):
The Bronze layer is where all data from external source systems is stored. The table structures in this layer match the source system structures "as-is," along with additional metadata columns that capture load date/time, process ID, etc. The focus of this layer is on quickly capturing change data, maintaining a historical record (cold storage), ensuring data lineage and auditability, and enabling reprocessing if necessary without reloading data from the source system.

Silver Layer (Cleaned and Processed Data):
In the Silver layer, data from the Bronze layer is combined, merged, conformed, and cleaned ("just enough") to provide a corporate view of key business entities, concepts, and transactions (e.g., master customers, stores, deduplicated transactions, and cross-reference tables).

The Silver layer brings together data from different sources to create a unified corporate view, enabling self-service analytics for ad hoc reporting, advanced analytics, and machine learning. It serves as a source for departmental analysts, data engineers, and data scientists to build projects and conduct analyses that address business challenges through enterprise and departmental data initiatives in the Gold layer.

In the lakehouse data engineering paradigm, the ELT methodology is typically followed instead of ETL. This means that only minimal or "just enough" transformations and data cleansing rules are applied when loading data into the Silver layer. The priority is speed and agility in ingesting and delivering data into the data lake, while more complex transformations and project-specific business rules are applied when moving data from Silver to Gold.

From a data modeling perspective, the Silver layer generally follows a 3rd Normal Form structure. Performance-optimized models, such as Data Vault, can also be used in this layer. 

Gold Layer (Business tables): In the lakehouse architecture, data in the Gold layer is typically organized into project-specific databases that are ready for consumption. This layer is optimized for reporting and uses denormalized data models that are optimized for read performance, minimizing the need for complex joins. The final stage of data transformations and data quality rules is applied at this level.

The Gold layer serves as the final presentation layer for projects such as Customer Analysis, Product Quality Analysis, Inventory Analysis, Customer Segmentation, Product Recommendations, Sales/Revenue Analysis, and more. Many data models in this layer follow a Kimball-style star schema or Inmon-style data marts, making them well-suited for business intelligence and analytics use cases.

## The project

![{012C0325-0143-427E-B5B6-2A8211279296}](https://github.com/user-attachments/assets/a43e725c-1e4a-4f6e-aef4-fb669bafd9b1)

The project was built using a fake database generated with the Faker library in Python. The Databricks File System (DBFS) was used as the landing zone for data ingestion, with data updates handled through Spark Streaming and Change Data Feed (CDC). This approach enabled the real-time availability of data in the Gold layer of the lakehouse architecture.

![{374D586C-797A-4BB8-ADCF-EC44BB24E785}](https://github.com/user-attachments/assets/c219d169-07fd-4b3b-bbd4-d33e88d4f66e)

Initially, a master notebook called "application" was created to set up the necessary environment for ingesting ".parquet" files into the landing layer. It also generates automatic data every 5 seconds to populate the Bronze layer tables.

From the generated data, the Silver layer unifies the information into a table called "consolidated", where all cleaned, processed, and ready-to-use data is managed using Change Data Feed (CDF).
Finally, tables in the Gold layer were created to answer business questions and help the company's technical lead compare best practices to address the challenges they faced.

