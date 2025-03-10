# 🏎️ F1 Data Analysis 📊

This project focuses on collecting, processing, and analyzing Formula 1 🏁 data. The goal is to structure the data effectively in a data warehouse to enable insightful analysis and visualization.

## 📂 Table of Contents

- [Project Overview](#project-overview)
- [Setup](#setup)
- [Features](#features)
- [Usage](#usage)
- [Project Workflow](#project-workflow)
- [License](#license)

## 📌 Project Overview

Formula 1 generates vast amounts of data, from race results to lap times. This project collects, processes, and loads this data into a structured data warehouse, making it easier to extract insights and build meaningful visualizations.

## Setup

### 1. Development Environment
This project comes with a **devcontainer**, making the setup process seamless. By using the provided development container, all necessary dependencies will be pre-installed.

### 2. Python dependancies
After setting up the devcontainer, open the terminal and run the following command: 

```bash
poetry install --no-root
```

### 3. Environment Variables
Before running the project, ensure that you set up the required environment variables. These can be found in the `.env-example` file. Copy this file and rename it to `.env`, then update the values accordingly.

#### AWS Access
```ini
AWS_ACCESS_KEY = "your_aws_access_key"
AWS_SECRET_KEY = "your_aws_secret_key"
s3_bucket = "your_s3_bucket_name"
aws_region = "your_aws_region"
```
#### BigQuery Access
```ini
BQ_PROJECT = "your_bigquery_project"
BQ_DATASET = "your_bigquery_dataset"
SERVICE_ACCOUNT_JSON_PATH = "/config/service_account.json"
```

### 4. GCP Service Account Key

To access Google Cloud resources, you'll need a GCP Service Account JSON key. Place the downloaded JSON key file inside the /config/ directory and ensure that the path matches the SERVICE_ACCOUNT_JSON_PATH variable in your .env file.



## 🚀 Features

- 📥 **Comprehensive Data Collection**: Retrieves F1 🏎️ data from the Jolpica API, including race results, sprint results, qualifications, championship standings, pit stop data, and lap times.
  
- 🛠️ **Optimized Data Preparation**: Cleans and structures raw data using Polars to ensure consistency and efficiency.

- 🏦 **Centralized Data Storage**: Loads structured data into Google BigQuery, providing a scalable and reliable data warehouse.

- 📊 **Advanced Data Transformation**: Uses dbt to refine and optimize data models, ensuring high-quality insights. The structured warehouse follows a layered approach (staging → intermediate → marts).

- 📈 **Flexible Data Utilization**: The processed data can be leveraged for dashboards in Power BI, in-depth analysis, or other business intelligence applications.

## ⚙️ Usage

Once the environment is set up, the project allows you to fetch, transform, and analyze Formula 1 data. More details on setup will be provided soon with the addition of a development container.

## 🔄 Project Workflow

<p align="center">
<img src=".docs/workflow.png" alt="My Image" width="700">
</p>

1. **📥 Data Collection**: Retrieve race data from the Jolpica API, including results, standings, pit stops, and lap times.

2. **🛠️ Data Preparation**: Clean and format raw data using Polars for consistency and accuracy.

3. **🏦 Data Storage**: Load structured data into Google BigQuery for scalable storage and efficient querying.

4. **📊 Data Transformation**: Use dbt to refine data models, following a structured approach (staging → intermediate → marts) to optimize insights.

5. **📈 Data Utilization**: Leverage the data for dashboards in Power BI, advanced analysis, or other business applications.

## 📜 License

This project is licensed under the MIT License. See the LICENSE file for details.
