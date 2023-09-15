# Chicago Crashes Data Engineering project
This is the final project of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks Club](https://datatalks.club)

## Introduction

Chicago is one of the biggest states in the US that is full of life. Developing a data pipeline for the Crash data will help analytics to understand why accidents 
happen and how to prevent them, which will help save lives and make the city a safer place. It also guides how we plan our streets and neighborhoods.

## Data Source
The traffic crashes dataset is taken from the [Chicago data portal](https://data.cityofchicago.org/browse?limitTo=datasets), which contains various open-access datasets related to the city.
In my project, I used two data sets the [Traffic Crashes - Crashes dataset](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if), which shows information about each traffic crash on city streets within the City of Chicago and
the [Traffic Crashes - People dataset](https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d), which shows information about people involved in a crash and if any injuries were sustained.

### Data API
The [Socrata Open Data API (SODA)](https://dev.socrata.com/foundry/data.cityofchicago.org/u6pd-qa9d) provides programmatic access to this dataset, including the ability to filter, query, and aggregate data.


## Technologies
The technologies I used in developing the pipeline are:

    Cloud: GCP
    Data Lake: GCS buckets
    Data Warehouse: BigQuery
    Infrastructure as Code (IaC): Terraform
    Workflow Orchestration: Prefect cloud
    Flow containerization: Docker
    Data Transformation: DBT cloud
    Data Visualization: Looker Studio
    

    


    
