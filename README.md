# Chicago Crashes Data Engineering project
This is the final project of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks Club](https://datatalks.club)

## Introduction

Chicago is one of the biggest states in the US that is full of life. Developing a data pipeline for the Crash data will help analytics to understand why accidents 
happen and how to prevent them, which will help save lives and make the city a safer place. It also guides how we plan our streets and neighborhoods.

## Data Source
The traffic crashes dataset is taken from the [Chicago data portal](https://data.cityofchicago.org/browse?limitTo=datasets), which contains various open-access datasets related to the city.
In my project I used two data sets; the [Traffic Crashes - Crashes dataset](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if), which shows information about each traffic crash on city streets within the City of Chicago and
the [Traffic Crashes - People dataset](https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d) which shows information about people involved in a crash and if any injuries were sustained.


## Technologies
The technologies that were chosen to use are the following:

    Cloud: GCP
    Data Lake: GCS buckets
    Data Warehouse: BigQuery
    Infrastructure as code (IaC): Terraform
    Workflow Orchestration: Prefect cloud
    Transforming Data: DBT cloud
    Data Visualization: Looker Studio
    
