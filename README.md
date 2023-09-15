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
    

## Project flow


<img width="1254" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/039e47ba-9a52-4bab-83ed-8c9ced32117a">



1. The flow module first ingests the bulk data with two flows one for each dataset (crashes and people data), those two flows are scheduled only once by `cron` date `schedule` expressions.
2. As the datasets are updated daily, another two flows are scheduled to execute daily for extracting newly updated data for each day
3. After extracting data, data are stored in Google Data Storage (buckets) as raw Paquet files and transferred to Bigquery Data warehouse
4. At this point transformation takes place through DBT. `stg` models are incremental models to synchronize the data flow pipeline daily updates, this model simply cleans the raw data found in the warehouse.
5. Another model is the `core` model, which is a group of analytical models that generate a `view` from the `stg` model for selecting some interesting insights from the data for future visualization


<img width="902" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/05d9306f-8c48-4acd-9850-8c9b8152fa9d">




6. Finally, I fed the Google Looker Studio with these views and made a report from them in different charts [Crashes Report](https://lookerstudio.google.com/reporting/d094f99d-e859-46e4-b9f7-dc703e43078a)





## Some insights from the report

1. Injury severity ratio for each crash prime contribution cause:
   this heatmap illustrates the percentage of severe injury from total injuries for each reported crash cause. From this report, we can see that bicycles advancing on the red light and the physical condition of the driver
   are the two most common crash types that result in severe injuries

<img width="1142" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/2339ebc5-80de-4e82-838e-4ecc7d2571ac">

2. Ratio of each sex as a driver involved in the crash:
   from this pie chart, we can see that more than half of the crashes involve male drivers 

<img width="1094" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/30985e98-3cea-4366-82f7-f88284d22953">

   
3. Sex ratio as a driver for each crash cause:
   In this chart, we can see that the females' highest ratio in crash types are distraction from inside the vehicle and passing stopped school buses. On the other side, the male ratio is the highest in the obstructive crosswalk and under alcohol/drug influence crash types 
<img width="1170" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/fcad3e84-0565-4eef-9212-2234e2c905cf">


4. Weather influence on crashes:
   in the below chart, for each weather condition, we can see the percentage of crashes caused by the weather from the total days having that weather condition. We can conclude that severe cross-wind gates and blowing snow are the two most common weather
   conditions that can contribute to traffic crashes
   
<img width="1170" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/0af80841-8611-4ec1-901d-df4e6a069de1">

5. Relation between time of the day and month and its effect on crash frequency:
   from this chart, we can't see a significant effect of the daytime in different seasons on crash frequency except for early-morning crashes their frequency slightly decreases during May, June, and July. As a general overview
   crashes most often happen in the evening and least likely at midnight
   
<img width="1170" alt="image" src="https://github.com/aya9aladdin/chicago-crashes-DEproject/assets/27581535/30cabc7d-4a4b-4d1c-85de-820f1175673f">


