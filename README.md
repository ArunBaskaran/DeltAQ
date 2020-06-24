# DeltAQ : A scalable data warehouse for spatio-temporal analysis of air quality data

### Table of Contents  
1. [Introduction](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#introduction)
2. [Directory Structure](https://github.com/ArunBaskaran/DeltAQ/blob/develop/README.md#directory-structure)
3. [Data Sources](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#data-sources)
4. [Tech Stack](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#tech-stack)
5. [Grafana demo](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#grafana-demo)
6. [Usage Instructions](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#usage-instructions)

### Introduction

Air quality is an important metric that influences health indicators such as life expectancy as well as economic indicators such as real-estate prices. Different factors play a role in determining the amount of pollutants in the air, like weather, economic activity, etc. This makes the data amenable to analysis both on the spatial as well as a temporal scale. From the perspective of use-case scenarios, this project has two main sets of stakeholders. The first group are the policy makers - like city planners - and DeltAQ helps them perform long-term planning, take informed anti-pollution measures, generate alarms, etc. The second targeted group are the environmental orgs and ngos that need to create reports and comparative studies. 

From the point of view of data, what makes this a challenging problem is that the raw data does not exist in a queryable form. It has a nested and a convoluted schema, and columns that are not required for an OLAP warehouse. In addition, the geolocation data is not intuitive from a user perspective (for example, indexed by zip codes). Towards addressing these issues, I have built an ETL pipeline that ingests historical and live air quality data, indexes the data according to zip-code, and loads the data into a TimescaleDB that can be queried from using Grafana 


### Directory Structure 

```
.
├── LICENSE
├── README.md
└── src
    ├── Data_examples
    │   ├── 1592513465.ndjson
    │   └── uszips.csv
    ├── Demo_video_1.gif
    ├── Geospatial_integration_Spark
    │   └── integrate_geo_data.py
    ├── Grafana_queries
    │   └── Queries.sql
    ├── Live_DataIngestion_SQS
    │   └── livedata_ndjson.py
    ├── Old_DataIngestion_Spark
    │   └── import_ts_ndjson.py
    └── Pipeline.png
```

### Data sources

Representative data files used for this project have been uploaded to the "Data" sub-repo. 

For complete data:
* OpenAQ data: https://openaq-fetches.s3.amazonaws.com/index.html
* Zip-code data: https://simplemaps.com/data/us-zips


### Tech Stack
![image](src/Pipeline.png)

### Grafana demo

<img src="src/Demo_video_1.gif" alt="drawing" width="1000" height="500"/>

### Usage instructions  (Detailed instructions to follow)

#### Required installations

* Spark v2.4.5: AWS EC2 four m5 nodes (1 master and 3 workers) 
* AWS SQS
* Postgres 10.0
* TimescaleDB 10.0
* Grafana 


#### Config options (To-do)

