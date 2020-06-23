# DeltAQ : A scalable data warehouse for spatio-temporal analysis of air quality data

### Table of Contents  
1. [Introduction](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#introduction)
2. [Data Sources](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#data-sources)
3. [Tech Stack](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#tech-stack)
4. [Front-end demo](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#front-end-demo)
4. [Usage Instructions](https://github.com/ArunBaskaran/DeltAQ/tree/develop/aws-implementation#usage-instructions)

### Introduction

Air quality is an important metric that influences health indicators such as life expectancy as well as economic indicators such as real-estate prices. Different factors play a role in determining the amount of pollutants in the air, like weather, economic activity, etc. This makes the data amenable to analysis both on the spatial as well as a temporal scale. From the perspective of use-case scenarios, this project has two main sets of stakeholders. The first group are the policy makers - like city planners - and DeltAQ helps them perform long-term planning, take informed anti-pollution measures, generate alarms, etc. The second targeted group are the environmental orgs and ngos that need to create reports and comparative studies. 

From the point of view of data, what makes this a challenging problem is that the raw data does not exist in a queryable form. It has a nested and a convoluted schema, and columns that are not required for an OLAP warehouse. In addition, the geolocation data is not intuitive from a user perspective (for example, indexed by zip codes). Towards addressing these issues, I have built an ETL pipeline that ingests historical and live air quality data, indexes the data according to zip-code, and loads the data into a TimescaleDB that can be queried from using Grafana 


### Data sources

Representative data files used for this project have been uploaded to the "Data" sub-repo. 

For complete data:
* OpenAQ data: https://openaq-fetches.s3.amazonaws.com/index.html
* Zip-code data: https://simplemaps.com/data/us-zips


### Tech Stack
![image](Pipeline.png)

### Front-end demo

<img src="Demo_video_1.gif" alt="drawing" width="1000" height="500"/>

### Usage instructions
