# Data Engineering Capstone Project

## Introduction
League of Legends is a team-based strategy game where two teams of five powerful champions face off to destroy the other’s base. We can choose over 140 different champions. The focus is take down towers as you battle and destroy the enemy’s Nexus first to win the game. The MOBA is currently the most played online game with a community of over 30 million people per day. This project consist in utilize data about Ranked matches over all regions available in Kaggle and using the Riot API Portal to transform and manipulate this data, resulting in analytics tables that can be easily explore to run tests, build models or get insights about Ranked Matches, like the most/less played champions in a season, or the higher/lower win rate champion.

## Project Description
In this project, we'll use a CSV Datasets available in Kaggle with over 1.8million rows about ranked matches info (our Fact Table), get some Dimensions tables from Riot API Developer in json with pandas Python library, transform all this data in parquet files stored in S3, then build a ETL that make some manipulation and cleaning data using Spark, and stores two analytics tables also in S3, one containing especific data about the players from the matches, and another containing aggregations data info about the entire match.

## Datasets Used

All data belongs to Riot Games API and their data policies applies.

We'll be working with the datasets provide by Kaggle resides in:

https://www.kaggle.com/paololol/league-of-legends-ranked-matches/data 

Containing the following data:

 - champs.csv
 - matches.csv
 - participants.csv
 - stats.csv
 - teambans.csv
 - teamstats.csv
 
 I also get some dimensions json data and stored as csv in the data_source folder to join in our data:
 
 https://developer.riotgames.com/docs/lol#data-dragon
 
  - gameModes.csv
  - maps.csv
  - queues.csv
  - seasons.csv

## Defining the Data Model

We'll be using the  Star Database Schema (Fact and Dimension Schema) for data modeling in this ETL pipeline. There is one fact table containing all the metrics (facts) associated to each player in a specific match, and the dimensions tables, containing associated information such as participants of the match, queue, team bans, team stats, maps, champs, seasons, etc. The goal is cleaning some data first then enables to explore and use all this data with the minimum number of JOINs possible.

### Schema for Matches Played Analysis

The stats dataset is a table with real data getting from the Riot API developer portal containing informations about the players in a match.

This will be our fact table that we are going to use to create our two Analytics Table. 

After the ETL run's, we will have the two Analytics Table with the following structure:

![image](https://user-images.githubusercontent.com/21292638/132261428-b5d7ff98-2dd5-4459-b1d6-303e597bd013.png)

The first is the players_statistics_per_match in the picture, and the second is using the same Schema but with some aggregation methods to gather info about the role team and get some insights in Team Side level, like win rate of Blue Side, firstblood rate of Red Side.

The full *Data Dictionary* is in the DataDictionary.md file

## Tools 

* We first used Pandas and Requests to get JSON data from Riot API and store as csv.
* After, we use Spark in all ETL process since the data contains a lot of joins, rows and columns.
* Then, we decided to save all in parquet format at S3, and partition the 2 analytics tables by year and month of the create match date.

### Data Quality Checks

During the ETL process, we have some checks like:

 - Check the count rows in parquet files from csv and dropping rows that contain only null values (func: check_csv_and_parquet_data).
 - Grant that there is no duplicate rows dropping if exists before writing the result table.
 - Check if the result table is not empty before writing the result table table (func: check_df_is_not_empty)
 
## Project Template

Files used on the project:
1. **data_source** folder with all csv data from Kaggle and Riot Developer Portal API.
2. **schemas.py** python file returning a dictionary mapping the parquet DataTypes schema for each table.
3. **json_collect_data.ipynb** python notebook used to collect dimensions table in json and store as csv in our data_source folder.
4. **explore_data.ipynb** python notebook used to model the etl pipeline, explore the data and find oportunities and insights with the data.
5. **etl.py** final ETL file to execute and process all ingest and transformations. 
6. **requirements.txt** text file to install pythons packages dependencys
7. **DataDictionary.md** Dictionary contains all columns by tables and contraints
8. We also need a **dhw.cfg** file that is not in this repository containing the following variables to reference the s3 buckets and csv path variables 

```
[PATH]
CSV_RAW_DATA_PATH = data_source
PARQUET_S3_DATA_PATH = 
ANALYSIS_S3_TABLES_PATH = 

[AWS]
AWS_ACCESS_KEY_ID = 
AWS_SECRET_ACCESS_KEY = 
```

## Steps to run the Project

1º Setup the dwh.cfg file in the root folder.

2º Install all the required packages in requirements.txt
```
pip install -r requiments.txt
```

3º Execute the following cells in the json_collect_data jupyter notebook to ensure that the jsons datasource are updated (*Optional*)

4º Run the ETL python file etl.py next:
```
python etl.py
```

5º Start use and explore the results !

### Writeups 

* How to scale the project to 100x data: Since we are using spark in all process steps, we will just need to adjust the clusters configuration power to deal better with this and choose the right columns to partition the Data to speedup the parallel processing, like times columns (year, month). In terms of Storage, we dont need to worry about also because using S3 and parquet files the data persisted can scale to lots of terabytes without any problems.

* Pipeline running on 7AM: We would need to use a scheduler to run the ETL. To this we can use Airflow, or some solutions from AWS like Glue Jobs scheduleds, AWS Batch or Step Functions.

* The database needed to be accesed by 100+ peoples: Since we are using S3 as storage, we can map this parquet files in AWS Athena Query, or also load the Analytics Tables in AWS Redshift. Its also possible to use S3 reads direct with HTTPS requests, since parquet files are already optimized for read.
