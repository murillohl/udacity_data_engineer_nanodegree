# Data Engineering Capstone Project

## Introduction
League of Legends is a team-based strategy game where two teams of five powerful champions face off to destroy the other’s base. We can choose over 140 different champions. The focus is take down towers as you battle and destroy the enemy’s Nexus first to win the game. The MOBA is currently the most played online game with a community of over 30 million people per day. This project consist in utilize data about Ranked matches over all regions available in Kaggle and using the Riot API Portal to transform and manipulate this data, resulting in analytics tables that can be easily explore to run tests, build models or get insights about Ranked Matches.

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

 **Table stats**

 |-- id: integer (nullable = true)
 |-- win: integer (nullable = true)
 |-- item1: integer (nullable = true)
 |-- item2: integer (nullable = true)
 |-- item3: integer (nullable = true)
 |-- item4: integer (nullable = true)
 |-- item5: integer (nullable = true)
 |-- item6: integer (nullable = true)
 |-- trinket: integer (nullable = true)
 |-- kills: integer (nullable = true)
 |-- deaths: integer (nullable = true)
 |-- assists: integer (nullable = true)
 |-- largestkillingspree: integer (nullable = true)
 |-- largestmultikill: integer (nullable = true)
 |-- killingsprees: integer (nullable = true)
 |-- longesttimespentliving: integer (nullable = true)
 |-- doublekills: integer (nullable = true)
 |-- triplekills: integer (nullable = true)
 |-- quadrakills: integer (nullable = true)
 |-- pentakills: integer (nullable = true)
 |-- legendarykills: integer (nullable = true)
 |-- totdmgdealt: integer (nullable = true)
 |-- magicdmgdealt: integer (nullable = true)
 |-- physicaldmgdealt: integer (nullable = true)
 |-- truedmgdealt: integer (nullable = true)
 |-- largestcrit: integer (nullable = true)
 |-- totdmgtochamp: integer (nullable = true)
 |-- magicdmgtochamp: integer (nullable = true)
 |-- physdmgtochamp: integer (nullable = true)
 |-- truedmgtochamp: integer (nullable = true)
 |-- totheal: integer (nullable = true)
 |-- totunitshealed: integer (nullable = true)
 |-- dmgselfmit: integer (nullable = true)
 |-- dmgtoobj: integer (nullable = true)
 |-- dmgtoturrets: integer (nullable = true)
 |-- visionscore: integer (nullable = true)
 |-- timecc: integer (nullable = true)
 |-- totdmgtaken: integer (nullable = true)
 |-- magicdmgtaken: integer (nullable = true)
 |-- physdmgtaken: integer (nullable = true)
 |-- truedmgtaken: integer (nullable = true)
 |-- goldearned: integer (nullable = true)
 |-- goldspent: integer (nullable = true)
 |-- turretkills: integer (nullable = true)
 |-- inhibkills: integer (nullable = true)
 |-- totminionskilled: integer (nullable = true)
 |-- neutralminionskilled: integer (nullable = true)
 |-- ownjunglekills: integer (nullable = true)
 |-- enemyjunglekills: integer (nullable = true)
 |-- totcctimedealt: integer (nullable = true)
 |-- champlvl: integer (nullable = true)
 |-- pinksbought: integer (nullable = true)
 |-- wardsbought: string (nullable = true)
 |-- wardsplaced: integer (nullable = true)
 |-- wardskilled: integer (nullable = true)
 |-- firstblood: integer (nullable = true)
 
Then we will have the following dimensions tables (we can get more, but for this project will be using just this ones) for the **Fact Table**.

 **Table champs**
 
 |-- name: string (nullable = true)
 |-- id: integer (nullable = true)
 
 **Table participants**
 
 |-- id: integer (nullable = true)
 |-- matchid: integer (nullable = true)
 |-- player: integer (nullable = true)
 |-- championid: integer (nullable = true)
 |-- ss1: integer (nullable = true)
 |-- ss2: integer (nullable = true)
 |-- role: string (nullable = true)
 |-- position: string (nullable = true)
 
 **Table matches**
 
 |-- id: integer (nullable = true)
 |-- gameid: string (nullable = true)
 |-- platformid: string (nullable = true)
 |-- queueid: integer (nullable = true)
 |-- seasonid: integer (nullable = true)
 |-- duration: double (nullable = true)
 |-- creation: double (nullable = true)
 |-- version: string (nullable = true)
 
 **Table teamstats**
 
 |-- matchid: integer (nullable = true)
 |-- teamid: integer (nullable = true)
 |-- firstblood: integer (nullable = true)
 |-- firsttower: integer (nullable = true)
 |-- firstinhib: integer (nullable = true)
 |-- firstbaron: integer (nullable = true)
 |-- firstdragon: integer (nullable = true)
 |-- firstharry: integer (nullable = true)
 |-- towerkills: integer (nullable = true)
 |-- inhibkills: integer (nullable = true)
 |-- baronkills: integer (nullable = true)
 |-- dragonkills: integer (nullable = true)
 |-- harrykills: integer (nullable = true)
 
 **Table queues**
 
 |-- description: string (nullable = true)
 |-- map: string (nullable = true)
 |-- notes: string (nullable = true)
 |-- queueId: integer (nullable = true)

After the ETL run's, we will have the two Analytics Table with the following structure:

 **Table players_statistics_per_match** - Partition By create_match_year, create_match_month
 
 |-- participants_id: integer (nullable = true)
 |-- matchid: integer (nullable = true)
 |-- player: integer (nullable = true)
 |-- championid: integer (nullable = true)
 |-- ss1: integer (nullable = true)
 |-- ss2: integer (nullable = true)
 |-- role: string (nullable = true)
 |-- position: string (nullable = true)
 |-- stats_id: integer (nullable = true)
 |-- win: integer (nullable = true)
 |-- item1: integer (nullable = true)
 |-- item2: integer (nullable = true)
 |-- item3: integer (nullable = true)
 |-- item4: integer (nullable = true)
 |-- item5: integer (nullable = true)
 |-- item6: integer (nullable = true)
 |-- trinket: integer (nullable = true)
 |-- kills: integer (nullable = true)
 |-- deaths: integer (nullable = true)
 |-- assists: integer (nullable = true)
 |-- largestkillingspree: integer (nullable = true)
 |-- largestmultikill: integer (nullable = true)
 |-- killingsprees: integer (nullable = true)
 |-- longesttimespentliving: integer (nullable = true)
 |-- doublekills: integer (nullable = true)
 |-- triplekills: integer (nullable = true)
 |-- quadrakills: integer (nullable = true)
 |-- pentakills: integer (nullable = true)
 |-- legendarykills: integer (nullable = true)
 |-- totdmgdealt: integer (nullable = true)
 |-- magicdmgdealt: integer (nullable = true)
 |-- physicaldmgdealt: integer (nullable = true)
 |-- truedmgdealt: integer (nullable = true)
 |-- largestcrit: integer (nullable = true)
 |-- totdmgtochamp: integer (nullable = true)
 |-- magicdmgtochamp: integer (nullable = true)
 |-- physdmgtochamp: integer (nullable = true)
 |-- truedmgtochamp: integer (nullable = true)
 |-- totheal: integer (nullable = true)
 |-- totunitshealed: integer (nullable = true)
 |-- dmgselfmit: integer (nullable = true)
 |-- dmgtoobj: integer (nullable = true)
 |-- dmgtoturrets: integer (nullable = true)
 |-- visionscore: integer (nullable = true)
 |-- timecc: integer (nullable = true)
 |-- totdmgtaken: integer (nullable = true)
 |-- magicdmgtaken: integer (nullable = true)
 |-- physdmgtaken: integer (nullable = true)
 |-- truedmgtaken: integer (nullable = true)
 |-- goldearned: integer (nullable = true)
 |-- goldspent: integer (nullable = true)
 |-- turretkills: integer (nullable = true)
 |-- inhibkills: integer (nullable = true)
 |-- totminionskilled: integer (nullable = true)
 |-- neutralminionskilled: integer (nullable = true)
 |-- ownjunglekills: integer (nullable = true)
 |-- enemyjunglekills: integer (nullable = true)
 |-- totcctimedealt: integer (nullable = true)
 |-- champlvl: integer (nullable = true)
 |-- pinksbought: integer (nullable = true)
 |-- wardsbought: string (nullable = true)
 |-- wardsplaced: integer (nullable = true)
 |-- wardskilled: integer (nullable = true)
 |-- firstblood: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- champs_id: integer (nullable = true)
 |-- match_id: integer (nullable = true)
 |-- gameid: string (nullable = true)
 |-- platformid: string (nullable = true)
 |-- queueid: integer (nullable = true)
 |-- seasonid: integer (nullable = true)
 |-- duration: double (nullable = true)
 |-- creation: double (nullable = true)
 |-- version: string (nullable = true)
 |-- create_match_date: timestamp (nullable = true)
 |-- create_match_hour: integer (nullable = true)
 |-- create_match_day: integer (nullable = true)
 |-- create_match_month: integer (nullable = true)
 |-- create_match_year: integer (nullable = true)
 
 **Table teams_statistics_per_match** - Partition By create_match_year, create_match_month
 
 |-- team_side: string (nullable = true)
 |-- matchid: integer (nullable = true)
 |-- win: integer (nullable = true)
 |-- total_kills: long (nullable = true)
 |-- total_deaths: long (nullable = true)
 |-- total_assists: long (nullable = true)
 |-- total_doublekills: long (nullable = true)
 |-- total_triplekills: long (nullable = true)
 |-- total_quadrakills: long (nullable = true)
 |-- total_pentakills: long (nullable = true)
 |-- total_totdmgdealt: long (nullable = true)
 |-- total_magicdmgdealt: long (nullable = true)
 |-- total_physicaldmgdealt: long (nullable = true)
 |-- total_truedmgdealt: long (nullable = true)
 |-- total_largestcrit: long (nullable = true)
 |-- total_totdmgtochamp: long (nullable = true)
 |-- total_magicdmgtochamp: long (nullable = true)
 |-- total_physdmgtochamp: long (nullable = true)
 |-- total_truedmgtochamp: long (nullable = true)
 |-- total_totheal: long (nullable = true)
 |-- total_totunitshealed: long (nullable = true)
 |-- total_dmgselfmit: long (nullable = true)
 |-- total_dmgtoobj: long (nullable = true)
 |-- total_dmgtoturrets: long (nullable = true)
 |-- total_visionscore: long (nullable = true)
 |-- total_goldearned: long (nullable = true)
 |-- total_goldspent: long (nullable = true)
 |-- total_totminionskilled: long (nullable = true)
 |-- total_neutralminionskilled: long (nullable = true)
 |-- total_pinksbought: long (nullable = true)
 |-- total_wardsbought: double (nullable = true)
 |-- total_wardsplaced: long (nullable = true)
 |-- total_wardskilled: long (nullable = true)
 |-- gameid: string (nullable = true)
 |-- platformid: string (nullable = true)
 |-- queueid: integer (nullable = true)
 |-- seasonid: integer (nullable = true)
 |-- duration: double (nullable = true)
 |-- creation: double (nullable = true)
 |-- version: string (nullable = true)
 |-- description: string (nullable = true)
 |-- map: string (nullable = true)
 |-- notes: string (nullable = true)
 |-- team_stats_match_id: integer (nullable = true)
 |-- firstblood: integer (nullable = true)
 |-- firsttower: integer (nullable = true)
 |-- firstinhib: integer (nullable = true)
 |-- firstbaron: integer (nullable = true)
 |-- firstdragon: integer (nullable = true)
 |-- firstharry: integer (nullable = true)
 |-- towerkills: integer (nullable = true)
 |-- inhibkills: integer (nullable = true)
 |-- baronkills: integer (nullable = true)
 |-- dragonkills: integer (nullable = true)
 |-- harrykills: integer (nullable = true)
 |-- create_match_date: timestamp (nullable = true)
 |-- create_match_hour: integer (nullable = true)
 |-- create_match_day: integer (nullable = true)
 |-- create_match_month: integer (nullable = true)
 |-- create_match_year: integer (nullable = true)

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
4. **explore_data.ipynb** python notebook used to model the etl pipeline, explore the data and find oportunities with the data.
5. **etl.py** final ETL file to execute and process all ingest and transformations. 
6. **requirements.txt** text file to install pythons packages dependencys
7. We also need a **dhw.cfg** file that is not in this repository containing the following variables to reference the s3 buckets and csv path variables 

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

* How to scale the project to 100x data: Since we are using spark in all process steps, we will just need to adjust the clusters configuration power to deal better with this. In terms of Storage, we dont need to worry about also because using S3 and parquet files the data persisted can scale to lots of terabytes without any problems.

* Pipeline running on 7AM: We would need to use a scheduler to run the ETL. To this we can use Airflow, or some solutions from AWS like Glue Jobs scheduleds, AWS Batch or Step Functions.

* The database needed to be accesed by 100+ peoples: Since we are using S3 as storage, we can map this parquet files in AWS Athena Query, or also load the Analytics Tables in AWS Redshift. Its also possible to use S3 reads direct with HTTPS requests, since parquet files are already optimized for read.
