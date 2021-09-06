import pandas as pd
import configparser
from datetime import datetime
import os
import numpy as np
from pyspark.sql.functions import sum,max,min,count
from schemas import dict_schemas, dict_numeric_columns, tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl,StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TS

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ 
    Create and return a Spark Session 
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_csv_data_to_parquet(spark, csv_tables, csv_raw_data, output_s3_path):
    """
        Transform the csv datasets in parquet files and load then to from s3, make some processing and extract
    the fields needs to populate the songs and artists dimensions tables
    
        params:
            spark: spark session
            csv_tables: list of tables to process
            csv_raw_data: path of the csv files
            output_s3_path: output path of parquet files
            
    """
    for table in csv_tables:
        
        # creating csv path file
        input_data_csv = r'{}/{}.csv'.format(csv_raw_data, table)
        print(f"path: {input_data_csv}")

        # read csv data file
        spark_df = spark.read.options(header='true').csv(input_data_csv, schema = dict_schemas[table])
    
        # count rows to check
        csv_rows = spark_df.count()
    
        # droping rows if all values are null
        spark_df = spark_df.dropna(how = 'all')
        
        # write table to parquet files in S3
        spark_df.write.parquet(f"{output_s3_path}/{table}")
        
        #checking if parquet files and csv files rows are the same
        check_csv_and_parquet_data(spark, spark_df, output_s3_path, table, csv_rows)

        print(f"{table} load success")
    
def check_csv_and_parquet_data(spark, csv_df, output_s3_path, table, csv_rows):
    """
        Check if the count of rows in csv files are equal to parquet stored files,
        and print a Warning if its not.
        
        params:
            spark: spark session
            csv_df: spark dataframe from csv file
            output_s3_path: path of the parquet recent stored file
            table: table name to be checked
            csv_rows: spark dataframe from csv total rows
            
    """
    pqt = spark.read.parquet(f"{output_s3_path}/{table}")
    
    pqt_rows = pqt.count()
        
    if pqt_rows == csv_rows:
        print( f'{table}: {csv_rows} rows Ok !' )
    else:
        print( f'CSV has: {csv_rows} rows, while PQT has: {pqt_rows}' )
        
def creating_players_analytic_table(spark, s3_processed_data, output_s3_table):
    """
        Create the first Analytic Table with data by Players Matches.
        Load the datasets stored as parquet files at s3, make some processing like join stataments
        and cleaning, then stores the result table in parquet files on S3.
        
        params:
            spark: spark session
            s3_processed_data: path of the parquet files used as input
            output_s3_table: path of the result table to be stored
            
    """

    # read players match info log data file and identifying the id column
    participants = spark.read.parquet(f"{s3_processed_data}/participants")
    
    participants = participants.withColumnRenamed("id","participants_id")
    
    # read players stats log data file and identifying the id column
    stats = spark.read.parquet(f"{s3_processed_data}/stats")  
    
    stats = stats.withColumnRenamed("id","stats_id")
    
    # read champs log data file and identifying the id column
    champs = spark.read.parquet(f"{s3_processed_data}/champs")
    
    champs = champs.withColumnRenamed("id","champs_id")
    
    # read matches log data file and identifying the id column
    matches = spark.read.parquet(f"{s3_processed_data}/matches")
    
    matches = matches.withColumnRenamed("id","match_id")
    
    # Joining the tables to get all information about the matches
    
    player_df = participants.join(stats, ([stats.stats_id == participants.participants_id]), "left")
    
    player_df = player_df.join(champs, ([player_df.championid == champs.champs_id]), "left")
    
    player_df = player_df.join(matches, ([player_df.matchid == matches.match_id]), "left")
    
    # Making some quality checks like removing duplicates and checking if the dataframe is not empty
    
    player_df = player_df.dropDuplicates()
    
    check_df_is_not_empty(player_df)
    
    # create time columns from original timestamp creation column
    player_df = create_time_columns(player_df, 'creation')
    
    # drop redundant or useless columns 
    player_df = player_df.drop('matchid', 'champid', 'matchid')
    
    # Loading table in s3
    
    player_df.filter(player_df['create_match_year'] == 2017).write.partitionBy("create_match_year", "create_match_month").parquet(f"{output_s3_table}/players_statistics_per_match/")
    
def creating_teams_analytic_table(spark, s3_processed_data, output_s3_table):
    """
        Create the second Analytic Table with data by Teams Matches
        Load the datasets stored as parquet files at s3, make some processing like join stataments
        and cleaning, then stores the result table in parquet files on S3.
        
        params:
            spark: spark session
            s3_processed_data: path of the parquet files used as input
            output_s3_table: path of the result table to be stored
            
    """
    
    # read players match info log data file and identifying the id column
    participants = spark.read.parquet(f"{s3_processed_data}/participants")
    
    participants = participants.withColumnRenamed("id","participants_id")
    
    # read players stats log data file
    stats = spark.read.parquet(f"{s3_processed_data}/stats")  
    
    stats = stats.withColumnRenamed("id","stats_id")
    
    # Joining the tables to get all information about the matches
    teams_df = participants.join(stats, ([participants.participants_id == stats.stats_id]), "left")
    
    # create side team column from player column
    get_side_team = udf(lambda x: 'Blue' if x <= 5 else 'Red', Str())

    teams_df = teams_df.withColumn("team_side", get_side_team(col("player")))
    
    # manipulating data to exclude matches data that dont have info about all 10 players
    count_players = teams_df.groupBy("matchid").count().withColumnRenamed('matchid', 'count_match_id')
    
    teams_df = teams_df.join(count_players, ([teams_df.matchid == count_players.count_match_id]), "left")
    
    teams_df = teams_df.filter(teams_df['count'] == 10)
    
    # Grouping infos by Match and Team Side
    teams_df = teams_df.groupBy('team_side', 'matchid', 'win').agg(\
                            sum('kills').alias('total_kills'), \
                            sum('deaths').alias('total_deaths'), \
                            sum('assists').alias('total_assists'), \
                            sum('doublekills').alias('total_doublekills'), \
                            sum('triplekills').alias('total_triplekills'), \
                            sum('quadrakills').alias('total_quadrakills'), \
                            sum('pentakills').alias('total_pentakills'), \
                            sum('totdmgdealt').alias('total_totdmgdealt'), \
                            sum('magicdmgdealt').alias('total_magicdmgdealt'), \
                            sum('physicaldmgdealt').alias('total_physicaldmgdealt'), \
                            sum('truedmgdealt').alias('total_truedmgdealt'), \
                            sum('largestcrit').alias('total_largestcrit'), \
                            sum('totdmgtochamp').alias('total_totdmgtochamp'), \
                            sum('magicdmgtochamp').alias('total_magicdmgtochamp'), \
                            sum('physdmgtochamp').alias('total_physdmgtochamp'), \
                            sum('truedmgtochamp').alias('total_truedmgtochamp'), \
                            sum('totheal').alias('total_totheal'), \
                            sum('totunitshealed').alias('total_totunitshealed'), \
                            sum('dmgselfmit').alias('total_dmgselfmit'), \
                            sum('dmgtoobj').alias('total_dmgtoobj'), \
                            sum('dmgtoturrets').alias('total_dmgtoturrets'), \
                            sum('visionscore').alias('total_visionscore'), \
                            sum('goldearned').alias('total_goldearned'), \
                            sum('goldspent').alias('total_goldspent'), \
                            sum('totminionskilled').alias('total_totminionskilled'), \
                            sum('neutralminionskilled').alias('total_neutralminionskilled'), \
                            sum('pinksbought').alias('total_pinksbought'), \
                            sum('wardsbought').alias('total_wardsbought'), \
                            sum('wardsplaced').alias('total_wardsplaced'), \
                            sum('wardskilled').alias('total_wardskilled'))
        
    print(f"groupby done ! rows: {teams_df.count()}")
    # read matches log data files with additional informations and adding to our df
    
    matches = spark.read.parquet(f"{s3_processed_data}/matches")
    
    matches = matches.withColumnRenamed("id","match_id")

    teams_df = teams_df.join(matches, ([teams_df.matchid == matches.match_id]), "left")
    
    # read and join dimension queue table in our df to identify the ranked match type
    
    queues = spark.read.parquet(f"{s3_processed_data}/queues")

    queues = queues.withColumnRenamed("queueId","queue_id")
    
    teams_df = teams_df.join(queues, ([teams_df.queueid == queues.queue_id]), "left")

    # read teamstats log data files with additional informations and adding to our df
    teamstats = spark.read.parquet(f"{s3_processed_data}/teamstats")

    teamstats = teamstats.withColumnRenamed("matchid","team_stats_match_id")
    
     # transform teamid to BLue(100) or Red(200) to join our df with teamstats data
    get_side_team_by_id = udf(lambda x: 'Blue' if int(x) == 100 else 'Red', Str())
    
    teamstats = teamstats.withColumn("side_team", get_side_team_by_id(col("teamid")))
    
    teams_df = teams_df.join(teamstats, ([teams_df.matchid == teamstats.team_stats_match_id,
                                          teams_df.team_side == teamstats.side_team]), "left")
    
    # create time columns from original timestamp creation column
    teams_df = create_time_columns(teams_df, 'creation')
    
    # drop redundant or useless columns 
    teams_df = teams_df.drop('side_team', 'match_id', 'queue_id', 'teamid')
    
    # Making some quality checks like removing duplicates and checking if the dataframe is not empty
    
    teams_df = teams_df.dropDuplicates()
    
    check_df_is_not_empty(teams_df)
    
    # loading data to S3 partition by year and month of the create date match
    teams_df.write.partitionBy("create_match_year", "create_match_month").parquet(f"{output_s3_table}/teams_statistics_per_match/")

def create_time_columns(df, ts_col):
    """
        Break timestamp column in time columns like year, month, day and hour 
        
        params:
            df: spark dataframe
            ts_col: timestamp column to use as parameter
            
    """
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TS())

    # create date column from original timestamp ts_col column
    df = df.withColumn("create_match_date", get_timestamp(col(ts_col)))
    
    # extract time detailed time columns
    df = df.withColumn("create_match_hour", hour("create_match_date"))
    df = df.withColumn("create_match_day", dayofmonth("create_match_date"))
    df = df.withColumn("create_match_month", month("create_match_date"))
    df = df.withColumn("create_match_year", year("create_match_date"))
    
    return df

def check_df_is_not_empty(df):
    """
        Check if the dataframe is not empty 
        
        params:
            df: spark dataframe
            
    """
    if df.count() > 0:
        print('Check OK !')
    else:
        raise ValueError('DataFrame empty')
        
def main():
    """
        Main function to execute the ETL process using Spark
    """
    
    csv_tables = tables
    
    spark = create_spark_session()

    csv_raw_data = config.get("PATH", "CSV_RAW_DATA_PATH")
    parquet_s3_data = config.get("PATH", "PARQUET_S3_DATA_PATH")
    analysis_s3_table = config.get("PATH", "ANALYSIS_S3_TABLES_PATH")
    
    print('Processing csv files to parquet')
    process_csv_data_to_parquet(spark, csv_tables, csv_raw_data, parquet_s3_data)
    
    print('Processing PLAYERS match info stats table')
    creating_players_analytic_table(spark, parquet_s3_data, analysis_s3_table)
    
    print('Processing TEAM match info stats table')
    creating_teams_analytic_table(spark, parquet_s3_data, analysis_s3_table)


if __name__ == "__main__":
    main()
