from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl,StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TS


champs_schema = R([
        Fld("name", Str(), True),
        Fld("id", Int(), True),
    ])

champs_int_columns = ['id']

seasons_schema = R([
        Fld("id", Int(), True),
        Fld("season", Str(), True),
    ])

seasons_int_columns = ['id']

maps_schema = R([
        Fld("mapId", Int(), True),
        Fld("mapName", Str(), True),
        Fld("notes", Str(), True),
    ])

maps_int_columns = ['mapId']

matches_schema = R([
        Fld("id", Int(), True),
        Fld("gameid", Str(), True),
        Fld("platformid", Str(), True),
        Fld("queueid", Int(), True),
        Fld("seasonid", Int(), True),
        Fld("duration", Dbl(), True),
        Fld("creation", Dbl(), True),
        Fld("version", Str(), True),
    ])

matches_int_columns = ['id', 'gameid', 'queueid', 'seasonid', 'duration', 'creation']

participants_schema = R([
        Fld("id", Int(), True),
        Fld("matchid", Int(), True),
        Fld("player", Int(), True),
        Fld("championid", Int(), True),
        Fld("ss1", Int(), True),
        Fld("ss2", Int(), True),
        Fld("role", Str(), True),
        Fld("position", Str(), True),
    ])

participants_int_columns = ['id', 'matchid', 'player', 'championid', 'ss1', 'ss2']

queues_schema = R([
        Fld("description", Str(), True),
        Fld("map", Str(), True),
        Fld("notes", Str(), True),
        Fld("queueId", Int(), True),
    ])

queues_int_columns = ['queueId']

stats_schema = R([
        Fld("id", Int(), True),
        Fld("win", Int(), True),
        Fld("item1", Int(), True),
        Fld("item2", Int(), True),
        Fld("item3", Int(), True),
        Fld("item4", Int(), True),
        Fld("item5", Int(), True),
        Fld("item6", Int(), True),
        Fld("trinket", Int(), True),
        Fld("kills", Int(), True),
        Fld("deaths", Int(), True),
        Fld("assists", Int(), True),
        Fld("largestkillingspree", Int(), True),
        Fld("largestmultikill", Int(), True),
        Fld("killingsprees", Int(), True),
        Fld("longesttimespentliving", Int(), True),
        Fld("doublekills", Int(), True),
        Fld("triplekills", Int(), True),
        Fld("quadrakills", Int(), True),
        Fld("pentakills", Int(), True),
        Fld("legendarykills", Int(), True),
        Fld("totdmgdealt", Int(), True),
        Fld("magicdmgdealt", Int(), True),
        Fld("physicaldmgdealt", Int(), True),
        Fld("truedmgdealt", Int(), True),
        Fld("largestcrit", Int(), True),
        Fld("totdmgtochamp", Int(), True),
        Fld("magicdmgtochamp", Int(), True),
        Fld("physdmgtochamp", Int(), True),
        Fld("truedmgtochamp", Int(), True),
        Fld("totheal", Int(), True),
        Fld("totunitshealed", Int(), True),
        Fld("dmgselfmit", Int(), True),
        Fld("dmgtoobj", Int(), True),
        Fld("dmgtoturrets", Int(), True),
        Fld("visionscore", Int(), True),
        Fld("timecc", Int(), True),
        Fld("totdmgtaken", Int(), True),
        Fld("magicdmgtaken", Int(), True),
        Fld("physdmgtaken", Int(), True),
        Fld("truedmgtaken", Int(), True),
        Fld("goldearned", Int(), True),
        Fld("goldspent", Int(), True),
        Fld("turretkills", Int(), True),
        Fld("inhibkills", Int(), True),
        Fld("totminionskilled", Int(), True),
        Fld("neutralminionskilled", Int(), True),
        Fld("ownjunglekills", Int(), True),
        Fld("enemyjunglekills", Int(), True),
        Fld("totcctimedealt", Int(), True),
        Fld("champlvl", Int(), True),
        Fld("pinksbought", Int(), True),
        Fld("wardsbought", Str(), True),
        Fld("wardsplaced", Int(), True),
        Fld("wardskilled", Int(), True),
        Fld("firstblood", Int(), True),
    ])

stats_int_columns = ['id', 'win', 'item1', 'item2', 'item3', 'item4', 'item5', 'item6',
       'trinket', 'kills', 'deaths', 'assists', 'largestkillingspree',
       'largestmultikill', 'killingsprees', 'longesttimespentliving',
       'doublekills', 'triplekills', 'quadrakills', 'pentakills',
       'legendarykills', 'totdmgdealt', 'magicdmgdealt', 'physicaldmgdealt',
       'truedmgdealt', 'largestcrit', 'totdmgtochamp', 'magicdmgtochamp',
       'physdmgtochamp', 'truedmgtochamp', 'totheal', 'totunitshealed',
       'dmgselfmit', 'dmgtoobj', 'dmgtoturrets', 'visionscore', 'timecc',
       'totdmgtaken', 'magicdmgtaken', 'physdmgtaken', 'truedmgtaken',
       'goldearned', 'goldspent', 'turretkills', 'inhibkills',
       'totminionskilled', 'neutralminionskilled', 'ownjunglekills',
       'enemyjunglekills', 'totcctimedealt', 'champlvl', 'pinksbought',
       'wardsbought', 'wardsplaced', 'wardskilled', 'firstblood']

teambans_schema = R([
        Fld("matchid", Int(), True),
        Fld("teamid", Int(), True),
        Fld("championid", Int(), True),
        Fld("banturn", Int(), True),
    ])

teambans_int_columns = ['matchid', 'teamid', 'championid', 'banturn']

teamstats_schema = R([
        Fld("matchid", Int(), True),
        Fld("teamid", Int(), True),
        Fld("firstblood", Int(), True),
        Fld("firsttower", Int(), True),
        Fld("firstinhib", Int(), True),
        Fld("firstbaron", Int(), True),
        Fld("firstdragon", Int(), True),
        Fld("firstharry", Int(), True),
        Fld("towerkills", Int(), True),
        Fld("inhibkills", Int(), True),
        Fld("baronkills", Int(), True),
        Fld("dragonkills", Int(), True),
        Fld("harrykills", Int(), True),
    ])

teamstats_int_columns = ['matchid', 'teamid', 'firstblood', 'firsttower', 'firstinhib',
       'firstbaron', 'firstdragon', 'firstharry', 'towerkills', 'inhibkills',
       'baronkills', 'dragonkills', 'harrykills']

tables = ['champs', 'maps', 'matches', 'participants', 'queues', 'seasons', 'stats', 'teambans', 'teamstats']

dict_schemas = {'champs': champs_schema, 
                'seasons': seasons_schema, 
                'maps': maps_schema,
                'matches': matches_schema, 
                'participants': participants_schema,
                'queues': queues_schema,
                'stats': stats_schema,
                'teambans': teambans_schema,
                'teamstats': teamstats_schema}

dict_numeric_columns = {'champs': champs_int_columns, 
                'seasons': seasons_int_columns, 
                'maps': maps_int_columns,
                'matches': matches_int_columns, 
                'participants': participants_int_columns,
                'queues': queues_int_columns,
                'stats': stats_int_columns,
                'teambans': teambans_int_columns,
                'teamstats': teamstats_int_columns}

