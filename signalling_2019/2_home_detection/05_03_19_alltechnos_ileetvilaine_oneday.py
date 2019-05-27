# ----------------------------------------------------------
# Population count test
# Technologies : 4G, 3G, 2G
# Time range : one day, 21PM-7AM
# Prior = uniform
# ----------------------------------------------------------
# 05/03/19 (mm-dd-yy)

# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 16G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py
# ------------------------------------------------------------------------------------------------------------
# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession
# conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.num.executors', '10'), ('spark.executor.cores', '3'), ('spark.default.parallelism', '100'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.sql.shuffle.partitions', '100'), ('spark.executorEnv.PYTHONHASHSEED', '321'), ('spark.sql.broadcastTimeout', '36000')])
# spark = SparkSession.builder.config(conf=conf).appName("Home Detection").getOrCreate()
# sc = spark.sparkContext
# ------------------------------------------------------------------------------------------------------------


# ============================
# Parametrage: Spark 2.1


# MODULES
import sys
from pyspark.sql.functions import (isnull, isnan, when, count, col, expr, 
round, lit, sum, avg, desc, concat, month, dayofmonth, hour, unix_timestamp,
broadcast)
from pyspark.sql.types import (StructType, StructField, IntegerType, 
StringType, TimestampType, DoubleType)
from pyspark.sql import SparkSession, Row, DataFrame
from functools import reduce

spark = SparkSession.builder.appName("Test appariement 4G").getOrCreate()
sc = spark.sparkContext

# IMPORT FUNCTIONS STORED IN A PYTHON FILE
program_path = 'hdfs:///User/zues5490/WORK/romain/inputs/common_functions.py'
sc.addFile(program_path)
from common_functions import *


# GENERAL PARAMETERS

hue_perso = "/User/zues5490/WORK/romain/"
hue_cancan = "/Data/P17100/SECURE/RAW/TXT/"

night_time = (21,7)




# Preprocessing of 4G data ----------------------------------------------------
 
# Import 4G data
df_4g = (spark.read.parquet(hue_cancan + 'S1C/2019/04/20/*'))
# Sample for test :
# df_4g = (spark.read.parquet(hue_cancan + "S1C/2019/04/20/19925827706671103"))

# Subset to night timerange
df_4g = subset_night(df_4g, "end_date_utc", night_time, psf)

# Subset to rows without error
df_4g = df_4g.where(col("cdr_result") == 0)

# Subset to rows without missing values for enodeb_id and cell_id
# (both necessay to compute complete CID)
# enodeb_id is almost never missing, but cell_id is in about 40% of events,
# mainly when type = 9 ("S1 Release")
df_4g = df_4g.where(col("enodeb_id").isNotNull() & col("cell_id").isNotNull())

# Creation of ci_sac column for merging : ci_sac = 256*enodeb_id + cell_id
# Verification possible using : https://www.cellmapper.net/enbid?lang=fr
# We also create lac column to have consistent columns with other technologies
# lac is always equal to 1 for 4G cells
df_4g = (df_4g
.withColumn("ci_sac", 256*col("enodeb_id") + col("cell_id"))
.withColumn("lac", lit(1))
)

# Keep relevant columns
df_4g = df_4g.selectExpr(
	"imsi", 
	"cast(lac as long) lac", 
	"cast(ci_sac as long) ci_sac")



# Preprocessing of 3G data ----------------------------------------------------

# Import 3G data
df_3g = (spark.read.parquet(hue_cancan + 'IU/2019/04/20/*'))
# Sample for test :
# df_3g = (spark.read.parquet(hue_cancan + "IU/2019/04/20/19964575927482404"))

# Subset to night timerange
df_3g = (df_3g
.withColumn("end_timestamp", 
	unix_timestamp(col("End_Time"), "dd/MM/yyyy HH:mm:ss"))
)

df_3g = subset_night(df_3g, "end_timestamp", night_time, psf)

# Subset to rows without missing values for SAC and LAC
# (both necessay to join with FV topology register)
df_3g = df_3g.where(col("LAC").isNotNull() & col("SAC_Value").isNotNull())

# Keep relevant columns and rename them as 4G
df_3g = (df_3g.selectExpr(
	"IMSI as imsi", 
	"cast(LAC as long) lac", 
	"cast(SAC_Value as long) ci_sac")
)




# Preprocessing of 2G data ----------------------------------------------------

# Import 2G data
df_2g = (spark.read.parquet(hue_cancan + 'A/2019/04/20/*'))
# Sample for test :
# df_2g = (spark.read.parquet(hue_cancan + "A/2019/04/20/19989484584153697"))

# Subset to night timerange
df_2g = (df_2g
.withColumn("end_timestamp", concat(col("end_day"), lit(" "), col("end_time")))
.withColumn("end_timestamp", 
	unix_timestamp(col("end_timestamp"), "dd/MM/yyyy HH:mm:ss"))
)

df_2g = subset_night(df_2g, "end_timestamp", night_time, psf)

# Subset to rows without missing values for SAC and LAC
# (both necessay to join with FV topology register)
df_2g = df_2g.where(col("LAC").isNotNull() & 
	col("Current_Cell_Identifier").isNotNull())

# Keep relevant columns and rename them as 4G
df_2g = df_2g.selectExpr("IMSI as imsi", 
	"cast(LAC as long) lac", 
	"cast(Current_Cell_Identifier as long) ci_sac")




# Concatenate 4G, 3G and 2G data ----------------------------------------------

concat_4g_3g = df_4g.union(df_3g)
mobile_data = concat_4g_3g.union(df_2g)

# Remove missing values (= invalid data)
mobile_data = mobile_data.na.drop()




# Preprocessing of FV topology register ---------------------------------------

# Import FV topology register
fv_topo = (spark.read
.option("delimiter", ";")
.option("header", True)
.option("inferSchema", True)
.csv(hue_perso + "inputs/Topologie_CSG_2018_08_28.csv")
)

# Preprocess data
fv_topo = (fv_topo
.where(col("Techno") == "4G")
.where(col("LAC") != "0") # Filter out invalid LAC
.withColumn("ci_sac", col("CI/SAC"))
.withColumn("lac", col("LAC"))
)

# Keep only cells inside a given spatial bounding box
# Were, we use "Ille et vilaine" bounding box
fv_topo_sub = (fv_topo
.filter(col("X").between(255036, 349782))
.filter(col("Y").between(2301823, 2420788))
)

# Keep relevant columns
fv_topo_sub = (fv_topo_sub
.selectExpr(
	"cast(lac as long) lac", 
	"cast(ci_sac as long) ci_sac")
)



# Merging on cell_id (ci_sac) and lac -----------------------------------------

# First (inner) join : get list of users who crossed at least one cell located
# in the bouding box we used to subset FV topology register
users_to_keep = (mobile_data
.join(broadcast(fv_topo_sub), ["lac", "ci_sac"])
.select("imsi")
.distinct())

# Second (inner) join : subset 4G data to all events made by users listed
# above (including events detected by cells not located in the bounding box)
mobile_data_sub = mobile_data.join(broadcast(users_to_keep), "imsi")



# Aggregation of individual events --------------------------------------------

# Count events by user x cell
mobile_data_agg = (mobile_data_sub
.groupBy("imsi", "lac", "ci_sac")
.count()
.withColumnRenamed("count", "n_calls"))




# Join with FV INDOOR coverage table ------------------------------------------

fv_indoor = spark.read.parquet(hue_perso + "inputs/fv_indoor_07_18_pqt")
fv_indoor = (fv_indoor
.withColumnRenamed("proba", "likelihood"))

# Create column giving FV coverage map .dma file name
mobile_data_agg = (mobile_data_agg
.withColumn("dma_name", 
	concat(col("lac"), lit("_"), col("ci_sac"), lit("_1700")))
)

df_bayes = (mobile_data_agg
.join(fv_indoor, "dma_name", how = 'left')
.drop("dma_name", "lac", "ci_sac")
)

# Verify whether observations number is coherent :
# df_bayes.count()
# fv_indoor.groupBy("dma_file").count().selectExpr("avg(count)").show(2)



# Bayesian allocation of events -----------------------------------------------

# Compute bayesian posterior for each individual event
# Uniform prior equal to one
df_bayes2 = df_bayes.withColumn('posterior_events', 
	col('n_calls')*col('likelihood'))

# Sum probabilities by user x tile (because a given user can be located by 
# multiple cells which cover a same grid tile)
# NB : we also group by x_tile and y_tile to keep them for final aggregation
df_bayes3 = (df_bayes2
.groupBy('imsi', 'x_tile', 'y_tile')
.sum('posterior_events')
.withColumnRenamed('sum(posterior_events)', 'posterior_indiv'))

# Normalize posterior so that it sums to one over the grid for each user
df_bayes4 = (df_bayes3
.groupBy('imsi')
.agg(sum("posterior_indiv").alias("sum_p")))

df_bayes5 = df_bayes3.join(df_bayes4, "imsi")

df_bayes6 = (df_bayes5
.withColumn("posterior", col("posterior_indiv") / col("sum_p"))
.drop("posterior_indiv", "sum_p"))

# Verify that probabilities sum to one per user : 
# users_to_keep.count()
# must be equal to :
# (df_bayes6
# .groupBy("imsi")
# .agg(sum("posterior").alias("p"))
# .select(sum("p"))
# .show(1))




# Count population at tile level ----------------------------------------------

pop_counts = (df_bayes6
.groupBy("x_tile", "y_tile")
.agg(sum("posterior").alias("n"))
.withColumn("n", round(col("n"), 0)))

(pop_counts.write
.option("header", False)
.csv(hue_perso + "05_03_19_pop_counts_alltechnos_ileetvilaine_oneday/snappy"))

# Verification of counts :
# schema_count = StructType([
# 	    StructField("x", DoubleType(), True),
# 	    StructField("y", DoubleType(), True),
# 	    StructField("n", DoubleType(), True)]
# 	    )

# count = (spark.read
# .option("header", False)
# .schema(schema_count)
# .csv(hue_perso + "05_02_19_pop_counts_4g_ileetvilaine_oneday/snappy/*"))

# count.select(sum("n")).show()
#
# Results :
# 350000 people with only 4G (coherent, as pop of Ille et Vilaine is about
# 1 million)