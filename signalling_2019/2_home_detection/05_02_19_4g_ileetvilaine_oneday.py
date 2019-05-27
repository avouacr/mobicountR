# ----------------------------------------------------------
# Population count test : 4G data, one day, Ile et Vilaine
# Prior = uniform
# ----------------------------------------------------------
# 05/02/19 (mm-dd-yy)

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
round, lit, sum, avg, desc, concat, month, dayofmonth, hour)
from pyspark.sql.types import (StructType, StructField, IntegerType, 
StringType, TimestampType, DoubleType)
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("Test appariement 4G").getOrCreate()
sc = spark.sparkContext

hue_perso = "/User/zues5490/WORK/romain/"
hue_cancan = "/Data/P17100/SECURE/RAW/TXT/"



# Preprocessing of 4G data ----------------------------------------------------
 
 # Import 4G data
df_4g = (spark.read
.parquet('/Data/P17100/SECURE/RAW/TXT/S1C/2019/03/11/*'))
# Sample for test :
# df_4g = (spark.read.parquet(hue_cancan + "S1C/2019/03/11/16534529428886368"))

# Subset to night timerange
night_time = (21,7)

df_4g = (df_4g
.withColumn("end_timestamp", col("end_date_utc").cast("timestamp"))
.withColumn("month", month("end_timestamp"))
.withColumn("hour", hour("end_timestamp"))
.filter((col("hour").between(max(night_time),24)) 
				| (col("hour").between(0,min(night_time))))
)		

# Subset to rows without error
df_4g = df_4g.where(col("cdr_result") == 0)

# Subset to rows without missing values for enodeb_id and cell_id
# (both necessay to compute complete CID)
# enodeb_id is almost never missing, but cell_id is in about 40% of events,
# mainly when type = 9 ("S1 Release")
df_4g = df_4g.where(col("enodeb_id").isNotNull() & col("cell_id").isNotNull())

# Creation of cid variable for merging : cid = 256*enodeb_id + cell_id
# Verification possible using : https://www.cellmapper.net/enbid?lang=fr
df_4g = df_4g.withColumn("cid", 256*col("enodeb_id") + col("cell_id"))

# Keep relevant columns
df_4g = df_4g.select("imsi", "cid")





# Preprocessing of FV topology register ---------------------------------------

# Import FV topology register
fv_topo = (spark.read
.option("delimiter", ";")
.option("header", True)
.option("inferSchema", True)
.csv(hue_perso + "Topologie_CSG_2018_08_28.csv"))

# Preprocess data
fv_topo = (fv_topo
.where(col("Techno") == "4G")
.where(col("LAC") != "0") # Filter out invalid LAC
.withColumn("cid", col("CI/SAC"))
# Create column giving FV coverage map .dma file name
.withColumn("dma_file", concat(col("LAC"), lit("_"), col("CI/SAC"), lit("_1700"))))

# Keep only cells inside a given spatial bounding box
# Were, we use "Ille et vilaine" (Rennes department) bounding box
fv_topo_sub = (fv_topo
.filter(col("X").between(255036, 349782))
.filter(col("Y").between(2301823, 2420788))
)

# Keep relevant columns
fv_topo_sub = fv_topo_sub.select("cid", "dma_file")



# Merging on cell_id (cid) ----------------------------------------------------

# First (inner) join : get list of users who crossed at least one cell located
# in the bouding box we used to subset FV topology register
users_to_keep = (df_4g
.join(fv_topo_sub, "cid")
.select("imsi")
.distinct())

# Second (inner) join : subset 4G data to all events made by users listed
# above (including events detected by cells not located in the bounding box)
df_4g_sub = df_4g.join(users_to_keep, "imsi")



# Aggregation of individual events --------------------------------------------

# Count monthly events by user x cell
df_4g_agg = (df_4g_sub
.groupBy("imsi", "cid")
.count()
.withColumnRenamed("count", "n_calls"))

# Join again with FV topology register to get .dma files names
# We use a left join so as to keep all events from the selected users
df_4g_agg = df_4g_agg.join(fv_topo_sub, "cid", how = 'left')



# Join with FV INDOOR coverage table ------------------------------------------

fv_indoor = spark.read.parquet(hue_perso + "fv_indoor_07_18_pqt")
fv_indoor = (fv_indoor
.withColumnRenamed("dma_name", "dma_file")
.withColumnRenamed("proba", "likelihood"))

df_bayes = df_4g_agg.join(fv_indoor, "dma_file", how = 'left')

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
.csv(hue_perso + "05_02_19_pop_counts_4g_ileetvilaine_oneday/snappy"))

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