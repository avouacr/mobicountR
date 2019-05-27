# ----------------------------------------------------------
# PHD test with quadtree grid for Marne (51)
# Technologies : 4G, 3G, 2G
# Time range : one week, 21PM-7AM
# Prior = uniform
# ----------------------------------------------------------
# 05/24/19 (mm-dd-yy)

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
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as psql_f
import pyspark.sql.types as psql_t
import pyspark.sql.window as psql_wdw

# # Config pour pyspark
# import pyspark
# from pyspark.conf import SparkConf
# conf = pyspark.SparkConf().setAll([
# ('spark.executor.memory', '8g'),
# ('spark.executor.cores', '6'),
# ('spark.num.executors', '80'),
# ('spark.dynamicAllocation.enabled', 'true'),
# ('spark.shuffle.service.enabled', 'true'),
# ])
# spark = SparkSession.builder.config(conf=conf).appName("05_24_19").getOrCreate()
spark = SparkSession.builder.appName("05_24_19").getOrCreate()

sc = spark.sparkContext
sql_ctxt = SQLContext(sc)

# IMPORT COMMON FUNCTIONS STORED IN A PYTHON FILE
program_path = 'hdfs:///User/zues5490/WORK/romain/inputs/cancan_common_functions.py'
sc.addFile(program_path)
from cancan_common_functions import *


hue_perso = "/User/zues5490/WORK/romain/"
dir_out_hue = hue_perso + "05_24_19_PHD_marne_1week/"



# Preprocessing of CANCAN data ------------------------------------------------
# Count records per user x cell per day, and store them as parquet

# # First and last day of the timespan should be passed as arguments
# # when submitting spark script
# month = "03"
# first_day = int(sys.argv[1])
# last_day = int(sys.argv[2])
# days_range = [str(i) for i in range(first_day, last_day + 1)]
 
# # Create intermediary tables, one per day
# for day in days_range :
	
# 	print("day " + day + " month " + month)

# 	if len(day) == 1 :
# 		day = "0" + day

# 	cancan = import_cancan(
# 	sc = sc, spark = spark, sql_ctxt = sql_ctxt,
# 	psql = psql, psql_f = psql_f,
# 	psql_t = psql_t, functools = functools,
# 	month = month, 
# 	day = day,
# 	timerange = (21,7),
# 	technos = ["2G", "3G", "4G"],
# 	test = False
# 	)

# 	(cancan
# 	.groupBy("imsi", "lac", "ci_sac")
# 	.count().withColumnRenamed("count", "n_events")
# 	.write.parquet(dir_out_hue + "tables_inter/" + month + "_" + day)
# 	)

# Import and join daily counts

daily_agg = spark.read.parquet(dir_out_hue + "tables_inter/*")




# Preprocessing of FV tables ---------------------------------------------------

# Import FV coverage maps after quadtree (indoor)
fv_cov_maps_quad = (spark.read
.option("header", True)
.option("inferSchema", True)
.csv(hue_perso + "inputs/indoor_quad_probas_51.csv")
.withColumnRenamed("proba", "likelihood")
)

# Import FV topology register
fv_topo = (spark.read
.option("delimiter", ";")
.option("header", True)
.option("inferSchema", True)
.csv(hue_perso + "inputs/Topologie_CSG_2019_02_18.csv")
)

# Preprocess data
fv_topo = (fv_topo
.select("LAC", "CI/SAC")
.withColumnRenamed("LAC", "lac")
.withColumnRenamed("CI/SAC", "ci_sac")
.withColumn("dma_name", 
	psql_f.concat(
		psql_f.col("lac"), psql_f.lit("_"), 
		psql_f.col("ci_sac"), psql_f.lit("_1700"))
	)
)

# Join tables
fv_complete = fv_cov_maps_quad.join(fv_topo, "dma_name")




# Join daily counts with FV complete table ------------------------------------

df_bayes = daily_agg.join(psql_f.broadcast(fv_complete), ["lac", "ci_sac"])

# Verify whether observations number is coherent :
# df_bayes.count()
# fv_indoor.groupBy("dma_file").count().selectExpr("avg(count)").show(2)



# Bayesian allocation of events -----------------------------------------------

# Compute bayesian posterior for each individual event
# Uniform prior equal to one
df_bayes2 = df_bayes.withColumn('posterior_events', 
	psql_f.col('n_events')*psql_f.col('likelihood'))

# Sum probabilities by user x tile (because a given user can be located by 
# multiple cells which cover a same grid tile)
# NB : we also group by x_tile and y_tile to keep them for final aggregation
df_bayes3 = (df_bayes2
.groupBy('imsi', 'x_quad', 'y_quad')
.sum('posterior_events')
.withColumnRenamed('sum(posterior_events)', 'posterior_indiv'))

# Normalize posterior so that it sums to one over the grid for each user
df_bayes4 = (df_bayes3
.groupBy('imsi')
.agg(psql_f.sum("posterior_indiv").alias("sum_p"))
)

df_bayes5 = df_bayes3.join(df_bayes4, "imsi")

df_bayes6 = (df_bayes5
.withColumn("posterior", psql_f.col("posterior_indiv") / psql_f.col("sum_p"))
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
.groupBy("x_quad", "y_quad")
.agg(psql_f.sum("posterior").alias("n"))
.withColumn("n", psql_f.round(psql_f.col("n"), 0)))

(pop_counts.write
.option("header", False)
.csv(dir_out_hue + "out_snappy"))

# Verification of counts :
# schema_count = StructType([
# 	    StructField("x", DoubleType(), True),
# 	    StructField("y", DoubleType(), True),
# 	    StructField("n", DoubleType(), True)]
# 	    )

# count = (spark.read
# .option("header", False)
# .schema(schema_count)
# .csv(dir_out_hue + "out_snappy/*"))

# count.select(sum("n")).show()
# #
