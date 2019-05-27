# -------------------------------------------------------------------------------
# Descriptive statistics of CANCAN data (ideas from Marco Fiore and Lino's paper)
# -------------------------------------------------------------------------------
# 05/21/19 (mm-dd-yy)
# Period for statistics computation : 25 to 31 march 2019

# ============================
# Parametrage: Spark 2.1


# MODULES

from pyspark.sql import SparkSession, SQLContext
import sys
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
# spark = SparkSession.builder.config(conf=conf).appName("05_09_19").getOrCreate()

spark = SparkSession.builder.appName("05_09_19").getOrCreate()
sc = spark.sparkContext
sql_ctxt = SQLContext(sc)

# IMPORT COMMON FUNCTIONS STORED IN A PYTHON FILE
program_path = 'hdfs:///User/zues5490/WORK/romain/inputs/cancan_common_functions.py'
sc.addFile(program_path)
from cancan_common_functions import *

dir_out_hue = "/User/zues5490/WORK/romain/05_21_19_stats_desc_marco/"



# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 12G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py 25 31
# ------------------------------------------------------------------------------------------------------------

# Computation of daily intermediate tables ------------------------------------

# # First and last day of the timespan should be passed as arguments
# # when submitting spark script
# first_day = int(sys.argv[1])
# last_day = int(sys.argv[2]) + 1
# # Dates are formatted as a list of tuple
# # For timerange spanning several months, just merge a list of tuple for each month
# dates = [("03", i) for i in range(first_day, last_day)]

# statistics  = [
# "records_per_techno", 
# "daily_cell_appear",
# "daily_records_per_user",
# "daily_locations_per_user",
# "hourly_records", 
# "hourly_unique_users"]

# # Indices of statistics to compute (starting from 0) should be passed as arguments
# # when submitting spark script
# # statistics = all_statistics[int(sys.argv[3]):(int(sys.argv[4])+1)]

# # Looping over provided dates
# for i in dates :

# 	month = i[0]
# 	day = str(i[1])
	
# 	if len(day) == 1 :
# 		day = "0" + day

# 	cancan = import_cancan(
# 	sc = sc, spark = spark, sql_ctxt = sql_ctxt,
# 	psql = psql, psql_f = psql_f,
# 	psql_t = psql_t, functools = functools,
# 	month = month, 
# 	day = day,
# 	timerange = None,
# 	technos = ["2G", "3G", "4G"],
# 	test = False
# 	)

# 	# Make cancan df persist on memory & disk
# 	# cancan.cache() 
# 	# Not useful finally : fails or takes even longer than without	

# 	print("CANCAN data imported")

# 	if "records_per_techno" in statistics :
# 		# Count records per technology
# 		(cancan
# 		.groupBy("techno")
# 		.count()
# 		.write.parquet(dir_out_hue + "tables_inter/records_per_techno/" + month + "_" + day)
# 		)

# 	if "daily_cell_appear" in statistics :
# 		# Lists cells that appear at least once
# 		(cancan
# 		.groupBy("lac", "ci_sac")
# 		.count()
# 		.select("lac", "ci_sac")
# 		.withColumn("day", psql_f.lit(day).cast("integer"))
# 		.withColumn("month", psql_f.lit(month).cast("integer"))
# 		.write.parquet(dir_out_hue + "tables_inter/daily_cell_appear/" + month + "_" + day)
# 		)
			
# 	if "daily_records_per_user" in statistics :
# 		# Count number of records per user
# 		(cancan
# 		.groupBy("imsi")
# 		.count()
# 		.withColumn("day", psql_f.lit(day).cast("integer"))
# 		.withColumn("month", psql_f.lit(month).cast("integer"))
# 		.write.parquet(dir_out_hue + "tables_inter/daily_records_per_user/" + month + "_" + day)
# 		)

# 	if "daily_locations_per_user" in statistics :
# 		# Count number of locations per user
# 		(cancan
# 		.groupBy("imsi")
# 		.agg(psql_f.countDistinct("lac", "ci_sac"))
# 		.withColumnRenamed("count(DISTINCT lac, ci_sac)", "count")
# 		.withColumn("day", psql_f.lit(day).cast("integer"))
# 		.withColumn("month", psql_f.lit(month).cast("integer"))
# 		.write.parquet(dir_out_hue + "tables_inter/daily_locations_per_user/" + month + "_" + day)
# 		)

# 	if "hourly_records" in statistics :
# 		# Count number of records per hour
# 		(cancan
# 		.groupBy("hour")
# 		.count()
# 		.withColumn("day", psql_f.lit(day).cast("integer"))
# 		.withColumn("month", psql_f.lit(month).cast("integer"))
# 		.write.parquet(dir_out_hue + "tables_inter/hourly_records/" + month + "_" + day)
# 		)

# 	if "hourly_unique_users" in statistics :
# 		# Count number of unique users per hour
# 		(cancan
# 		.groupBy("hour")
# 		.agg(psql_f.countDistinct("imsi"))
# 		.withColumnRenamed("count(DISTINCT imsi)", "count")
# 		.withColumn("day", psql_f.lit(day).cast("integer"))
# 		.withColumn("month", psql_f.lit(month).cast("integer"))
# 		.write.parquet(dir_out_hue + "tables_inter/hourly_unique_users/" + month + "_" + day)
# 		)

# 	print("Statistics for day " + day + " month " + month + " done")



# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 12G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=1000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=1000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py 25 31
# ------------------------------------------------------------------------------------------------------------

# Aggregation of daily statistics and export to csv ---------------------------

statistics  = [
"records_per_techno", 
"daily_cell_appear",
"daily_records_per_user",
"daily_locations_per_user",
"hourly_records", 
"hourly_unique_users"]

for stat in statistics :

	if stat == "daily_records_per_user" :

		# Import counts of daily events per user
		daily_records = (spark
		.read.parquet(dir_out_hue + "tables_inter/" + stat + "/*")
		.where(psql_f.col("imsi").substr(1, 5) == "20801")
		)

		# Count number of distinct users over the whole week
		n_users_week = (daily_records
		.select("imsi")
		.distinct()
		.count()
		)
		print("Number of distinct IMSI over the week : " + str(n_users_week))
		# 23,979,625 distinct IMSI over one week

		# Count the number of day each user appears over the week, and export
		# the distribution
		n_days_appear = (daily_records
		.groupBy("imsi")
		.count()
		.select("count")
		.write.format("csv")
		.option("header", False)
		.save(dir_out_hue + "tables_inter/n_days_user_appear_csv")
		)

		# Compute average number of daily events per user over the week
		avg_daily_records = (daily_records
		.groupBy("imsi")
		.mean("count")
		.select("avg(count)")
		.write.format("csv")
		.option("header", False)
		.save(dir_out_hue + "tables_inter/avg_n_daily_events_user_csv")
		)

	elif stat == "daily_locations_per_user" :
		daily_locations = (spark
		.read.parquet(dir_out_hue + "tables_inter/" + stat + "/*")
		.where(psql_f.col("imsi").substr(1, 5) == "20801")
		)

		avg_daily_locations = (daily_locations
		.groupBy("imsi")
		.mean("count")
		.select("avg(count)")
		.write.format("csv")
		.option("header", False)
		.save(dir_out_hue + "tables_inter/avg_n_daily_locations_user_csv")
		)

	else :
		(spark.
		read.parquet(dir_out_hue + "tables_inter/" + stat + "/*")
		.repartition(10)
		.write.format("csv")
		.option("header", False)
		.save(dir_out_hue + "tables_inter/" + stat + "_agg_csv")
		)