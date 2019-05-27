# ----------------------------------------------------------
# Hourly aggregations at antenna level over a week
# (for Francois's intern)
# ----------------------------------------------------------
# 05/09/19 (mm-dd-yy)

# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 12G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py
# ------------------------------------------------------------------------------------------------------------

# ============================
# Parametrage: Spark 2.1


# MODULES
# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession
# conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.num.executors', '10'), ('spark.executor.cores', '3'), ('spark.default.parallelism', '100'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.sql.shuffle.partitions', '100'), ('spark.executorEnv.PYTHONHASHSEED', '321'), ('spark.sql.broadcastTimeout', '36000')])
from pyspark.sql import SparkSession, SQLContext
import sys
import pyspark.sql.functions as psql_f
import pyspark.sql.types as psql_t
import pyspark.sql.window as psql_wdw

spark = SparkSession.builder.appName("05_09_19").getOrCreate()
sc = spark.sparkContext
sql_ctxt = SQLContext(sc)

# IMPORT COMMON FUNCTIONS STORED IN A PYTHON FILE
program_path = 'hdfs:///User/zues5490/WORK/romain/inputs/cancan_common_functions.py'
sc.addFile(program_path)
from cancan_common_functions import *



def hourly_pop_cell(sc, spark, sql_ctxt,
psql, psql_f, psql_t, functools,
method, month, days_range
) :

	hue_perso = "/User/zues5490/WORK/romain/"

	for i in days_range :

		print("day " + i)

		cancan = import_cancan(
		sc = sc, spark = spark, sql_ctxt = sql_ctxt,
		psql = psql, psql_f = psql_f,
		psql_t = psql_t, functools = functools,
		month = month, 
		day = i,
		timerange = None,
		technos = ["2G", "3G", "4G"],
		test = False
		)

		print("CANCAN data imported")

		hourly_indiv_x_cell = (cancan
		.groupBy("imsi", "hour", "lac", "ci_sac")
		.count())

		print("Computing hourly counts by cell using " + method + " method")

		if method == "cell_max" :

			w = (psql_wdw
			.Window().partitionBy("imsi", "hour")
			.orderBy(psql_f.col("count").desc()))

			hourly_counts = (hourly_indiv_x_cell
			.withColumn("rank", psql_f.row_number().over(w))
			.where(psql_f.col("rank") == 1)
			.select("imsi", "hour", "lac", "ci_sac")
			.groupBy("hour", "lac", "ci_sac")
			.count()
			)

		elif method == "repartition" :

			hourly_counts = (hourly_indiv_x_cell
			.groupBy("imsi", "hour")
			.sum("count").withColumnRenamed("sum(count)", "sum")
			.join(hourly_indiv_x_cell, ["imsi", "hour"])
			.withColumn("count_norm", psql_f.col("count") / psql_f.col("sum"))
			.groupBy("hour", "lac", "ci_sac")
			.sum("count_norm")
			.withColumn("count", 
				psql_f.round(psql_f.col("sum(count_norm)"), 0).cast("integer"))
			.drop("sum(count_norm)")
			)

		hourly_counts = hourly_counts.withColumn("day", psql_f.lit(i).cast("integer"))

		print("Exporting results")

		(hourly_counts.write.format("csv")
		.option("header", False)
		.save(hue_perso + "05_09_19_aggregations_Francois/" + method + "/" + i))
"""
	Compute hourly user counts per cell, for each day of a given range

	Parameters
	----------
	sc, spark, sql_ctxt, psql, psql_f, psql_t, functools: 
		sparkContext, SparkSession, SQLContext, pyspark.sql.functions, 
		pyspark.sql.types, functools
		Modules passed as dependencies
	month: str
		Month of data to import, format "MM"
	days_range: list of str
		List of days to perform counts for, format "DD" (e.g. ["11", "12"])
		(if None, data of whole month is imported)
	method: str
		Method to use to perform aggregation. 2 possibilities :
		"cell_max" : each user is counted at the antenna he connects to the most in the hour
		"repartition" : each user is "distributed" over the antennas he connects to in the
		hour, weighted by the number of connections for each
	
	return: SparkDataFrame	
	"""

first_day = int(sys.argv[2])
last_day = int(sys.argv[3])
days_range = [str(i) for i in range(first_day, last_day + 1)]

hourly_pop_cell(
sc = sc, spark = spark, sql_ctxt = sql_ctxt,
psql = psql, psql_f = psql_f,
psql_t = psql_t, functools = functools,
method = sys.argv[1],
month = "04", days_range = days_range, 
)



# sc = sc
# spark = spark 
# sql_ctxt = sqlContext
# psql = psql 
# psql_f = psql_f
# psql_t = psql_t
# functools = functools
# month = "04"
# day = "20"



# # Fictive example to verify that both methods produce coherent results

# from pyspark.sql import Row
# from pyspark.sql.types import StructField, StructType, StringType, LongType

# myManualSchema = StructType([
# StructField("imsi", psql_t.StringType(), True),
# StructField("day", psql_t.IntegerType(), True),
# StructField("hour", psql_t.IntegerType(), True),
# StructField("lac_ci", psql_t.StringType(), False),
# StructField("count", psql_t.IntegerType(), False)
# ])

# hourly_indiv_x_cell = spark.createDataFrame(
# 	[Row("user1", 20, 12, "lac1", 3),
# 		Row("user1", 20, 12, "lac2", 1),
# 		Row("user1", 20, 15, "lac1", 2),
# 		Row("user1", 20, 15, "lac2", 5),
# 		Row("user2", 20, 12, "lac1", 7),
# 		Row("user2", 20, 12, "lac2", 4),
# 		Row("user2", 20, 15, "lac1", 2),
# 		Row("user2", 20, 15, "lac2", 9)], myManualSchema)

# # Windows Method

# w = (psql_wdw
# .Window()
# .partitionBy("imsi", "day", "hour")
# .orderBy(psql_f.col("count").desc()))

# hourly_counts_max = (hourly_indiv_x_cell
# .withColumn("rank", psql_f.row_number().over(w))
# .where(psql_f.col("rank") == 1)
# .select("imsi", "day", "hour", "lac_ci")
# .groupBy("day", "hour", "lac_ci")
# .count()
# )

# hourly_counts_max.show()

# # Repartition method

# hourly_counts_repart = (hourly_indiv_x_cell
# .groupBy("imsi", "day", "hour")
# .sum("count").withColumnRenamed("sum(count)", "sum")
# .join(hourly_indiv_x_cell, ["imsi", "day", "hour"])
# .withColumn("count_norm", psql_f.col("count") / psql_f.col("sum"))
# .groupBy("day", "hour", "lac_ci")
# .sum("count_norm").withColumnRenamed("sum(count_norm)", "count")
# )

# hourly_counts_repart.show()




# Verification of output csv

# from pyspark.sql.functions import countDistinct

# hue_perso = "/User/zues5490/WORK/romain/"

# schema_verif = psql_t.StructType([
# psql_t.StructField("day", psql_t.IntegerType(), True),
# psql_t.StructField("hour", psql_t.IntegerType(), True),
# psql_t.StructField("lac_ci", psql_t.StringType(), False),
# psql_t.StructField("count", psql_t.IntegerType(), False)
# ])

# test = (spark.read
# .schema(schema_verif)
# .csv(hue_perso + "05_09_19_aggregations_Francois/cell_max/hourly_counts_cell_max_11_17_april_19.csv")
# )

# test.groupBy("day").count().show()