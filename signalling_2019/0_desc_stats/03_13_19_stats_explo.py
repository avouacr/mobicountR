# ------------------------------------------------------
#              Exploration of CANCAN data
# ------------------------------------------------------
# 13/03/19

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
format_number, lit, sum)
from pyspark.sql.types import (StructType, StructField, IntegerType, 
StringType, TimestampType, DoubleType)
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("Exploration CANCAN").getOrCreate()
sc = spark.sparkContext


path_save = "/User/zues5490/WORK/13_03_19_stats_explo_cancan"

# 4G --------------------------------------------------------------------------
 
df_4g = (spark.read
.parquet('/Data/P17100/SECURE/RAW/TXT/S1C/2019/03/11/*'))
# For test :
# df_4g = (spark.read
# .parquet('/Data/P17100/SECURE/RAW/TXT/S1C/2019/03/11/16534529428886368'))

df_4g.createOrReplaceTempView("sql_4g")
nrows = df_4g.count()

# Descriptive statistics
des_stats = df_4g.select([c for c in df_4g.columns]).describe()

# Count pct of Null values in each column
null_counts = df_4g.agg(*[format_number(count(when(isnull(c), c)) / float(nrows), 2)
	.alias(c) for c in df_4g.columns])
null_counts = (null_counts.withColumn("summary", lit("pct_null"))
.select(["summary"] + [c for c in df_4g.columns]))

# Count pct of NaN values in each column
# nan_counts = df_4g.agg(*[format_number(count(when(isnan(c), c)) / float(nrows), 2)
# 	.alias(c) for c in df_4g.columns])
# nan_counts = (nan_counts.withColumn("summary", lit("pct_nan"))
# .select(["summary"] + [c for c in df_4g.columns]))

# Vertical joins of tables
des_stats = des_stats.union(null_counts)
des_stats.write.format("csv").option("header", "true").save(path_save + "/4G" + "/des_stats")

# Shares of "type" variable modalities
types_shares = spark.sql("SELECT type, count(*) as n FROM sql_4g GROUP BY type ORDER BY n DESC")
types_shares = types_shares.withColumn("share", format_number(types_shares.n 
	/ float(nrows), 3))
types_shares.write.format("csv").option("header", "true").save(path_save + "/4G" + "/types_shares")

# Shares of "cdr_result" variable modalities
cdr_result_shares = spark.sql("SELECT cdr_result, count(*) as n FROM sql_4g GROUP BY cdr_result ORDER BY n DESC")
cdr_result_shares = cdr_result_shares.withColumn("share", format_number(cdr_result_shares.n 
	/ float(nrows), 3))
cdr_result_shares.write.format("csv").option("header", "true").save(path_save + "/4G" + "/cdr_result_shares")

# Cross-table : "type" by "cdr_result"
# cross1 = df_4g.stat.crosstab("type", "cdr_result")
# cross1 = (cross1.withColumn("0", format_number(col("0") / nrows, 3))
# .withColumn("1", format_number(col("1") / nrows, 3))
# .withColumn("2", format_number(col("2") / nrows, 3)))
# cdr_result_shares.write.format("csv").option("header", "true").save(path_save + "/4G" + "/cross_type_cdrresult")

# Cross-table : "type" by dummy indicating if cell_id is missing or not
df_4g = (df_4g.withColumn("missing_cell_id", when(isnull("cell_id"), 1))
.fillna(0, subset = ['missing_cell_id']))
cross2 = df_4g.stat.crosstab("type", "missing_cell_id")
cross2 = (cross2.withColumn("0", format_number(col("0") / nrows, 3))
.withColumn("1", format_number(col("1") / nrows, 3)))
cross2.write.format("csv").option("header", "true").save(path_save + "/4G" + "/cross_type_missingcellid")





