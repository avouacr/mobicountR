import sys
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pyspark.sql.window as psw
from pyspark.sql.functions import isnan, when, count, col, expr, date_format, split, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

# ============================
# Parametrage: Spark 2.1

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Stats desc nuit").getOrCreate()
sc = spark.sparkContext

program_path = 'hdfs:///User/vblx1269/WORK/lino/Functions/CommonFunctions.py'
sc.addFile(program_path)
from CommonFunctions import *

# ============================
# IMPORT FUNCTION FILE
# ============================

night_time = (19,9)

data_cdr = readLocationCDR(spark = spark, pst = pst,
		psf = psf, month = "09",
		CDRpath = "/Data/O19593/SECURE/RAW/CSV/CRA_OFR/2007")

if night_time is not None:
	data_cdr = (data_cdr.
		filter((psf.col("hour").between(max(night_time),24)) 
				| (psf.col("hour").between(0,min(night_time)))))

# Summary stats:
# ---------------------

daily_calls = (data_cdr
	.groupBy('CodeId','day')
	.agg(psf.count(psf.lit(1)).alias("ncalls"))
	)

daily_calls_user = (daily_calls
	.groupBy('CodeId')
	.agg(psf.avg('ncalls').alias('mean_daily_events'),
		psf.count(psf.lit(1)).alias('number_distinct_days')
		)
	)

if night_time is None:
	path_out = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/11_04_19_Stats_Desc_Sept07/snappy"
else :
	path_out = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/11_04_19_Stats_Desc_Sept07_19h_09h/snappy"

daily_calls_user.write.csv(path_out, header = False)