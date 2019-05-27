# ------------------------------------------------------
#              PROBABILISTIC HOME DETECTION
# ------------------------------------------------------
# 14/01/2019

# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 16G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py
# ------------------------------------------------------------------------------------------------------------
# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession
# conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '14g'), ('spark.num.executors', '80'), ('spark.executor.cores', '6'), ('spark.default.parallelism', '8000'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.sql.shuffle.partitions', '8000'), ('spark.executorEnv.PYTHONHASHSEED', '321'), ('spark.sql.broadcastTimeout', '36000')])
# spark = SparkSession.builder.config(conf=conf).appName("Home Detection").getOrCreate()
# sc = spark.sparkContext
# ------------------------------------------------------------------------------------------------------------


# ============================
# Parametrage: Spark 2.1


# MODULES
import sys
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pyspark.sql.window as psw
from pyspark.sql.functions import isnan, when, count, col, expr, date_format, split, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Home Detection").getOrCreate()
sc = spark.sparkContext


# IMPORT FUNCTIONS STORED IN A PYTHON FILE
program_path = 'hdfs:///User/vblx1269/WORK/lino/Functions/CommonFunctions.py'
sc.addFile(program_path)
from CommonFunctions import *


# =====================================
# 	PROGRAM


# Arguments pour tests
# month      = '09'
# path       =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior"
# gridPath   =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior"
# prior = True
# night_time = (19,9)
# reduceData = False

def ProbHomeDetection(month = '09',
	path = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/31_01_19_NappesProb_priors_normalized_France",
	gridPath = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/31_01_19_NappesProb_priors_normalized_France/prior_rfl_bdtopo_france.csv",
	prior_type = "rfl",
	aggregate = True,
	night_time = (19,9)) :
	"""
	Perform home detection based on CDR registered communications. Write that as csv file

	Keyword arguments:
		month  -- Month to consider. If None, all available months are used (default '09')
		path   -- Path where data should be written
		gridPath -- Path for the grid-voronoi intersections file with probabilities (including prior)
		prior_type -- What prior should be used ? (possible : "bd_topo", "rfl", "uniform")
		aggregate -- Should the function return counts of caller for each tile ?
		night_time -- Hour used to define night. By default, equal to 19h-9h.
	"""


	# =========================================
	# 			PART I: READ DATA
	# =========================================

	# DEFINE SCHEMA
	# ---------------------------

	schema = pst.StructType([
	    pst.StructField("CCR_ID", pst.IntegerType(), True),
	    pst.StructField("DT_HOROD", pst.TimestampType(), True),
	    pst.StructField("NDE_NU_CONCERNE", pst.DoubleType(), True),
	    pst.StructField("NDE_NU_CORRESP", pst.DoubleType(), True),
	    pst.StructField("NB_UNITE", pst.StringType(), True),
	    pst.StructField("NB_UT", pst.StringType(), True),
	    pst.StructField("CO_TYPUNIT", pst.StringType(), True),
	    pst.StructField("CODETYS", pst.IntegerType(), True),
	    pst.StructField("CODESUP", pst.IntegerType(), True),
	    pst.StructField("IN_ACTDOS", pst.StringType(), True),
	    pst.StructField("CSI_ID", pst.StringType(), True),
	    pst.StructField("NU_IMEI", pst.StringType(), True),
	    pst.StructField("NU_SVIMEI", pst.StringType(), True),
	    pst.StructField("CO_ZLOC", pst.IntegerType(), True),
	    pst.StructField("CO_TYEMET", pst.StringType(), True),
	    pst.StructField("LB_EMET", pst.StringType(), True),
	    pst.StructField("CO_IDCELL", pst.IntegerType(), True)]
	    )


	# IMPORT DATA
	# ---------------------------

	if (month is None):
		data_cdr1 = (spark.read.format('csv')
		.option("delimiter",";").option("header","false")
		.option("timestampFormat", "dd/MM/yyyy hh:mm:ss")
		.schema(schema)
		.load("/Data/O19593/SECURE/RAW/CSV/CRA_OFR/2007/*")
		)
	else:
		data_cdr1 = (spark.read.format('csv')
		.option("delimiter",";").option("header","false")
		.option("timestampFormat", "dd/MM/yyyy hh:mm:ss")
		.schema(schema)
		.load("/Data/O19593/SECURE/RAW/CSV/CRA_OFR/2007/" + month + "/*")
		)

	# FOR TESTS ONLY
	# data_cdr1 = data_cdr1.sample(False, 0.1)


	# KEEP CALLS AND MESSAGES
	# ---------------------------

	# On choisit les événements: appels et sms
	data_cdr2 = (data_cdr1
		.filter(psf.col("CCR_ID").isin([0,1])) #0: Sortant ou 1: entrant
		.filter(psf.col("CODESUP").isin([17,33,34])) # 17: appel et 33&34: sms
		.filter(psf.col("CODETYS") == 0) #0: appels classiques
		.filter(psf.col("CCR_ID").isNotNull()) 
		.filter(psf.col("NDE_NU_CONCERNE").isNotNull())
		.filter(psf.col("CO_ZLOC").isNotNull())
		.filter(psf.col("CO_ZLOC") != 0)
		.filter(psf.col("CO_IDCELL") != 0)
		.filter(psf.col("CSI_ID").startswith("208") != 0)
		)


	# KEEP NIGHTTIME EVENTS IF night_time=False
	# ----------------------------------------------

	if night_time is not None:
		data_cdr2 = (data_cdr2.
			filter((psf.hour("DT_HOROD").between(max(night_time),24)) 
				| (psf.hour("DT_HOROD").between(0,min(night_time))))
			)

	# Ensure we have consistent CodeId
	data_cdr3 = data_cdr2.rdd.filter(lambda l: len(str(l['CSI_ID'])) == 15)

	# Repartition data
	data_cdr3 = data_cdr3.repartition(sc.defaultParallelism)


	# =========================================
	# 			PART II: ARRANGE DATA
	# =========================================

	# CREATE CODE FOR EVENT TYPE
	# ------------------------------------

	def defineEvent(CCR_ID,CODESUP):
		if (CCR_ID == 1):
			return 'VI' if CODESUP==17 else 'SI1'
		else:
			return 'VO' if CODESUP==17 else 'SO1'


	# Define event
	data_cdr4 = data_cdr3.map(lambda l: flat_tuple((tuple(l), defineEvent(l['CCR_ID'],l['CODESUP']))))

	# data_cdr4.take(2)
	# [(1, datetime.datetime(2007, 9, 1, 0, 8, 45), 33670579670.0, 33689004324.0, '1', '0', 'M', 0, 33, '1', '208013801444747', '35619601517228', '0', 20484, '0', '3304200', 5978, 'SI1'), (1, date
	# time.datetime(2007, 9, 1, 0, 21, 37), 33670579670.0, 33689004324.0, '1', '0', 'M', 0, 33, '1', '208013801444747', '35619601517228', '0', 20484, '0', '3304200', 33817, 'SI1')]


	# ARRANGE DATA
	# ------------------------------------

	schema = pst.StructType(data_cdr2.schema.fields +  [pst.StructField("event", pst.StringType(), True)])


	data_cdr5 = (spark.createDataFrame(data_cdr4,schema)
		.withColumn("month",psf.month("DT_HOROD"))
		.withColumn("day",psf.dayofmonth("DT_HOROD"))
		.withColumn('day_full', psf.dayofyear("DT_HOROD"))
		.withColumn("hour",psf.hour("DT_HOROD"))
		.withColumnRenamed("CO_IDCELL","area_id")
		.withColumnRenamed("CO_ZLOC","caller_tower")	
		.withColumnRenamed("NDE_NU_CONCERNE","caller")
		.withColumn("key", psf.concat(psf.col("area_id"), psf.lit(":"), psf.col("caller_tower")))	
		)



	# ==========================================================
	# 			PART III: JOIN WITH GEOGRAPHICAL LOCATION
	# ==========================================================
	print("Join with geographical coordinates of celltowers")


	# IMPORT NIDT DATA
	# -----------------------

	schema2 = pst.StructType([
	    pst.StructField("nidt", pst.StringType(), True),
	    pst.StructField("ci", pst.IntegerType(), True),
	    pst.StructField("lac", pst.IntegerType(), True),
	    pst.StructField("lat", pst.DoubleType(), True),
	    pst.StructField("lon", pst.DoubleType(), True),
	    pst.StructField("x", pst.IntegerType(), True),
	    pst.StructField("y", pst.IntegerType(), True)]
	    )



	# Import NIDT data
	nidtcilac1 = (spark.read.format('csv')
		.option("delimiter","\t").option("header","false")
		.schema(schema2)
		.load("/User/vblx1269/WORK/nidt_cilac/nidt_cilac.tsv")
		#.rdd.repartition(sc.defaultParallelism)
		)

	# Keep relevent information
	nidtcilac2 = nidtcilac1.select('nidt','ci','lac','x','y')


	# BROADCAST JOIN
	# -----------------------


	# Replicated join in PIG becomes broadcast in Spark
	nidtcilac3 = (nidtcilac2
		.withColumn("key", psf.concat(
			psf.concat_ws(':',
			col('ci').cast("string"),
			col('lac').cast("string"))))
		.select('key', 'nidt', 'x', 'y')
		)


	# nidtcilac3.show(2)
	# +-----------+----------+------+-------+
	# |        key|      nidt|     x|      y|
	# +-----------+----------+------+-------+
	# |53571:32768|00000001A1|634700|2552680|
	# |62542:32768|00000001A1|634700|2552680|
	# +-----------+----------+------+-------+
	# only showing top 2 rows

	nidtcilac5 = data_cdr5.join(psf.broadcast(nidtcilac3), ['key']) \
	.select('month','caller','nidt','x','y','day_full')

	# nidtcilac5.show(2)
	# +-----+---------------+----------+------+-------+--------+
	# |month|         caller|      nidt|     x|      y|day_full|
	# +-----+---------------+----------+------+-------+--------+
	# |    9|3.3684672704E10|00000191R1|550674|2507436|     244|
	# |    9|3.3672914127E10|00000016R1|556720|2530480|     244|
	# +-----+---------------+----------+------+-------+--------+
	# only showing top 2 rows





	# FOR TESTS ONLY
	# data_cdr5 = data_cdr5.sample(False, 0.1)

	# ARRANGE grid_id/voronoi INTERSECTIONS + probabilities (including prior)
	# ------------------------------------------------------------------------

	# Import data
	schema3 = pst.StructType([
	    pst.StructField("id", pst.StringType(), True),
	    pst.StructField("proba_inter", pst.DoubleType(), True),
	    pst.StructField("proba_build", pst.DoubleType(), True),
	    pst.StructField("proba_rfl", pst.DoubleType(), True)]
	    )
	grid1 = (spark.read.format('csv')
		.option("header","true")
		.schema(schema3)
		.load(gridPath)
		)
	# Incorporate prior if prior = True
	print("Using prior : " + prior_type)
	if prior_type == "rfl" :
		grid1 = grid1.select('id','proba_rfl').withColumnRenamed('proba_rfl','proba')
	elif prior_type == "bd_topo" :
		grid1 = grid1.select('id','proba_build').withColumnRenamed('proba_build','proba')
	elif prior_type == "uniform" :
		grid1 = grid1.select('id','proba_inter').withColumnRenamed('proba_inter','proba')

	# Keep only tiles with strictly positive probability
	grid1 = grid1.filter(col("proba") > 0)
	
	# Arrange datagrid
	grid2 = (grid1.withColumn('split', split(grid1['id'], '\:'))
	.withColumn('nidt', col('split')[0])
	.withColumn('grid_id', col('split')[1])
	.drop('split').select("nidt", "grid_id", "proba")
	.na.drop()
	)


	# JOIN AND TRANSFORM
	# ---------------------------

	nidtcilac5 = nidtcilac5.na.drop()

		# SUM BY NIDTxINDIV BEFORE MERGING WITH GRID PROBABILITIES
	nidtcilac6 = (nidtcilac5
		.groupBy('month','caller','nidt')
		.count()
		.withColumnRenamed('count','p(v)')
		)


	# nidtcilac6.show(2)
	# +-----+---------------+----------+----+                                         
	# |month|         caller|      nidt|p(v)|
	# +-----+---------------+----------+----+
	# |    9|3.3670735881E10|00000218M2|   8|
	# |    9|3.3675274756E10|00000380E1|   7|
	# +-----+---------------+----------+----+
	# only showing top 2 rows


	# nidtcilac6 = nidtcilac6.repartition(sc.defaultParallelism)


	# ==========================================================
	# 			PART IVa: PROBABILISTIC HOME DETECTION
	# ==========================================================
	# With that algorithm, p(v)=1 by assumption thus p(c)=sum(p(c|v))
	print("Estimating home for all phone users")


	# Join NIDT and grid
	# -----------------------------
	#
	nidtcilac7 = nidtcilac6.join(psf.broadcast(grid2),['nidt'],'left_outer')
	nidtcilac8 = nidtcilac7.withColumnRenamed('caller','CodeId')
	# nidtcilac7.show(2)
	# +----------+-----+---------------+----+--------------------+                    
	# |      nidt|month|         CodeId|p(v)|                grid|
	# +----------+-----+---------------+----+--------------------+
	# |00005707B1|    8|3.3675046434E10|   1|[[1148625,6.89375...|
	# |00000303R1|    8|3.3607552114E10|   6|[[4000896,7.18648...|
	# +----------+-----+---------------+----+--------------------+	
	#
	# FlatMap
	# ------------------------
	#
	# # Debug NaN
	# debug2 = nidtcilac8.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in nidtcilac8.columns])
	# debug2 = debug2.coalesce(1)
	# debug2.write.csv(path + "/debug2")
	#
	# nidtcilac
	# nidtcilac8.show(2)
	# +-----+--------------+----+-------+--------------------+
	# |month|        CodeId|p(v)|grid_id|              p(c|v)|
	# +-----+--------------+----+-------+--------------------+
	# |    9|3.360759668E10|   5|1067089|6.934666289234769E-4|
	# |    9|3.360759668E10|   5|1067090|0.018459342443495202|
	# +-----+--------------+----+-------+--------------------+
	# only showing top 2 rows			
	#
	#
	# Sum probas by month:id:grid
	# ----------------------------------
	#
	nidtcilac9 = nidtcilac8.withColumn('p(c)',psf.col('p(v)')*psf.col('proba'))
	nidtcilac9bis = nidtcilac9.groupBy('month','CodeId','grid_id').sum('p(c)')
	nidtcilac9bis = nidtcilac9bis.withColumnRenamed('sum(p(c))','p')
	
	
	# Renormalize probabilities per caller
	nidtcilac10 = nidtcilac9bis.groupBy('CodeId').agg(expr("sum(p)").alias("sum_p"))
	nidtcilac11 = nidtcilac9bis.join(nidtcilac10, 
		nidtcilac9bis["CodeId"] == nidtcilac10["CodeId"])
	nidtcilac11 = nidtcilac11.withColumn("p_norm", expr("p/sum_p"))
	#
	# If aggregate = TRUE, return counts at tile level
	# Else, return a table with all caller_id x grid probabilities (very large)
	if aggregate :
		# Probabilistic assignation of callers to the grid
		df_final = nidtcilac11.groupBy('grid_id').agg(expr("sum(p_norm)").alias("n"))
	else :
		df_final = nidtcilac11		

	print("Saving data")
	df_final.write.csv(path + "/prior_" + prior_type + "/" + str(month))
	print("End of function")






ProbHomeDetection(month = '09',
	path = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/31_01_19_NappesProb_priors_normalized_France",
	gridPath = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/31_01_19_NappesProb_priors_normalized_France/prior_rfl_bdtopo_france.csv",
	prior_type = sys.argv[1],
	aggregate = True,
	night_time = (19,9))