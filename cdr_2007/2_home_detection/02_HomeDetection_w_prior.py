# ------------------------------------------------------
#           COMPUTE PEOPLE CONTACT FREQUENCY FOR
#  		  			  SSI COMPUTATIONS
# ------------------------------------------------------
# 2018-07-30

# ------------------------------------------------------------------------------------------------------------
# spark-submit --executor-memory 16G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000' script.py
# pyspark --executor-memory 16G --num-executors 80 --executor-cores 6 --queue HQ_OLPS --conf 'spark.default.parallelism=8000' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.shuffle.partitions=8000' --conf 'spark.executorEnv.PYTHONHASHSEED=321' --conf 'spark.sql.broadcastTimeout=36000'
# ------------------------------------------------------------------------------------------------------------
# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession
# conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '14g'), ('spark.num.executors', '80'), ('spark.executor.cores', '6'), ('spark.default.parallelism', '8000'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.sql.shuffle.partitions', '8000'), ('spark.executorEnv.PYTHONHASHSEED', '321')])
# spark = SparkSession.builder.config(conf=conf).appName("Home Detection").getOrCreate()
# sc = spark.sparkContext
# ------------------------------------------------------------------------------------------------------------


# ============================
# Parametrage: Spark 2.1


# MODULES
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pyspark.sql.window as psw
from pyspark.sql.functions import col
from pyspark.sql.functions import date_format
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
# method     = 'ma'
# path       =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior"
# gridPath   =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior"
# useGrid    = True
# prior = True
# night_time = (19,9)
# reduceData = True
# pathNIDT = '/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior'
# filenameNIDT = 'nidt_list_paris_uu.csv'

def homeDetection(month = '09',
	method = 'dd',
	path = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior",
	gridPath = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior",
	useGrid  = True,
	prior = True,
	night_time = None,
	reduceData = False,
	pathNIDT = '/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior',
	filenameNIDT = 'nidt_list.csv'):
	"""
	Perform home detection based on CDR registered communications. Write that as csv file

	Keyword arguments:
		month  -- Month to consider. If None, all available months are used (default '09')
		method -- Heuristics that should be used. Should be chosen among 'dd' (distinct 
	days) or 'ma' (maximum action) (default 'dd')
		path   -- Path where data should be written
		gridPath -- Path for the grid-voronoi intersections file
		useGrid -- Boolean indicating whether we should use grid (True) or voronoi (False)
		night_time -- Hour used to define night. By default, equal to None (no filter)
	as spatial unit 
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
		.load("/Data/O19593/SECURE/RAW/CSV/CRA_OFR/2007/"+month+"/*")
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

	if reduceData:
	# ------------ REDUCE DATA TO PEOPLE THAT CROSSED A COLLECTION OF ANTENNAS
		# IMPORT LIST OF INTERESTING NIDT
		df_restricted = (spark.read.format('csv')
				.option("header","true")
				.load(pathNIDT + "/" + filenameNIDT)
				.select('NIDT')
				.withColumnRenamed('NIDT','nidt')
				.withColumn('i', psf.lit("ok"))
				)
		# MERGE TO CDR TO KNOW WHO HAS BEEN MEASURED THERE
		nidtcilac5_2 = (nidtcilac5
			.join(psf.broadcast(df_restricted), ['nidt'], 'left_outer')
			.filter(psf.col('i').isNotNull()
			))
		list_indiv = nidtcilac5_2.select('caller').distinct().withColumn('i', psf.lit("ok"))
		# KEEP ONLY PEOPLE THAT ENCOUNTERED ONCE THOSE ANTENNAS
		nidtcilac5_3 = (nidtcilac5
			.join(psf.broadcast(list_indiv),['caller'],'left_outer')
			.filter(psf.col('i').isNotNull())
			.drop('i')
			)
		nidtcilac5 = nidtcilac5_3



	# FOR TESTS ONLY
	# data_cdr5 = data_cdr5.sample(False, 0.1)

	# ARRANGE grid_id/voronoi INTERSECTIONS
	# ------------------------------------------

	if useGrid:
		# Import data
		schema3 = pst.StructType([
		    pst.StructField("id", pst.StringType(), True),
		    pst.StructField("proba_inter", pst.DoubleType(), True),
		    pst.StructField("S_intersect", pst.DoubleType(), True),
		    pst.StructField("S_tot", pst.DoubleType(), True),
		    pst.StructField("vol_cell", pst.DoubleType(), True),
		    pst.StructField("prior_cell", pst.DoubleType(), True),
		    pst.StructField("proba_final", pst.DoubleType(), True)]
		    )
		grid1 = (spark.read.format('csv')
			.option("header","true")
			.schema(schema3)
			.load(gridPath + "/prob_grid_prior.csv")
			)
		schema3 = StructType([StructField("nidt", StringType(), True),
			StructField("grid_id", StringType(), True)] + grid1.schema.fields)
		# Arrange data
		grid2 = grid1.rdd.map(lambda l: flat_tuple((l[0].split(":"), tuple(l))))
		grid2 = spark.createDataFrame(grid2, schema3)
		# Incorporate prior if prior = True
		if prior:
			grid2 = grid2.select('nidt','grid_id','proba_final').withColumnRenamed('proba_final','proba')
		else:
			grid2 = grid2.select('nidt','grid_id','proba_inter').withColumnRenamed('proba_inter','proba')
		# Transform in (NIDT, (grid1,p1),...(gridj,pj))
		grid3 = (grid2.groupBy('nidt')
			.agg(psf.collect_list("grid_id"),
				psf.collect_list("proba"))
			)
		grid4 = grid3.rdd.map(lambda l: (l['nidt'],list(zip(l[1],l[2])))).toDF()
		grid4 = grid4.withColumnRenamed('_1','nidt').withColumnRenamed('_2','grid')


	# JOIN AND TRANSFORM
	# ---------------------------

	nidtcilac5 = nidtcilac5.na.drop()

	if (useGrid) & (method == 'dd'):
		# Broadcast join with grid
		nidtcilac5bis = nidtcilac5.join(psf.broadcast(grid4),['nidt'],'left_outer')
		nidtcilac5bis = (nidtcilac5bis
			.na.drop()
			.drop("nidt").withColumn("key",
					psf.concat_ws(":",
						psf.col("month"),
						psf.col("caller"),
						psf.col("day_full")
						))
			.select('key','grid')
			)
		# FlatMap
		nidtcilac5bis2 = nidtcilac5bis.rdd.flatMapValues(lambda l: l)
		nidtcilac5bis2 = nidtcilac5bis2.map(lambda l: flat_tuple((l[0],tuple(l[1]))))
		nidtcilac6 = (nidtcilac5bis2
			.map(lambda l: flat_tuple((l[0].split(":"),l[1],l[2])))
			)
		# Tran dataframe
		nidtcilac6 = spark.createDataFrame(nidtcilac6).toDF(*('month','caller','day_full','grid_id','p(c|v)'))
		# Key: month:caller:grid_id:day_full
		nidtcilac6 = (nidtcilac6
		.withColumn('key',
			psf.concat_ws(":",
				psf.col("month"),
				psf.col("caller"),
				psf.col("grid_id"),
				psf.col("day_full")				
			))
		)
	elif (useGrid) & (method == 'ma'):
		# SUM BY NIDTxINDIV BEFORE MERGING WITH GRID PROBABILITIES
		nidtcilac6 = (nidtcilac5
			.groupBy('month','caller','nidt')
			.count()
			.withColumnRenamed('count','p(v)')
			)
	else:
		nidtcilac6 = (nidtcilac5
			.withColumn('key',
				psf.concat_ws(":",
					psf.col("month"),
					psf.col("caller"),				
					psf.col("nidt")
					)
			.select('key','day_full'))
			)


	# # IF (grid_id) & method=='dd'
	# nidtcilac6.show(5)                                                          
	# +-----+---------------+--------+-------+--------------------+--------------------+
	# |month|         caller|day_full|grid_id|              p(c|v)|                 key|
	# +-----+---------------+--------+-------+--------------------+--------------------+
	# |    8|3.3677396295E10|     219|4655252|8.669109454640224E-6|8:3.3677396295E10...|
	# |    8|3.3677396295E10|     219|4655253| 0.05892549054979285|8:3.3677396295E10...|
	# |    8|3.3677396295E10|     219|4655254| 0.08611265627303633|8:3.3677396295E10...|
	# |    8|3.3677396295E10|     219|4655255| 0.08270046274255294|8:3.3677396295E10...|
	# |    8|3.3677396295E10|     219|4655256| 0.07100898098926664|8:3.3677396295E10...|
	# +-----+---------------+--------+-------+--------------------+--------------------+
	# only showing top 5 rows


	# # IF (grid_id) & method=='ma'
	# nidtcilac6.show(2)
	# +-----+---------------+----------+----+                                         
	# |month|         caller|      nidt|p(v)|
	# +-----+---------------+----------+----+
	# |    9|3.3670735881E10|00000218M2|   8|
	# |    9|3.3675274756E10|00000380E1|   7|
	# +-----+---------------+----------+----+
	# only showing top 2 rows


	# nidtcilac6 = nidtcilac6.repartition(sc.defaultParallelism)

	def count_tiles(x) :


	# ==========================================================
	# 			PART IVa: MAX ACTIVITY
	# ==========================================================
	# With that algorithm, p(v)=1 by assumption thus p(c)=sum(p(c|v))
	print("Estimating home for all phone users")

	if method=='ma':
		if useGrid:
			#
			# Join NIDT and grid
			# -----------------------------
			#
			nidtcilac7 = nidtcilac6.join(psf.broadcast(grid4),['nidt'],'left_outer')
			nidtcilac7 = nidtcilac7.withColumnRenamed('caller','CodeId')
			# nidtcilac7.show(2)
			# +----------+-----+---------------+----+--------------------+                    
			# |      nidt|month|         CodeId|p(v)|                grid|
			# +----------+-----+---------------+----+--------------------+
			# |00005707B1|    8|3.3675046434E10|   1|[[1148625,6.89375...|
			# |00000303R1|    8|3.3607552114E10|   6|[[4000896,7.18648...|
			# +----------+-----+---------------+----+--------------------+
			# only showing top 2 rows			
			nidtcilac7 = (nidtcilac7
				.na.drop()
				.withColumn("key",
				psf.concat_ws(":",
						psf.col("month"),
						psf.col("CodeId"),
						psf.col("p(v)")
						))
				.select('key','grid', 'nidt')
			) # Key: month:codeId:p(v) (p(v) must be kept)
			#
			# FlatMap
			# ------------------------
			#
			nidtcilac7bis = nidtcilac7.rdd.flatMapValues(lambda l: l)
			nidtcilac7bis = nidtcilac7bis.map(lambda l: flat_tuple((l[0],tuple(l[1]))))
			nidtcilac7bis = (nidtcilac7bis
				.map(lambda l: flat_tuple((l[0].split(":"),l[1],l[2])))
				)
			nidtcilac8 = spark.createDataFrame(nidtcilac7bis).toDF(*('month','CodeId','p(v)','grid_id','p(c|v)'))
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
			nidtcilac8 = nidtcilac8.withColumn('p(c)',psf.col('p(v)')*psf.col('p(c|v)'))
			nidtcilac8bis = (nidtcilac8
				.groupBy('month','CodeId','grid_id')
				.sum('p(c)').withColumnRenamed('sum(p(c))','p(c)')
				)
			# nidtcilac8bis = (nidtcilac8bis
			# 	.withColumn("key",
			# 	psf.concat_ws(":",
			# 			psf.col("month"),
			# 			psf.col("CodeId")
			# 	))
			# 	.select('key','grid_id','p(c)')
			# 	)
			# nidtcilac8bis.show(2)
			# +-----------------+-------+-------------------+                                 
			# |              key|grid_id|               p(c)|
			# +-----------------+-------+-------------------+
			# |9:3.3685030047E10|3887274|0.20095599659083557|
			# |9:3.3676292867E10|1133595|0.02073060319295847|
			# +-----------------+-------+-------------------+
			# only showing top 2 rows
			#
			# Select grid_id that maximizes the sum sum(p(c|v)) month:grid
			# ---------------------------------------------------------------
			#
			# CREATE WINDOW	TO ORDER COUNTS BY ID:MONTH
			w = psw.Window().partitionBy('month','CodeId').orderBy(psf.col("p(c)").desc())
			nidtcilac_max = (nidtcilac8bis
				.withColumn("rn", psf.row_number().over(w))
				.where(psf.col("rn") == 1)
				.withColumnRenamed('grid_id','cell_id')
				.select('month','CodeId','cell_id')
				)
			#
			#
			# nidtcilac_max.show(5)
			# +-----+---------------+-------+                                                 
			# |month|         CodeId|cell_id|
			# +-----+---------------+-------+
			# |    8|3.3607041063E10|3708333|
			# |    8|3.3607042172E10|3735767|
			# |    8|3.3607066213E10|3591742|
			# |    8| 3.360708584E10|3555900|
			# |    8| 3.360708984E10|3887280|
			# +-----+---------------+-------+
			# only showing top 5 rows
			#
			#
			df = nidtcilac_max			
			# nidtcilac9 = (nidtcilac8bis.rdd
			# 	.map(lambda l: (l['key'], (l['grid_id'], l['p(c)'])))
			# 	.reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[-1])) # Keep, for each key (month:CodeId), observation that appears most
			# 	.map(lambda l: flat_tuple((l[0].split(":"),l[1])))
			# 	)
			# # Transform in DataFrame
			# nidtcilac9a = spark.createDataFrame(nidtcilac9).toDF('month','CodeId','grid_id','p')
			# if month is not None:
			# 	nidtcilac9a = nidtcilac9a.withColumn('month',psf.lit(month))
			# nidtcilac9a = nidtcilac9a.select('month', 'CodeId','grid_id')
			# df = nidtcilac9a.withColumnRenamed('grid_id','cell_id')
			# nidtcilac9a.show(2)
			# +-----+---------------+-------+
			# |month|         CodeId|cell_id|
			# +-----+---------------+-------+
			# |   09|3.3672684671E10|3978254|
			# |   09|3.3608826884E10|3724338|
			# +-----+---------------+-------+
			# only showing top 2 rows
			# -----------------------------------------------
			#
		else: # VORONOI
			nidtcilac6bis = (nidtcilac6
				.withColumn("key",
				psf.concat_ws(":",
						psf.col("month"),
						psf.col("caller")
				))
				.select('key','nidt','p(v)')
				.rdd
				.map(lambda l: (l['key'], (l['nidt'], l['p(v)'])))
					.reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[-1])) # Keep, for each key (CodeId), observation that appears most
					.map(lambda l: flat_tuple((l[0].split(":"),l[1])))
				)
			# Transform in DataFrame
			nidtcilac6bis2 = spark.createDataFrame(nidtcilac6bis).toDF('month','CodeId','nidt','p')
			if month is not None:
				nidtcilac6bis2 = nidtcilac6bis2.withColumn('month',psf.lit(month))
			nidtcilac6bis2 = nidtcilac6bis2.select('month', 'CodeId','nidt')
			df = nidtcilac6bis2.withColumnRenamed('nidt','cell_id')
			#
			# nidtcilac6a2.show(2)
			# +-----+---------------+----------+
			# |month|         CodeId|   cell_id|
			# +-----+---------------+----------+
			# |   09|3.3608846779E10|00004653L1|
			# |   09|3.3608401671E10|00000070H1|
			# +-----+---------------+----------+
			# only showing top 2 rows


	# ===============================================================
	# 			PART IVb: DISTINCT DAYS
	# ===============================================================
	# We list every event by day and collect that list afterward

	if method == 'dd':
		# 
		# Keep one appearance by day for each id for each cell
		# --------------------------------------------------------
		nidtcilac6b = (nidtcilac6
			.select('key')
			.distinct() # Can be quite long
		)
		# nidtcilac6b.show(5, truncate = False)
		# +-----------------------------+                                                 
		# |key                          |
		# +-----------------------------+
		# |8:3.3671878975E10:2699795:222|
		# |8:3.3608479246E10:2690332:215|
		# |8:3.3608966164E10:1419553:222|
		# |8:3.363080747E10:3708747:239 |
		# |8:3.3677352676E10:1279607:234|
		# +-----------------------------+
		# only showing top 5 rows		
		#
		# Count by key
		# --------------------------
		# nidtcilac6b_G  = nidtcilac6b.groupBy('key').count()
		#
		# nidtcilac6b_G.show(2, truncate = False)
		# +-------------------------------------------+-----+                             
		# |key                                        |count|
		# +-------------------------------------------+-----+
		# |9:3.3683401249E10:00000144H3:832580:2111570|1    |
		# |9:3.364224054E10:00010998M1:263680:2244535 |1    |
		# +-------------------------------------------+-----+
		# only showing top 2 rows
		#
		# Arrange to keep month,CodeId,cell_id,number occurrence distinct days 
		# -------------------------------------------------------------------------
		#
		nidtcilac6b_G2 = (nidtcilac6b
			.withColumn('month', psf.split(psf.col("key"), ":").getItem(0))
			.withColumn('CodeId', psf.split(psf.col("key"), ":").getItem(1))
			.withColumn('cell_id', psf.split(psf.col("key"), ":").getItem(2))
			.drop('key')
			)
		#
		# Keep max for month,CodeId
		# ----------------------------------
		#
		nidtcilac6b_G3 = (nidtcilac6b_G2
				.groupBy('month','CodeId','cell_id')
				.count()
				)
		# CREATE WINDOW	TO ORDER COUNTS BY ID:MONTH
		w = psw.Window().partitionBy('month','CodeId').orderBy(psf.col("count").desc())
		nidtcilac_max = (nidtcilac6b_G3
			.withColumn("rn", psf.row_number().over(w))
			.where(psf.col("rn") == 1)
			.select('month','CodeId','cell_id')
			)
		#
		#
		# nidtcilac_max.show(5)
		# +-----+---------------+-------+                                                 
		# |month|         CodeId|cell_id|
		# +-----+---------------+-------+
		# |    8|3.3607041063E10|3706046|
		# |    8|3.3607042172E10|3735767|
		# |    8|3.3607066213E10|3584881|
		# |    8| 3.360708584E10|3562757|
		# |    8| 3.360708984E10|3887280|
		# +-----+---------------+-------+		
		# only showing top 5 rows		
		#
		#
		df = nidtcilac_max


	print("Saving data")
	if prior :
		df.write.csv(path + "/prior/" + str(month))
	else :
		df.write.csv(path + "/no_prior/" + str(month))
	print("End of function")

# MA with time restraint, september, Paris, no prior
# homeDetection(month = '09',
# 	method = 'ma',
# 	path = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior",
# 	gridPath = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior",
# 	useGrid = True,
# 	prior = False,
# 	night_time = (19,9),
# 	reduceData = True,
# 	pathNIDT = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior",
# 	filenameNIDT = "nidt_list_paris_uu.csv"
# 	)

# MA with time restraint, september, Paris, prior
homeDetection(month = '09',
	method = 'ma',
	path = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior",
	gridPath = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior",
	useGrid = True,
	prior = True,
	night_time = (19,9),
	reduceData = True,
	pathNIDT = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior",
	filenameNIDT = "nidt_list_paris_uu.csv"
	)

# Arguments pour tests
# month      = '09'
# method     = 'ma'
# path       =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior/15_11_18_Comparaison_MA_with_prior"
# gridPath   =  "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior"
# useGrid    = True
# prior      = True
# night_time = (19,9)
# reduceData = True
# pathNIDT = "/User/vblx1269/WORK/romain/Voronoi_to_grid_with_prior"
# filenameNIDT = "nidt_list_paris_uu.csv"


# # ===============================
# # COMPARE TO MAARTEN RESULTS

# if use.grid == False:
# 	schema = StructType([
# 	    StructField("CodeId", StringType()),
# 	    StructField("month", StringType()),
# 	    StructField("NIDT", StringType())
# 	])
# 	homedetection1 = (spark.read.format('csv')
# 		.schema(schema)
# 		.load('/User/vblx1269/WORK/lino/Home_detection/homedetection'+month+'.csv')
# 		.rdd
# 		.map(lambda l: (l[0].replace("(u","").replace("'",""),
# 						l[1].replace("u","").replace("'","").replace(" ",""),
# 						l[2].replace("u","").replace("'","").replace(")","").replace(" ","")
# 						))
# 		.map(lambda l: (int(float(l[0])),l[1][1],l[2]))
# 		)
# 	homedetection = home_detection.map(lambda l: (l[0],l[2])).toDF()
# 	homedetection1 = homedetection1.map(lambda l: (l[0],l[2])).toDF()
# 	df1.join(df2, df1("col1") === df2("col1"), "left_outer")
# 	df = homedetection.join(homedetection1, homedetection._1 == homedetection1._1,"left_outer")
# 	df = df.rdd.map(lambda l: (l[1],l[3]))
# 	df.filter(lambda l: l[0]==l[1]).count()
# 	df.filter(lambda l: l[0]!=l[1]).count()



