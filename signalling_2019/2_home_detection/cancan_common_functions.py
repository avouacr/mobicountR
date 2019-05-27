# =====================================================
# 				FUNCTIONS THAT ARE USED BY 
# 					SEVERAL FILES
# ======================================================

# To get it read by Spark, use :
# -----------------------------------

#program_path = 'hdfs:///User/zues5490/WORK/romain/common_functions.py'
#sc.addFile(program_path)
#from cancan_common_functions import *

import pyspark
from pyspark.sql import SQLContext
import pyspark.sql as psql
import pyspark.sql.functions as psql_f
import pyspark.sql.types as psql_t
import functools as functools


def subset_time(psql_f, df, timerange) :
	#
	if timerange[1] >= timerange[0] :
		df_sub = df.filter((psql_f.col("hour")
			.between(timerange[0], timerange[1]))
		)
	else :
		df_sub = df.filter((psql_f.col("hour").between(timerange[0], 24)) 
			| (psql_f.col("hour").between(0, timerange[1]))
			)
	#
	return df_sub
"""
	Subset mobile phone data to a given timerange

	Parameters
	----------
	psql_f: pyspark.sql.functions
	  	Module passed as dependency
	df: SparkDataFrame
		Mobile phone data
	timerange: tuple
		Timerange to subset to. Example : (21,7) for 21PM-7AM


	return: SparkDataFrame	
	"""


def import_cancan(sc, spark, sql_ctxt,
psql, psql_f, psql_t, functools,
month, day,
timerange,
technos = ["2G", "3G", "4G"],
test = False,
) :
	#
	# Define paths to CANCAN data
	hue_cancan = "/Data/P17100/SECURE/RAW/TXT/"
	#
	# Define schema for empty dataframes
	schema = psql_t.StructType(
		[psql_t.StructField("imsi", psql_t.StringType(), True),
 		 psql_t.StructField("techno", psql_t.IntegerType(), True),
 		 # psql_t.StructField("month", psql_t.IntegerType(), True),
 		 # psql_t.StructField("day", psql_t.IntegerType(), True),
 		 psql_t.StructField("hour", psql_t.IntegerType(), True),
 		 psql_t.StructField("lac", psql_t.IntegerType(), True),
 		 psql_t.StructField("ci_sac", psql_t.IntegerType(), True)]
 	)
	#
	if day is None :
		print("No day was provided so data of all month is loaded.")
		tempo_path = month + "/*"
	else :
		tempo_path = month + "/" + day + "/*"
	# 
	# Import 2G data if specified
	if "2G" in technos :
		print("Import 2G data")
		if not test :
			end_path_2g = "A/2019/" + tempo_path
		else :
			end_path_2g = "A/2019/04/20/19989484584153697"
		#
		df_2g = spark.read.parquet(hue_cancan + end_path_2g)
		#
		# Keep only Orange clients (filter out imsi from roaming, connected objects..)
		df_2g = df_2g.where(psql_f.col("IMSI").substr(1, 5) == "20801")
		#
		# Extract month, day and hour from timestamp
		df_2g = (df_2g
		.withColumn("end_timestamp", 
			psql_f.concat(psql_f.col("end_day"), 
				psql_f.lit(" "), psql_f.col("end_time")))
		.withColumn("end_timestamp", 
			psql_f.unix_timestamp(psql_f.col("end_timestamp"), 
				"dd/MM/yyyy HH:mm:ss"))
		.withColumn("end_timestamp", psql_f.col("end_timestamp").cast("timestamp"))
		# .withColumn("month", psql_f.month("end_timestamp"))
		# .withColumn("day", psql_f.dayofmonth("end_timestamp"))
		.withColumn("hour", psql_f.hour("end_timestamp"))
		)
		# Subset to night timerange if specified
		if timerange is not None :
			df_2g = (df_2g
		    .withColumn("end_timestamp", 
		    	psql_f.concat(psql_f.col("end_day"), psql_f.lit(" "), 
		    		psql_f.col("end_time"))
		    	)
		    .withColumn("end_timestamp", 
		    	psql_f.unix_timestamp(psql_f.col("end_timestamp"), 
		    		"dd/MM/yyyy HH:mm:ss"))
		    )
		    #
			df_2g = subset_time(psql_f = psql_f, df = df_2g, timerange = timerange)
		#
		# Subset to rows without missing values for SAC and LAC
		# (both necessay to join with FV topology register)
		df_2g = df_2g.where(psql_f.col("LAC").isNotNull() & 
		psql_f.col("Current_Cell_Identifier").isNotNull()
		)
		#
		# Keep relevant columns and rename them as 4G
		df_2g = (df_2g.selectExpr("IMSI as imsi", 
				"cast(LAC as long) lac", 
				"cast(Current_Cell_Identifier as long) ci_sac",
				"cast('2' as long) techno",
				# "month as month",
				# "day as day",
				"hour as hour")
		# .withColumn("lac_ci", 
		# 	psql_f.concat(psql_f.col("lac"), psql_f.lit("_"), psql_f.col("ci_sac")))
		# .drop("lac").drop("ci_sac")
				)
	# Create empty dataframe if technology is not required, to enable final join
	else :
		df_2g = sql_ctxt.createDataFrame(sc.emptyRDD(), schema)
	#
	# Import 3G data if specified
	if "3G" in technos :
		print("Import 3G data")
		if not test :
			end_path_3g = "IU/2019/" + tempo_path
		else :
			end_path_3g = "IU/2019/04/20/19986993211057390"
		#
		df_3g = spark.read.parquet(hue_cancan + end_path_3g)
		#
		# Keep only Orange clients (filter out imsi from roaming, connected objects..)
		df_3g = df_3g.where(psql_f.col("IMSI").substr(1, 5) == "20801")
		#
		# Extract month, day and hour from timestamp
		df_3g = (df_3g
		.withColumn("end_timestamp", 
			psql_f.unix_timestamp(psql_f.col("End_Time"), "dd/MM/yyyy HH:mm:ss"))
		.withColumn("end_timestamp", psql_f.col("end_timestamp").cast("timestamp"))
		# .withColumn("month", psql_f.month("end_timestamp"))
		# .withColumn("day", psql_f.dayofmonth("end_timestamp"))
		.withColumn("hour", psql_f.hour("end_timestamp"))
		)
		# Subset to night timerange if specified
		if timerange is not None :
			df_3g = subset_time(psql_f = psql_f, df = df_3g, timerange = timerange)
		#
		# Subset to rows without missing values for SAC and LAC
		# (both necessay to join with FV topology register)
		df_3g = df_3g.where(psql_f.col("LAC").isNotNull() &
		psql_f.col("SAC_Value").isNotNull())
		#
		# Keep relevant columns and rename them as 4G
		df_3g = (df_3g.selectExpr(
		"IMSI as imsi", 
		"cast(LAC as long) lac", 
		"cast(SAC_Value as long) ci_sac",
		"cast('3' as long) techno",
		# "month as month",
		# "day as day",
		"hour as hour")
		# .withColumn("lac_ci", 
		# 	psql_f.concat(psql_f.col("lac"), psql_f.lit("_"), psql_f.col("ci_sac")))
		# .drop("lac").drop("ci_sac")
		)
	# Create empty dataframe if technology is not required, to enable final join
	else :
		df_3g = sql_ctxt.createDataFrame(sc.emptyRDD(), schema)
	#
	#
	# Import 4G data
	if "4G" in technos :
		print("Import 4G data")
		if not test :
			end_path_4g = "S1C/2019/" + tempo_path
		else :
			end_path_4g = "S1C/2019/04/20/19925827706671103"
		#
		df_4g = spark.read.parquet(hue_cancan + end_path_4g)
		#
		# Keep only Orange clients (filter out imsi from roaming, connected objects..)
		df_4g = df_4g.where(psql_f.col("imsi").substr(1, 5) == "20801")
		#
		# Extract month, day and hour from timestamp
		df_4g = (df_4g
		.withColumn("end_timestamp", psql_f.col("end_date_utc").cast("timestamp"))
		# .withColumn("month", psql_f.month("end_timestamp"))
		# .withColumn("day", psql_f.dayofmonth("end_timestamp"))
		.withColumn("hour", psql_f.hour("end_timestamp"))
		)
		# Subset to night timerange if specified
		if timerange is not None:
			df_4g = subset_time(psql_f = psql_f, df = df_4g, timerange = timerange)
		#
		# Subset to rows without error
		df_4g = df_4g.where(psql_f.col("cdr_result") == 0)
		# Subset to rows without missing values for enodeb_id and cell_id
		# (both necessay to compute complete CID)
		# enodeb_id is almost never missing, but cell_id is in about 40% of events,
		# mainly when type = 9 ("S1 Release")
		df_4g = df_4g.where(psql_f.col("enodeb_id").isNotNull() & psql_f.col("cell_id").isNotNull())
		# Creation of ci_sac column for merging : ci_sac = 256*enodeb_id + cell_id
		# Verification possible using : https://www.cellmapper.net/enbid?lang=fr
		# We also create lac column to have consistent columns with other technologies
		# lac is always equal to 1 for 4G cells
		df_4g = (df_4g
		.withColumn("ci_sac", 256*psql_f.col("enodeb_id") + psql_f.col("cell_id"))
		.withColumn("lac", psql_f.lit(1))
		)
		#
		# Keep relevant columns
		df_4g = (df_4g.selectExpr(
					"imsi",
					"cast(lac as long) lac",
					"cast(ci_sac as long) ci_sac",
					"cast('4' as long) techno",
					# "month as month",
					# "day as day",
					"hour as hour")
		# .withColumn("lac_ci",
		# 	psql_f.concat(psql_f.col("lac"), psql_f.lit("_"), psql_f.col("ci_sac")))
		# .drop("lac").drop("ci_sac")
		)
	# Create empty dataframe if technology is not required, to enable final join
	else :
		df_4g = sql_ctxt.createDataFrame(sc.emptyRDD(), schema)
	#
	# Concatenate imported datasets
	print("Concatenate data of specified technologies")
	if "2G" in technos and "3G" in technos and "4G" in technos :
		data_cancan = df_2g.union(df_3g).union(df_4g)
	else :
		list_df = [df_2g, df_3g, df_4g]
		data_cancan = functools.reduce(psql.DataFrame.unionAll, list_df)
	#
	# Drop missing/null values
	data_cancan = data_cancan.dropna(how = "any")
	#
	return data_cancan
"""
	Import CANCAN data for specified technologies and timeranges

	Parameters
	----------
	sc, spark, sql_ctxt, psql, psql_f, psql_t, functools: 
		sparkContext, SparkSession, SQLContext, pyspark.sql.functions, 
		pyspark.sql.types, functools
		Modules passed as dependencies
	month: str
		Month of data to import, format "MM"
	day: str
		Day of data to import, format "DD" 
		(if None, data of whole month is imported)
	timerange: tuple
		Timerange to subset data to. Example : (21,7) for 21PM-7AM
		If no subsetting needed, pass subset_time = None  	
	technos: list of str
		Possible : "2G", "3G, "4G".
		Default : ["2G", "3G, "4G"] (all technologies are imported)
	test: boolean
		If True, import one small file of data from a hard coded day (04/20/19)
		for each technology specified in technos, for testing purpose.
		Default to False.

	
	return: SparkDataFrame	
	"""
#
# Example snippet to add to the import_cancan function to dismiss invalid parquet files
# cmd = 'hdfs dfs -ls /Data/P17100/SECURE/RAW/TXT/IU/2019/04/10'
# files = subprocess.check_output(cmd, shell=True)
# files = "".join( chr(x) for x in files).strip().split("\n")[1:]
# paths = [re.search('(\/Data.+)', x).group(1) for x in files]
# paths = [x for x in paths if 
# x != "/Data/P17100/SECURE/RAW/TXT/IU/2019/04/10/19186827159136415"]
# df_3g = spark.read.parquet(*paths)