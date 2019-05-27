# =====================================================
# 				FUNCTIONS THAT ARE USED BY 
# 					SEVERAL FILES
# ======================================================

# To get it read by Python, use
# -----------------------------------

# --py-files '/User/vblx1269/WORK/lino/_Functions/00_CommonFunctions.py'
# in Spark context call and:
# within filfrom CommonFunctions import *
# or you must do:
# program_path = 'hdfs:///User/vblx1269/WORK/lino/Functions/CommonFunctions.py'
# sc.addFile(program_path)
# from CommonFunctions import *


from typing import Iterable
import pyspark.sql.functions as psf
import pyspark

# ===============================================
# 		 I - FUNCTIONS TO HANDLE TUPLES
# ===============================================


def flat_tuple(a):
    """Flattens tuple and list combinations into a single tuple.

	Parameters
	----------
	a: tuple
		Nested tuple that should be flattened

    Example Usage:
    	stuff = [[([[5,6], ("poo", 0, 1, None, 2), (3, 4, 5), (6, [7, 8])])]]
    	flat_tuple(stuff) == (5, 6, 'poo', 0, 1, None, 2, 3, 4, 5, 6, 7, 8)
    True
    """
    if type(a) not in (tuple, list): return (a,)
    if len(a) == 0: return tuple(a)
    return flat_tuple(a[0]) + flat_tuple(a[1:])


def flat_list(a):
    """Flattens tuple and list combinations into a single list.

	Parameters
	----------
	a: list
		Nested list that should be flattened

    Example Usage:
    	stuff = [[([[5,6], ("poo", 0, 1, None, 2), (3, 4, 5), (6, [7, 8])])]]
    	flat_list(stuff) == [5, 6, 'poo', 0, 1, None, 2, 3, 4, 5, 6, 7, 8]
    True
    """
    if type(a) not in (tuple, list): return [a]
    if len(a) == 0: return list(a)
    return flat_list(a[0]) + flat_list(a[1:])


def convertTuple(l):
	"""Convert elements within tuple into float

	Parameters
	----------
	l: tuple
		A tuple storing string that could be converted into float

	Returns the inial tuple with elements inside being converted
	"""
	return tuple(map(float, l))


def addTuple(a,b,lambda1 = 1,lambda2 = 1):
	"""Consistently add tuple dimensions: lambda1*(x1,...,xN) + lambda2*(y1,...,yN) --> (lambda1*x1 + lambda2*y1,...,lambda1*xN + lambda2*yN)

	Parameters
	----------
	a: tuple
		Tuple
	b: tuple
		Tuple with same length than a
	lambda1: float
		Factor by which dimensions of a should be multiplied. 
		Default is to keep unchanged a_1,...,a_n
	lambda2: float
		Factor by which dimensions of b should be multiplied. 
		Default is to keep unchanged b_1,...,b_n		

	Example Usage:
		addTuple((1,2),(3,4), lambda1 = 2)
		(5,8)
	"""
	return tuple(lambda1*t1 + lambda2*t2 for t1,t2 in zip(a,b))



def factorTuple(factor,a,index_min = 1):
	"""Apply a factor to all tuples dimensions: lambda1*(x1,...,xN) --> (lambda1*x1,...,lambda1*xN)

	Parameters
	----------
	factor: double 
		Factor that should multiply all elements in a
	a: tuple
		Tuple
	index_min: int
		Index from which we should start factorization

	Return:
		(factor,tuple)
	It is also possible to start factorization at a given index
	Example Usage:
		factorTuple(2,(1,2,3))
		(2, 4.0, 6.0)
		factorTuple(2,(1,2,3, index_min = 0))
		(2, 2.0, 4.0, 6.0)
	"""
	if index_min>0:
		return flat_tuple((factor, tuple(i * float(factor) for i in a[index_min:])))
	else:
		return flat_tuple((factor, tuple(i * float(factor) for i in a)))


# ===============================================
# 		 II - COMBINERS
# ===============================================


def to_list(a):
	"Wrapper to transform int/str/double into list"
	return [a]

def append(a, b):
	"Wrapper for append function"
	a.append(b)
	return a

def extend(a, b):
	"Wrapper for extend function"
	a.extend(b)
	return a


# ===============================================
# 		 III - EFFICIENT JOIN THROUGH BROADCAST
# ===============================================

# FUNCTION TO KEEP ONLY PEOPLE WITHIN URBAN CLUSTER OF INTEREST
def individualParis(l, list_paris):
	"""
	Match Code_Id given in l with a list of individuals living in a given city
	NB: Can be applied to any city, not only Paris

	:return: a tuple with None as third or fourth position if CodeId1 or CodeId2 is
	not identified as living in the city
	
	Keyword arguments
	-----------------
	l -- Spark rdd with CodeId1 and Code_Id2 as l[0] and l[1]
	list_paris -- Spark broadcasted data of CodeId of people living in the city
	"""
	val1 = list_paris.value.get(l[0])
	val2 = list_paris.value.get(l[1])
	return (l[0],l[1],val1,val2)


def HDparis(l, list_paris):
	"""
	Wrapper to make a broadcast join given a list
	of individuals that live in a city

	Parameters
	----------
	l: tuple
		Elements called by lambda function inside map for rdd
	list_paris: dictionnary 
		A list stored as dictionnary after collectAsMap() storing
		individuals identifiers and cell identifier for their
		residence

	return: 
		A broadcasted rdd
	"""
	val = list_paris.value.get(l[1])
	return (l[0],l[1],val)


# ===============================================
# 		 IV - SSI AND PSI FUNCTIONS
# ===============================================


def score(i,j,total_user):
	"""
	Social Similarity Function defined in Yang's Paper

	Parameters
	----------
	i: int
		Rank of first individual
	j: int
		Rank of second individual
	total_user: int
		Number of individuals living in the city (N)

	return:
		s_{x \to y} in Yang's paper
	"""
	if i<=0.5 * total_user:
		if i==j:
			similarity = 1
		elif (j<=((2*i)-1)):
			similarity = 1-((4*abs(i-j)-1)/(2*(total_user-1)))
		else:
			similarity = 1-((j-1)/(total_user-1))
	else:
		if i==j:
			similarity = 1
		elif j>=((2*i)-total_user):
			similarity = 1-((4*abs(i-j)-1)/(2*(total_user-1)))
		else:
			similarity = 1-((total_user-j)/(total_user-1))
	return similarity


def ssi_1iter(line,total_user):
	"""
	[Discarded] Function to compute SSI when we perform median imputation
	"""
	res=[]
	for i in range(len(line)):
		val,poids = 0,0
		ind = line[i][0]
		list1 = [item for item in line if item[0]!=ind]
		for j in range(len(list1)):
			val+=score(float(line[i][2]),float(list1[j][2]),float(total_user))*float(list1[j][1])
			poids+=float(list1[j][1])
		res.append([line[i][0],line[i][1],val/float(poids)]) #CodeId,prob,score
	return res


def ssi_Kiter(line,K,total_user):
	"""
	[Discarded] Function to compute SSI when we perform mutiple income simulations
	"""
	res=[]
	for i in range(len(line)):
		val,poids = [float(0)]*K,[float(0)]*K
		ind = line[i][0]
		list1 = [item for item in line if item[0]!=ind]
		for iteration in range(K):
			for j in range(len(list1)):
				val[iteration]+=score(float(line[i][2][iteration]),float(list1[j][2][iteration]),float(total_user))*float(list1[j][1])
				poids[iteration]+=float(list1[j][1])
		res.append(flat_list(
		[line[i][0],line[i][1],[x/float(y) for x, y in zip(val, poids)]])
		) #CodeId,prob,score
	return res


# ===============================================
# 		 V - FUNCTIONS TO RETURN INFORMATION
# ===============================================

def returnIncome(l, math, method = 'noiseQuantiles'):
	"""
	Funtion to return income given an uniform distribution simulation

	Parameters
	----------
	l: tuple
		An element called by a lambda function inside a map stage of an RDD
	math: module
		math module from Python
	method: str
		Method that is used for income simulation. Should be 'noiseQuantiles' or
		'interpoQuantiles'

	return:
		A tuple storing income given the values of uniform sample
	"""
	if method == 'noiseQuantiles':
		return [l[1][k] for k in list(l[2:])]
	else:
		return [float(l[1][math.floor(k)])+float(math.modf(k)[0])*(float(l[1][math.ceil(k)])-float(l[1][math.floor(k)])) for k in list(l[2:])] #q1 + partdecimale(u)*(q2-q1)



# ===============================================
# 		 VI - SIMULATE INCOME
# ===============================================

# pst = pyspark.sql.types
# psf = pyspark.sql.functions
# pmr = pyspark.mllib.random
# city = 'marseille'
# month = '09'
# K=3
# useGrid = True
# impute_median = False
# nonParametric = True
# method = 'noiseQuantiles'
# nquant = 20
# weights = 'fixedNumber'
# HD_path = "/User/vblx1269/WORK/lino/_Home Detection Grid"
# quantiles_path = "/User/vblx1269/WORK/lino/_Quantiles by city grid"



def simulateIncome(spark, sc, pst, psf, pmr, math,
	city = 'marseille', K=3, month = '09',
	useGrid = True,
	impute_median = False,
	nonParametric = True,
	method = 'noiseQuantiles',
	nquant = 20,
	weights = 'fixedNumber',
	scale = 'level',
	HD_path = "/User/vblx1269/WORK/lino/_Home Detection Grid",
	quantiles_path = "/User/vblx1269/WORK/lino/_Quantiles by city grid",
	statGrid_path = "/User/vblx1269/WORK/lino/_Stats grid/500m"
	):
	"""
	Function to assign phone users income given the raster cell in which they live

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	sc: SparkSession.sparkContext
	  Spark context passed as dependency
	pst: pyspark.sql.types
	  Module passed as dependency
	psf: pyspark.sql.functions
	  Module passed as dependency
	pmr: pyspark.mllib.random
	  Module passed as dependency
	math: math
	  Module passed as dependency
	city: str
	  City that will be used studied
	K: int
	  Number of iterations for simulation. Ignored if impute_median = False (default)
	month: str
	  Month that should not be considered
	useGrid: bool
	  Should we use voronoi (useGrid = False) or grid (useGrid = True)
	impute_median: bool
	  Should we perform median imputation (True) or income simulation ?
  nonParametric: bool
    Should we perform income simulation based on parametric approach (False), i.e. 
    normal distribution simulation, or non parametric approach (True)
	method: str
	  Method for income simulation. Should be 'noiseQuantiles' or 'interpoQuantiles'
	nquant: int
	  Number of quantiles to impute if weights = 'fixedNumber'
	weights: str
	  Which dataframe format shoud be used to read quantiles?:
	    * 'fixedNumber': Fixed number of quantiles (given by 'nquant' argument)
	    * 'fixedWeights': Fixed representativity weight for each quantiles (11 individuals)
	HD_path: str
	  Path where home detection data are stored as csv
  quantiles_path:
    Path where quantiles are stored
	statGrid_path: str
	  Path where mean and median by grid are stored. Ignored if nonParametric = True (default)
	 
	return:
		SparkDataFrame with CodeId and income assignation for all iterations
  """

	if impute_median:
		print("Assigning income based on median income within spatial unit")
	else:
		print("Income simulation: "+ str(K) + ' iterations')
		print("nonParametric: " + str(nonParametric) + " and method: " + str(method))


	if scale not in ['level', 'log']:
		sys.exit("'scale' argument not valid: should be 'level' or 'log'")


	# QUANTILES BY SPATIAL UNIT
	# -----------------------------

	if useGrid:		
		if impute_median:
			# IMPORT QUANTILES BY GRID
			grid_paris = (spark.read.format('csv').option("header", "true")
			.load(quantiles_path + "/quantiles_" + city +".csv")
			.select('grid_id','`revenu.person_50`')
			.withColumnRenamed('revenu.person_50','mediane')
			)
		else:
			# IMPORT QUANTILES BY GRID
			grid_paris = (spark.read.format('csv').option("header", "true")
				.load(quantiles_path + "/quantiles_" + city +".csv")
				)
			# COMPUTE JITTER WHEN NECESSARY
			if (nonParametric == True) & (method == 'noiseQuantiles'):
				jitter = (grid_paris
					.select('`revenu.person_25`','`revenu.person_75`')
					.withColumn("x", psf.col('`revenu.person_75`')-psf.col('`revenu.person_25`')) # Q75 - Q25
					.filter(psf.col("x") > 100) # Ensure there is large enough jitter to change data a little bit
					.agg(psf.min('x')) # Keep juste one jitter: minimum
					.collect()[0] # Collect value
					)
				jitter = jitter["min(x)"]
			if (weights=="fixedNumber"):
				# LIST
				grid_paris = (grid_paris.rdd.map(lambda l: (l[0],list(l[1:])))
					.toDF()
					.withColumnRenamed("_1","grid_id")
					.withColumnRenamed("_2","quantiles")
					)
			else:
				grid_paris = (spark.read.format('csv').option("header", "true")
					.load(quantiles_path + "/quantiles_" + city +"2.csv")
					.groupBy('grid_id')
					.agg(psf.collect_list("q").alias("quantiles"))
					) # Take the same format				
			# END OF if (nonParametric == True) & (method == 'noiseQuantiles')
		schema2 = pst.StructType([
				pst.StructField("CodeId", pst.StringType(), True),
				pst.StructField("month", pst.StringType(), True),
				pst.StructField("grid_id", pst.StringType(), True)]
		    )
	    # WHERE PEOPLE ARE ASSIGNED
		HD_paris = (spark.read.format('csv').option("header", "false")
				.schema(schema2)
				.load(HD_path + "/HD_grid_"+ month +".csv")
				.select('CodeId','grid_id')
		)
		# BROADCAST JOIN TO SPEED UP PROGRAM
		df_rank = HD_paris.join(psf.broadcast(grid_paris), ['grid_id'], 'left_outer')
		if impute_median:
			df_rank = df_rank.filter(psf.col("mediane").isNotNull())
		else:
			df_rank = df_rank.filter(psf.col("quantiles").isNotNull())
	else:
		# VORONOI LEVEL OBSERVATIONS
		df_rank = (spark.read.format("csv")
				.option("header", "true")
				.load(quantiles_path + "/quantiles_"+month+"_"+city+".csv")
			)
		# ECART INTERQUARTILE
		if (impute_median==False) & (nonParametric == True) & (method == 'noiseQuantiles'):
			jitter = (df_rank
					.select('`revenu.person25`','`revenu.person75`')
					.withColumn("x", psf.col('`revenu.person75`')-psf.col('`revenu.person25`')) # Q75 - Q25
					.filter(psf.col("x") > 100) # Ensure there is large enough jitter to change data a little bit
					.agg(psf.min('x')) # Keep juste one jitter: minimum
					.collect()[0] # Collect value
					)


	# Example: impute_median==False
	# df_rank.show(2)
	# +-------+-----------+--------------------+
	# |grid_id|     CodeId|           quantiles|
	# +-------+-----------+--------------------+
	# | 898098|33689396941|[0, 9338.5, 13342...|
	# |1014695|33683744446|[461.333333333333...|
	# +-------+-----------+--------------------+
	# only showing top 2 rows

	# Example: impute_median==True
	# df_rank.show(2)
	# +-------+-----------+------------------+
	# |grid_id|     CodeId|           mediane|
	# +-------+-----------+------------------+
	# | 898098|33689396941|             27790|
	# |1014695|33683744446|16619.565217391304|
	# +-------+-----------+------------------+
	# only showing top 2 rows

	if useGrid:
		if impute_median:
			df_rank = (df_rank
				.filter(psf.col("mediane").isNotNull())
				.filter(psf.col("mediane").isin(['NA', 'NaN',''])==False)
				.select('CodeId','mediane')
				)
		else:
			df_rank = df_rank.drop('grid_id').rdd.map(lambda l: flat_tuple(tuple(l)))
			df_rank = (df_rank
			.filter(lambda l: (sum(x is None for x in l[1:])==0))
			.filter(lambda l: (sum(x in ['NA', 'NaN',''] for x in l[1:])==0))
			)
	else:
		if impute_median:
			df_rank = (df_rank
				.select('CodeId','`revenu.person50`')
				.withColumnRenamed('revenu.person50','mediane')
				.filter(psf.col("mediane").isNotNull())
				.filter(psf.col("mediane").isin(['NA', 'NaN',''])==False)
				)
		else:
			df_rank2 = (df_rank.rdd.map(lambda l: (l[0],list(l[1:])))
				.toDF()
				.withColumnRenamed("_1","CodeId")
				.withColumnRenamed("_2","quantiles")
				)


	# DETERMINE N
	# -----------------------------

	if (weights == 'fixedWeights') & (impute_median == False) & useGrid:
		df_rank2 = df_rank.map(lambda l: (str(l[0]),list(l[1:])))
		total_user = df_rank2.count()
	elif (weights == 'fixedNumber') & (impute_median == False) & useGrid:
		df_rank2 = df_rank.map(lambda l: (str(l[0]),l[1:]))
		total_user = df_rank2.count()
	elif (useGrid == False) & (impute_median == False):
		total_user = df_rank2.count()
	else:
		total_user = df_rank.count()


	# END FUNCTION EXECUTION HERE IF impute_median==True
	# --------------------------------------------------------

	if impute_median:
		df_rank = df_rank.withColumn("mediane", psf.col("mediane").cast(pst.DoubleType()))
		print('Income simulation succeeded')
		return df_rank
	# End if impute_median==True


	# SIMULATION
	# ---------------------

	if nonParametric:
		if K==1:
			# Discarded
			if method == 'noiseQuantiles':
				simulation = (pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K)
					.map(lambda l: float(l))
					.map(lambda l: int(math.floor(l*nquant)))
					.map(lambda l: (l,))
					)
			else:
				simulation = (pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K)
					.map(lambda l: float(l))
					)				
		else:
			if weights == "fixedNumber":
				if method == 'noiseQuantiles':
					simulation = (pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K)
						.map(lambda l: tuple(l*nquant))
						.map(lambda l: map(int,map(math.floor,l)))
						.map(lambda l: tuple(l))
						)
				else: #method=='interpoQuantiles'
					simulation = (pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K)
						.map(lambda l: tuple(l*(nquant-1))) # L-1 because Python indexes from 0 to L-1
						)
			else:
				# Weights: fixedWeights
				simulation = pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K).map(lambda l: tuple(l))
		if method == 'noiseQuantiles':
			# CREATE JITTER
			simu_jitter = (pmr.RandomRDDs.normalVectorRDD(sc, total_user, K)
					.map(lambda l: tuple(l*jitter/(nquant/4)))
					.map(lambda l: map(int,map(math.floor,l)))
					.map(lambda l: tuple(l))
					)
			simu_jitter = simu_jitter.zipWithIndex().map(lambda l: (l[1],l[0]))
	else:
		param_norm = (spark.read.format("csv")
			.option("header", "true")
			.load(statGrid_path + "/" + city + ".csv")
			.select(*('grid_id','mu','sd'))
			)
		simulation = pmr.RandomRDDs.normalVectorRDD(sc, total_user, K).map(lambda l: tuple(l))

	# Example: nonParametric = True 
	# simulation.take(2)
	# [(8, 18, 1), (4, 8, 4)

	# Example: nonParametric = False
	# simulation.take(2)
	# [(-0.16878333744271926, 0.06482650354898963, -0.6205224792339801), (-0.19073697946737642, -0.8046811820919559, -0.46408569147302)]


	# JOIN SIMULATION WITH INITIAL DATA
	# ----------------------------------------

	if useGrid==False:
		df_rank2 = df_rank2.rdd

	# MERGE SIMULATION WITH CODE ID
	if nonParametric:
		df_rank2=df_rank2.zipWithIndex().map(lambda l: (l[1],l[0]))
	else:
		df_rank2 = HD_paris.rdd.zipWithIndex().map(lambda l: (l[1],tuple(l[0])))
	
	simulation=simulation.zipWithIndex().map(lambda l: (l[1],l[0]))
	simulation2=df_rank2.join(simulation).map(lambda l: l[1][0]+l[1][1])

	# Rescale uniform draws when weigths=='fixedWeights'
	if weights=='fixedWeights':
		if method == 'interpoQuantiles':
			simulation2 = simulation2.map(lambda l: (l[0], l[1]) + tuple(x*len(l[1]) for x in l[2:]))
		else:
			simulation2 = simulation2.map(lambda l: (l[0], l[1]) + tuple(math.floor(x*(len(l[1]))-1) for x in l[2:])) # len(.) -1 because Python indexes from 0 to L-1


	# ASSIGN INCOME USING NUMERICAL SIMULATION
	# ----------------------------------------------

	if nonParametric:
		if K==1:
			if method == 'noiseQuantiles':
				simulation3 = (simulation2
					.map(lambda l: (str(l[0]),float(l[1][l[2]]))) # Assign a quantile based on result U[0,5]
				)
		else:
			simulation3 = (simulation2
				.map(lambda l: (str(l[0]), returnIncome(l, math, method)))
				)
			simulation3bis = simulation3.map(lambda l: flat_tuple(l))
		if method == 'noiseQuantiles':			
		# Jitterize data
			simulation3=simulation2.zipWithIndex().map(lambda l: (l[1],l[0]))
			if impute_median==False:
				simulation3bis=simulation3.join(simu_jitter)
				simulation3bis=(simulation3bis
					.map(lambda l: l[1])
					.map(lambda l: (l[0][0], convertTuple(l[0][1]),l[1]))
					.map(lambda l: (l[0],addTuple(l[1],l[2])))
					.map(lambda l: flat_tuple(l))
					)
			else:
				simulation3bis = simulation3.map(lambda l: flat_tuple(l[1]))
	else:
		simulation3 = simulation2.map(lambda l: (l[0],l[1], [float(s) for s in l[2:]]))
		simulation3 = spark.createDataFrame(simulation3,('CodeId','grid_id','u'))
		simulation3bis = (simulation3
			.join(psf.broadcast(param_norm), ['grid_id'],'left_outer')
			.filter(psf.col('mu').isNotNull() & psf.col('sd').isNotNull())
			)
		if scale == 'level':
			simulation3bis = (simulation3bis.rdd
				.map(lambda l: (l['CodeId'],
					[float(l['mu']) + s*float(l['sd']) for s in l['u']]))
				.map(lambda l: flat_tuple(l)))
		else:
			simulation3bis = (simulation3bis.rdd
				.map(lambda l: (l['CodeId'],
					[math.exp(
						float(l['mu']) + s*float(l['sd'])
						) for s in l['u']]))
				.map(lambda l: flat_tuple(l)))


	# TRANSFORM IN DATAFRAME
	# ----------------------------------------------


	if K==1:
	 	simulation4 = spark.createDataFrame(simulation3bis,("CodeId","income"))
	else:
		if impute_median:
			simulation4 = simulation3bis.withColumnRenamed("mediane",'income')
		else:
		 	simulation4 = (spark.createDataFrame(simulation3bis,
		 		flat_tuple(("CodeId", tuple([str('income') + str(l) for l in range(K)]))))
		 	)
		 	simulation4 = (simulation4.rdd
			.filter(lambda l: sum(x is None for x in l)==0)
			.toDF()
			)

	return simulation4


# ==============================================================
# 		 VII - RECOVER QUANTILE INDIVIDUAL BELONGS TO
# ==============================================================

def assignQuantile(spark, pmf, psf,
	simulation4,
	impute_median = True, K = 3, numQuantiles = 10):
	"""
	Function to recover where a phone user is in the income distribution given 
	his simulated income
	
	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	pmf: pyspark.ml.feature
	  Module passed as dependency
	psf: pyspark.sql.functions
	  Module passed as dependency
	simulation4: Spark DataFrame produced by simulationIncome
    DataFrame produced by simulationIncome
	K: int
	  Number of iterations for simulation. Ignored if impute_median = False (default)
	impute_median: bool
	  Should we perform median imputation (True) or income simulation ?
	numQuantiles: int
	  Assign individuals its place in income distribution. Default is indicating
	  which decile individual comes from. 

	return:
		SparkDataFrame. This information is not used in SSI or PSI computations but 
		is returned for ex-post information on income group analysis. 
  """

	# Create a duplicate that will be iteratively modified
	simulation4q = simulation4


	# DEFINE A DISCRETIZER FOR FIRST COLUMN
	# ----------------------------------------------

	# Define discretizer that cuts continuous variable into discrete values
	discretizer = pmf.QuantileDiscretizer(numBuckets=numQuantiles, inputCol=simulation4q.columns[1],
		outputCol=str(simulation4q.columns[1]) + "_q"
	)

	# Apply that discretizer
	result = (discretizer.fit(simulation4q)
		.transform(simulation4q))

	# IF impute_median==True, END OF FUNCTION
	if impute_median:
		result = result.withColumnRenamed("mediane_q", "decile")
		return result 


	# STACK DISCRETIZER FOR OTHER COLUMNS ITERATIVELY
	# -----------------------------------------------------

	if K==2:
		# Define discretizer that cuts continuous variable into discrete values
		discretizer = pmf.QuantileDiscretizer(numBuckets=numQuantiles, inputCol=simulation4q.columns[K],
			outputCol=str(simulation4q.columns[K]) + "_q"
		)
		# Apply that discretizer
		result = (discretizer.fit(result)
			.transform(result))
	else:
		for k in range(2,K+1):
		# Define discretizer that cuts continuous variable into discrete values
			discretizer = pmf.QuantileDiscretizer(numBuckets=numQuantiles, inputCol=result.columns[k],
				outputCol=str(result.columns[k]) + "_q"
			)
		# Apply that discretizer
			result = (discretizer.fit(result)
				.transform(result))

	# TRANSFORM IN ARRAYED DATAFRAME
	# -----------------------------------------------------

	# Stack income and quantiles into [income_K,quantile_K] columns
	result2 = result.withColumn("iter_0", psf.array('income0','income0_q'))
	if K==2:
		result2 = result2.withColumn("iter_"+str(K),
			psf.array('income'+str(k),'income'+str(K)+"_q"))
	else:
		for k in range(1,K): 
			result2 = result2.withColumn("iter_"+str(k),
				psf.array('income'+str(k),'income'+str(k)+"_q"))

	# KEEP ONLY STACKED ARRAY
	result2 = result2.select(*['CodeId'] +  [k for k in result2.columns if k.startswith('iter_')])

	# result2.show(2)
	# +-----------+--------------------+--------------------+--------------------+
	# |    CodeId1|              iter_0|              iter_1|              iter_2|
	# +-----------+--------------------+--------------------+--------------------+
	# |33607577128|[17513.3333333333...|[12173.9999999999...|[17555.3333333333...|
	# |33637872511|[15662.3333333333...|[25214.3000000000...|     [20817.75, 5.0]|
	# +-----------+--------------------+--------------------+--------------------+
	# only showing top 2 rows	

	return result2


# ==============================================================
# 		 VIII - COMPUTE RANKS FOR EACH ITERATIONS
# ==============================================================


def rankPeople(spark,psw, psf,
	simulation4,
	impute_median = True, K=3, ties = "average"):
	"""
	Function to assign phone users rank given income simulation performed

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	psf: pyspark.sql.functions
	  Module passed as dependency
	psw: pyspark.sql.window
	  Module passed as dependency
	K: int
	  Number of iterations for simulation. Ignored if impute_median = False (default)
	impute_median: bool
	  Should we perform median imputation (True) or income simulation ?
	ties: str
	  "average" to use fractional ranking, "first" to use ordinal ranking

	return:
		SparkDataFrame with CodeId and rank for all iterations
	"""

	# IF ONLY ONE COLUMN (IMPUTING MEDIAN OR ONE SIMULATION), RANK AND EXIT
	# --------------------------------------------------------------------------

	if ((K==1)|(impute_median)):
		if K==1:
			w = psw.Window.orderBy("income")
		else:
			w = psw.Window.orderBy("mediane")
		if ties=='average':
			# Rank using psf.rank(): ties are averaged out
			simulation4 = (simulation4
				.withColumn("rank",psf.rank().over(w))
				.drop('income','mediane')
				)
		else:
			# Rank using psf.row_number(): ties are also ranked
			simulation4 = (simulation4
				.withColumn("rank",psf.row_number().over(w))
				.drop('income','mediane')
				)			
		return simulation4

	# simulation4.show(2)
	# +-----------+----+                                                              
	# |     CodeId|rank|
	# +-----------+----+
	# |33632871991|   1|
	# |33632856252|   1|
	# +-----------+----+
	# only showing top 2 rows

	# INITIALIZE LOOP
	# -----------------------------------------------------

	# Define window function for 'income0' variable
	w = psw.Window.orderBy("income0")
	if ties=='average':
		# Rank using psf.rank(): ties are averaged out
		simulation4bis = (simulation4
				.withColumn("rank_0",psf.rank().over(w))
				)
	else:
		# Rank using psf.row_number(): ties are also ranked
		simulation4bis = (simulation4
				.withColumn("rank_0",psf.row_number().over(w))
				)

	# ITERITIVELY RANK FOR EACH SIMULATION
	# -----------------------------------------------------

	for col_name in simulation4.columns[2:]:
		rkname = col_name.replace("income","rank_")
		w = psw.Window.orderBy(col_name)
		if ties=='average':
			# Rank using psf.rank(): ties are averaged out
			simulation4bis = (simulation4bis
				.withColumn(rkname,psf.rank().over(w))
				)
		else:
			# Rank using psf.row_number(): ties are also ranked
			simulation4bis = (simulation4bis
				.withColumn(rkname,psf.row_number().over(w))
				)

	# KEEP ONLY RANKS
	simulation4bis = simulation4bis.select(*['CodeId'] +  [k for k in simulation4bis.columns if k.startswith('rank_')])

	# simulation4bis.show(2)
	# +-----------+------+------+------+                                              
	# |     CodeId|rank_0|rank_1|rank_2|
	# +-----------+------+------+------+
	# |33633682044| 88213|     7|     1|
	# |33671640948|116754|271123|     2|
	# +-----------+------+------+------+
	# only showing top 2 rows

	return simulation4bis




def importProba(spark, pst, psf,
	useGrid = True, month = '09',
	city = 'marseille', read_path = "/User/vblx1269/WORK/lino/PROBAS PSI grid csv"):
	"""
	Import presence probabilities computed by preparePSI

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	pst: pyspark.sql.types
		Module passed as dependency
	psf: pyspark.sql.functions
		Module passed as dependency
	useGrid: bool
		Should we use grid (True) or voronoi (False)
	month: str
		Month
	city: str
		City under consideration
	read_path: str
		Path where probability should be found

	return:
		DataFrame of presence probability
	"""
	# CREATE SCHEMA
	schema = pst.StructType([
	    pst.StructField("CodeId", pst.StringType(), True),
	    pst.StructField("period", pst.StringType(), True),
	    pst.StructField("Prob", pst.DoubleType(), True)]
	    )
	# IMPORT DATA
	if useGrid:
		# With grid, data are structured in a unique file by 03b_GridProba.py file
		db5 = (spark.read.format('csv')
			.option("header","true").schema(schema)
			.load(read_path + "/"+month+"/"+city+".csv")
			)
	else:
		schema = pst.StructType([
	    pst.StructField("CodeId", pst.StringType(), True),
	    pst.StructField("NIDT", pst.StringType(), True),
	    pst.StructField("Prob", pst.DoubleType(), True)]
	    )
		# Data need to be imported with a loop when useGrid==False
		dayhour=0
		day='weekjob'
		db1 = (spark.read.format('csv')
			.option("header","true")
			.schema(schema)
			.load(read_path + "/"+month+"/"+city+"/"+ day+"/hour"+str(dayhour)+".csv")
			.withColumn('period',psf.lit(":" + str(day)+":"+ str(dayhour)))
			)
		# Import other hours and append in a loop
		for dayweek in ['weekjob','weekend']:
			print("Importing data for " +dayweek)
			if dayweek == 'weekjob':
				for hour in range(1,24):
					db_temp = (spark.read.format('csv')
						.option("header","true").schema(schema)
						.load(read_path + "/"+month+"/"+city+"/"+ dayweek+"/hour"+str(hour)+".csv")
						.withColumn('period',psf.lit(":" + str(dayweek)+":"+ str(hour)))
						)
					db1 = db1.union(db_temp)
			else:
				for hour in range(24):
					db_temp = (spark.read.format('csv')
						.option("header","true").schema(schema)
						.load(read_path+"/"+month+"/"+city+"/"+ dayweek+"/hour"+str(hour)+".csv")
						.withColumn('period',psf.lit(":" + str(dayweek)+":"+ str(hour)))
						)
					db1 = db1.union(db_temp)
		db5 = db1.withColumn("period", psf.concat(psf.col("NIDT"),
			psf.col("period"))).drop("NIDT")
	return db5


# ======================
# Filter individuals
# ======================

def filter_individuals(spark, pst, psf,
	city, useGrid,
	data, id_var = 'CodeId',
	HD_path = "/User/vblx1269/WORK/lino/_Home Detection Grid",
	month = '09'):
	"""
	Keep only people that live inside urban CLUSTER

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	psf: pyspark.sql.functions
		Module passed as dependency
	pst: pyspark.sql.types
		Module passed as dependency
	city: str
		City considered
	useGrid: bool
		Boolean indicating whether we should use grid (True) or voronoi (False)
	data: SparkDataFrame
		Data whose phone user list should be inspected
	id_var: Union[str, list, tuple]
		Column name(s) of columns that should be inspected
	HD_path: str
		Path where home detection csv is stored

	return: SparkDataFrame
	"""
	#
	#
	# Create list of city phone users
	# ---------------------------------------------
	#
	if useGrid:
		# Import grid list in city
		grid_paris = (spark.read.format('csv').option("header", "true")
			.load("/User/vblx1269/WORK/lino/_Quantiles by city grid/quantiles_" + city +".csv")
			.select('grid_id','`revenu.person_50`').withColumnRenamed('revenu.person_50',"x")
			)
		schema = pst.StructType([pst.StructField("CodeId", pst.StringType(), True),
			pst.StructField("month", pst.IntegerType(), True),
			pst.StructField("grid_id", pst.IntegerType(), True)])
		HD_paris = (spark.read.format('csv').option("header", "false").schema(schema)
			.load(HD_path + "/HD_grid_"+ month +".csv")
			.select('CodeId','grid_id')
			)
		indiv_paris = HD_paris.join(psf.broadcast(grid_paris), ['grid_id'])
		indiv_paris = (indiv_paris.filter(indiv_paris.x.isNotNull())
			.select('CodeId','grid_id')
			.withColumnRenamed("CodeId","CodeId_paris"))
	else:
		indiv_paris = (spark.read.format('csv').option("header", "true")
			.load("/User/vblx1269/WORK/lino/_Individuals by city/indiv_"+ month + "_" + city +".csv")
			.rdd.map(lambda l: (l[0],l[1]))
			)
		indiv_paris = (indiv_paris.toDF()
			.withColumnRenamed("_1","CodeId_paris").withColumnRenamed("_2","grid_id")
			) # NIDT are called grid_id to limit number of 'if'...'else'
	#
	#
	#
	# Filter data using that list
	# ---------------------------------------------
	#	
	if (isinstance(id_var, list)) | (isinstance(id_var, tuple)):
		# id_var is a list or tuple
		i = 0
		for nam in id_var:
			i +=1
			if (nam==id_var[0]):
				data2 = (data
					.join(
						psf.broadcast(indiv_paris.withColumnRenamed('CodeId_paris',nam)),
						[nam],
						'left_outer'
						)
					.withColumnRenamed("grid_id","grid_id" + str(i))
					)
			else:
				data2 = (data2
					.join(
						psf.broadcast(indiv_paris.withColumnRenamed('CodeId_paris',nam)),
						[nam],
						'left_outer'
						)
					.withColumnRenamed("grid_id","grid_id" + str(i))				
					)			
	elif isinstance(id_var,str):
		# Just one column
		data2 = (data
			.join(
				psf.broadcast(indiv_paris.withColumnRenamed('CodeId_paris',id_var)),
				[id_var],
				'left_outer'
				)
			)
	else:
		return None
	#
	# Remove empty cases
	data2 = data2.na.drop()
	# Keep only initial columns
	if isinstance(data.columns,list):
		data2 = data2.select(*data.columns)
	else:
		data2 = data2.select(data.columns)		
	# Return DataFrame
	return data2





# ===================
# RESHAPE DATA
# ===================

from pyspark.sql import DataFrame
from typing import Iterable 

def melt(df: DataFrame,
	id_vars: Iterable[str], value_vars: Iterable[str],
	var_name: str="variable", value_name: str="value",
	psf = psf):
    """
    Convert :class:`DataFrame` from wide to long format.
    """
    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = psf.array(*(
        psf.struct(psf.lit(c).alias(var_name), psf.col(c).alias(value_name)) 
        for c in value_vars))
    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", psf.explode(_vars_and_vals))
    cols = id_vars + [
            psf.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)



 # ============== OLD

def individualParis(l, list_paris):
	"""
	Match Code_Id given in l with a list of individuals living in a given city
	NB: Can be applied to any city, not only Paris

	:return: a tuple with None as third position if CodeId is
	not identified as living in the city
	
	Keyword arguments
	-----------------
	l -- Spark rdd with CodeId as l[2]
	list_paris -- Spark broadcasted data of CodeId of people living in the city
	"""
	val = list_paris.value.get(l[2])
	return (l[0],l[1],l[2],l[3],val)

# RETURN DAYSWEEK (between 0 and 6)
def day_week(l):
	"""
	Transform date vector d/m/Y in a new format 
	"""
	day,month,year = l.split('/')
	val = datetime.date(int(year),int(month),int(day))
	res = datetime.date.weekday(val)
	return res 
# Assumes date vector is %d/%m/%Y

def processingAggregate(data_cdr7, heure,weekend = True): ## dayweek prend des valeurs de 0 (Lundi) a 6 (Dimanche). Le weekday va de 0 a 4 et le weekend 5,6
	"""
	Keep calls belonging to weekend or weekday

	Parameters
	----------
	data_cdr7: Spark rdd
	heure: dayhour we want to keep
	weekend: Boolean indicating whether we look at weekends or weekdays
	"""
	if weekend:
		data = data_cdr7.filter(lambda l: l[2]>4).filter(lambda l:l[1]==heure)
	else:
		data = data_cdr7.filter(lambda l: l[2]<=4).filter(lambda l:l[1]==heure)		
	return data


def processing(data_cdr7, dayweek, heure): ## dayweek prend des valeurs de 0 (Lundi) a 6 (Dimanche). Le weekday va de 0 a 4 et le weekend 5,6
	"""
	Keep calls belonging for specific day in week at a given time

	Parameters
	----------
	data_cdr7: Spark rdd
	dayweek: dayhour we want to keep
	weekend: Boolean indicating whether we look at weekends or weekdays
	"""
	data = data_cdr7.filter(lambda l: l[2]==dayweek).filter(lambda l:l[1]==heure)
	return data


# FUNCTION TO MATCH ANTENNA LIST WITH DATA
def mapping(l, bv_paris):
	"""
	Map CodeId in l with list of individuals living in the city

	Parameters
	----------
	l: Spark rdd storing tuples
	bv_paris: broadcasted variable
	"""	
	val = bv_paris.value.get(l[3])
	return (l[0],l[1],l[2],l[3],val)


# COMPUTE PROBA: eq. (11) in Yang paper
def prob(l):
	"""
	Compute probability of presence to a given place
	"""
	val=[[k,l.count(k)/float(len(l))] for k in set(l)]
	return val	


# Return dataframe
def f(x): return x


def try_returnIncome(func,l, math, method):
	"""
	simulation3 = (simulation2.map(lambda l: (str(l[0]), l, try_returnIncome(returnIncome, l, math, method))))
	simulation3.filter(lambda l: l[2]==False).take(2)
	[('33682207160', ('33682207160', ['801.304347826087', '3600', '5619.09090909091', '7477.772093023257', '9448.51851851852', '11441.3333333333', '13776.2375', '15618.13333333332', '17359.53333
	3333347', '19279.249999999975', '21258.4', '23070.933333333363', '25424', '27602.6666666667', '29547.166666666664', '32206', '35642.6666666667', '40983.8461538461', '50916', '302813.33333333
	3'], 16.68650757754365, 19.039753423963692, 12.12685375200557), False), ('33608599414', ('33608599414', ['7661', '12447.333333333334', '13832.666666666666', '15429.333333333334', '16539.5', 
	'17061.666666666668', '17776.86141304348', '19179.285714285714', '21404.833333333332', '23840.434782608696', '25019.250000000004', '25592.173913043498', '27764.69230769231', '28386.600000000
	002', '30395', '35466', '37876.750000000015', '44884', '56222.41666666667', '161803'], 2.671200706842447, 19.415047569202834, 3.8817035433838876), False)]
	"""
	try:
		func(l, math, method)
		return True
	except:
		return False




def readLocationCDR(spark, pst, psf,
	month = '09', CDRpath = "/Data/O19593/SECURE/RAW/CSV/CRA_OFR/2007"):
	"""
	Read France level CDR and format them to get individuals locations

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	psf: pyspark.sql.functions
		Module passed as dependency
	pst: pyspark.sql.types
		Module passed as dependency
	month: str
		Month considered given as string of type '08' or '10'
	CDRpath: str
		Path where CDRs are stored

	return: SparkDataFrame	
	"""
	#
	#
	# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	# 					PART I: IMPORT CDR
	# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	#
	#
	# CDR SCHEMA
	schema = pst.StructType([
		    pst.StructField("CCR_ID", pst.IntegerType(), True),
		    pst.StructField("DT_HOROD", pst.TimestampType(), True),
		    pst.StructField("NDE_NU_CONCERNE", pst.StringType(), True),
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
	#
	# READ CDR
	data_cdr1 = (spark.read.format('csv').option("delimiter",";")
		.option("timestampFormat", "dd/MM/yyyy hh:mm:ss")
		.schema(schema)
		.load(CDRpath + "/"+month+"/*")
		)
	#
	#
	# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	# 					PART II: EXTRACT INFORMATION
	# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	#
	#
	# KEEP TIMESTAMP, ID, LAC, CI
	data_cdr2 = (data_cdr1
	.select('DT_HOROD','NDE_NU_CONCERNE','CO_ZLOC','CO_IDCELL')
	.withColumnRenamed('NDE_NU_CONCERNE','CodeId')
	)
	#
	# data_cdr2.show(2)
	# +--------------------+---------------+-------+---------+
	# |            DT_HOROD|NDE_NU_CONCERNE|CO_ZLOC|CO_IDCELL|
	# +--------------------+---------------+-------+---------+
	# |2007-09-01 19:20:...|3.3686015862E10|   null|     null|
	# |2007-09-01 21:35:...|3.3686015862E10|   9729|    10705|
	# +--------------------+---------------+-------+---------+
	# only showing top 2 rows
	#
	#
	# KEEP ONLY ROWS WHERE WE HAVE LAC AND CI
	# ------------------------------------------
	#
	data_cdr3 = (data_cdr2
		.where(
			(psf.col('CO_ZLOC').isNotNull()) & (psf.col('CO_IDCELL').isNotNull())
			)
		)
	#
	# KEEP ROWS WHERE CODEID IS CORRECT (eleven character long & starts by 33*)
	# ------------------------------------------------------------
	#
	data_cdr4 = (data_cdr3
		.filter(psf.length(psf.col('CodeId')) == 11)
		.filter(psf.col('CodeId').startswith('33'))
		)
	#
	# data_cdr4.show(2)
	# +--------------------+-----------+-------+---------+
	# |            DT_HOROD|     CodeId|CO_ZLOC|CO_IDCELL|
	# +--------------------+-----------+-------+---------+
	# |2007-09-01 21:35:...|33686015862|   9729|    10705|
	# |2007-09-01 22:32:...|33686015862|   9729|    10705|
	# +--------------------+-----------+-------+---------+
	# only showing top 2 rows
	# data_cdr4.count()
	# 3024884663 # 3 milliards d'obs. 
	#
	# KEEP ONE CALL BY HOUR BY ID BY ANTENNA
	# ------------------------------------------
	#
	data_cdr5 = (data_cdr4
		.withColumn('day',psf.col('DT_HOROD').cast(pst.DateType()))
		.withColumn('hour',psf.hour('DT_HOROD'))
		.withColumn('weekday', psf.date_format('DT_HOROD','u'))
		.drop('DT_HOROD')
		)
	# REDUCE NOISE BY KEEPING ONLY ONE CALL BY LOCATION BY TIME SLOT
	data_cdr5 = data_cdr5.distinct()
	# 
	# data_cdr5.count()
	# 1907446527	
	return data_cdr5




def getCoordinatesCDR(spark, psf, pst, dataCDR, time_var = 'period',
	XYpath = "/User/vblx1269/WORK/hospice"):
	"""
	Join CDR data with WSG84 coordinates

	Parameters
	----------
	spark: SparkSession
		SparkSession that should be passed as a dependency
	psf: pyspark.sql.functions
		Module passed as dependency
	pst: pyspark.sql.types
		Module passed as dependency
	data: SparkDataFrame
		CDRs at individual level
	time_var: str
		Time variable
	XYpath: str
		Path where antennas coordinates are stored

	return: SparkDataFrame	
	"""
	#
	# ++++++++++++++++++++++++++++++++++++++++++++++
	# 		PART I: COUNT DATA BY TIME SLOT
	# ++++++++++++++++++++++++++++++++++++++++++++++
	#
	# SELECT ONLY RELEVENT DATA
	data_cdr5 = dataCDR.select(*('CodeId','CO_ZLOC','CO_IDCELL',time_var))
	#
	#
	# data_cdr5.show(2)
	# +-----------+-------+---------+----------+----+
	# |     CodeId|CO_ZLOC|CO_IDCELL|       day|hour|
	# +-----------+-------+---------+----------+----+
	# |33686015862|   9729|    10705|2007-09-01|  21|
	# |33686015862|   9729|    10705|2007-09-01|  22|
	# +-----------+-------+---------+----------+----+
	# only showing top 2 rows
	#
	#
	#
	# PREPARE MERGING TO ANTENNAS COORDINATES
	data_cdr6 = data_cdr5.withColumn('lac_ci',psf.concat(psf.col("CO_ZLOC"),psf.col("CO_IDCELL")))
	# COUNT EVENTS BY LOCATION FOR ALL USERS
	data_cdr7 = data_cdr6.groupBy('CodeId','lac_ci',time_var).count()
	#
	#
	# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	# 			PART II: TRANSFORM IN COORDINATES DATA
	# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	#
	# LIST ANTENNAS
	# --------------------
	#
	## antennas_paris.csv, antennas_lyon.csv, antennas_marseille.csv dans mon espace HUE
	xy_bts = (spark.read.format('csv').option("header", "true").option("delimiter", "\t")
		.schema(
			pst.StructType([
				pst.StructField("NIDT", pst.StringType(), True),
				pst.StructField("ci", pst.StringType(), True),
				pst.StructField("lac", pst.StringType(), True),
				pst.StructField("lon", pst.DoubleType(), True),
				pst.StructField("lat", pst.DoubleType(), True),
				pst.StructField("x", pst.DoubleType(), True),
				pst.StructField("y", pst.DoubleType(), True),			
				])
			)
		.load(XYpath + "/NIDT_CILAC_XY.csv")
		)
	#
	xy_bts = (xy_bts
		.withColumn("lac_ci", psf.concat(psf.col("lac"),psf.col("ci")))
		.drop("lac","ci","x","y", "NIDT") #Don't need Lambert 93 coordinates (x,y), WSG84 enough
		)
	#
	#
	# xy_bts.show(2)
	# +------+-------+----------+
	# |   lon|    lat|    lac_ci|
	# +------+-------+----------+
	# 2.81948|49.9694|3276853571|
	# 2.81948|49.9694|3276862542|
	# -------+-------+----------+
	# only showing top 2 rows
	#
	#
	# MERGE ANTENNAS LIST
	# -----------------------------
	#
	data_cdr8 = data_cdr7.join(psf.broadcast(xy_bts),
		['lac_ci'],'left_outer')
	#
	#
	# data_cdr8.show(2)
	# +----------+-----------+-------+-----+--------+-------+                         
	# |    lac_ci|     CodeId| period|count|     lon|    lat|
	# +----------+-----------+-------+-----+--------+-------+
	# |1869157224|33689512861|weekend|    1|0.897654|47.3326|
	# |2125257439|33685634374|weekend|    1| 5.55859|47.3944|
	# +----------+-----------+-------+-----+--------+-------+
	# only showing top 2 rows
	#
	#
	# KEEP NON MISSING LOCATIONS AND TIME
	data_cdr8 = data_cdr8.na.drop(subset=["lon","lat","period"])
	return data_cdr8



def haversine_python(math, lon1, lat1, lon2, lat2, unit = 'radians'):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    This is a variant from the great_circle_distance available in .helpers.maths

	Parameters
	----------
	math: module
		Python math module
	lon1: float
		Longitude 1
	lat1: float
		Latitude 1
	lon2: float
		Longitude 2
	lat2: float
		Latitude 2
	unit: Measurement unit. If needed, conversion into radians is performed

	return: Great circle distance in kilometers
    """
    # convert decimal degrees to radians
    if unit != 'radians':
    	lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def haversine_spark(spark, psf,
	data,
	lon1_var = "lon", lon2_var = "lon_bar", lat1_var = "lat", lat2_var = "lat_bar",
	unit = 'radians'):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	This is an optimized Spark variant from the great_circle_distance available
	in Python module .helpers.maths

	Parameters
	----------
	math: module
		Python math module
	lon1_var: str
		Longitude 1 variable name
	lon2_var: str
		Longitude 2 variable name
	lat1_var: str
		Latitude 1 variable name
	lat2_var: str
		Latitude 2 variable name
	unit: Measurement unit. If needed, conversion into radians is performed

	return: SparkDataFrame with great circle distance in kilometers into 'haversine' column
	"""
	#
	# CONVERT INTO RADIANS IF NEEDED
	# ---------------------------------
	# if unit != 'radians':
	# 	data = (data
	# 		.withColumn(lon1_var, psf.radians(lon1_var))
	# 		.withColumn(lon2_var, psf.radians(lon2_var))
	# 		.withColumn(lat1_var, psf.radians(lat1_var))
	# 		.withColumn(lat2_var, psf.radians(lat2_var))
	# 		)
	#
	# HAVERSINE FORMULA FOR GREAT CIRCLE DISTANCE
	# ----------------------------------------------
	data_haversine = (data
		.withColumn('dlon', psf.col(lon1_var) - psf.col(lon2_var))
		.withColumn('dlat', psf.col(lat1_var) - psf.col(lat2_var))
		.withColumn('a',
			psf.pow(psf.sin(psf.col('dlat')/2),2)
			+ psf.cos(psf.col('lat'))*psf.cos(psf.col('lat_bar'))
			* psf.pow(psf.sin(psf.col('dlon')/2),2)
			)
		.withColumn("haversine",
			2*6371*psf.asin(
				psf.sqrt(psf.col("a"))
				))  # Radius of earth in kilometers. Use 3956 for miles
		)
	return data_haversine







def simulateClass(spark, sc, pst, psf, pmr, math,
	city = 'marseille',
	month = '09',
	K = 3,
	read_path = "/User/vblx1269/WORK/lino/PROBAS PSI grid csv",
	write_path = "/User/vblx1269/WORK/lino/Results PSI",
	statGrid_path = "/User/vblx1269/WORK/lino/_Stats grid/500m",
	HD_path = "/User/vblx1269/WORK/lino/_Home Detection Grid",
	frequency_path = "/User/vblx1269/WORK/lino/Filosofi"):

	# =======================================
	# PART I: READ ALL NECESSARY INPUTS
	# =======================================	
	
	# READ FREQUENCY COMPUTED FROM FILOSOFI
	grid_paris = (spark.read.format('csv').option("header", "true")
	.load(frequency_path + "/highIncome_" + city +".csv")
	)

	# READ HOME DETECTION OUTPUT
	schema2 = pst.StructType([
				pst.StructField("CodeId", pst.StringType(), True),
				pst.StructField("month", pst.StringType(), True),
				pst.StructField("grid_id", pst.StringType(), True)]
		    )
	HD_paris = (spark.read.format('csv').option("header", "false")
			.schema(schema2)
			.load(HD_path + "/HD_grid_"+ month +".csv")
			.select('CodeId','grid_id')
	)

	# JOIN DATA
	df_rank = HD_paris.join(psf.broadcast(grid_paris), ['grid_id'], 'left_outer')
	df_rank = df_rank.filter(psf.col("highIncome").isNotNull())


	# =======================================
	# PART II: BINOMIAL SIMULATION
	# =======================================	

	# UNIFORM SIMULATIONS
	total_user = df_rank.count()
	simulation = pmr.RandomRDDs.uniformVectorRDD(sc, total_user, K)

	# MERGE SIMULATIONS AND CODEID
	df_rank2=df_rank.rdd.zipWithIndex().map(lambda l: (l[1],l[0]))
	simulation=simulation.zipWithIndex().map(lambda l: (l[1],l[0]))
	simulation2=df_rank2.join(simulation).map(lambda l: l[1])

	# TRANSFORM UNIFORM TO BINOMIAL
	simulation3= (simulation2
		.map(lambda l: (l[0]['CodeId'],
		[int(s<float(l[0]['highIncome'])) for s in l[1]]))
	)
	simulation3 = simulation3.map(lambda l: flat_tuple(l))

	# RENAME COLUMNS
	simulation = spark.createDataFrame(simulation3,
		['CodeId'] + ['simulation_' + str(k) for k in range(K)])

	# simulation.show(5)
	# +-----------+------------+------------+------------+
	# |     CodeId|simulation_0|simulation_1|simulation_2|
	# +-----------+------------+------------+------------+
	# |33678243422|           1|           0|           0|
	# |33682508819|           1|           1|           1|
	# |33677542015|           1|           1|           1|
	# |33674510762|           1|           1|           1|
	# |33689396941|           1|           1|           1|
	# +-----------+------------+------------+------------+
	# only showing top 5 rows

	return(simulation)



def write_program(
	write_path = "/User/vblx1269/WORK/lino",
	files = ['db1.csv','db2.shp'],
	scripts = ['/User/vblx1269/WORK/lino/Functions/CommonFunctions.py',
	'/User/vblx1269/WORK/lino/Functions/computeSSI.py']):
	#
	#
	import datetime
	import time
	#
	#
	header = """
	====================================================
				OUTPUT GENERATING PROGRAM
	====================================================
	This program has been automatically created to keep information
	on database versions and the underlying programs that generated them


	Timestamp:
	----------------
	%s


	Databases:
	----------------
	%s	


	Programs:
	----------------
	%s



	THE PROGRAMS ARE APPENDED BELOW:
	"""  % (datetime.datetime.now(), ";".join(files), ";".join(scripts))

	def read_program(file):
		val = sc.textFile(file).collect()
		return "\t".join(val)

	hd = """
	-----------------------
		Program: %s
	-----------------------

	""" % ("\t \t".join([read_program(s) for s in scripts]))


	fileinfo = header + "\t \t \t " + hd
	
	text_file = open("hdfs://" + write_path + "/" + time.strftime("%Y_%m_%d") + ".txt", "w")
	text_file.write("Purchase Amount: %s" % TotalAmount)
	text_file.close()




	