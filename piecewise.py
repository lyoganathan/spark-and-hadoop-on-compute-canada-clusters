#spark is sqlsession and sc is spark context which is setup automatically
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Using External Library")
sc = SparkContext(conf=conf)
#sc.addPyFile("dependencies.zip")
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import PolynomialExpansion
from pyspark.ml.regression import LinearRegression
import matplotlib
matplotlib.use('Agg') #Don't need Xserver
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


#conf = pyspark.SparkConf()
#conf.setAppName('Interpolation')
#conf.setMaster('local[*]')
#sc = pyspark.SparkContext(conf=conf)

#Load sqlContext
#spark = pyspark.sql.SparkSession.builder \
#        .master("local[*]") \
#        .appName("Interpolation") \
#        .getOrCreate()

spark = pyspark.sql.SparkSession.builder\
        .appName("Interpolation")\
        .getOrCreate()

T1_df2=spark.read.csv("T1_Lookup.csv", header = True, inferSchema=True) # faster than converting pandas DF to sparkDF
T1_df2 = T1_df2.repartition(10000)
knotRatio = np.linspace(0.00003,2.7,1000)
knotB1 = np.linspace(0.1,2.0,1000)

T1_df3 = T1_df2.select('*',F.array([F.when(T1_df2.ratio > c, T1_df2.ratio - c).otherwise(0) for c in knotRatio]).alias("RatioKnots"))
T1_df4 = T1_df3.select('*',F.array([F.when(T1_df2.B1err > c, T1_df2.B1err - c).otherwise(0) for c in knotB1]).alias("B1Knots"))

#Convert arrays to vectors: credit https://stackoverflow.com/questions/42138482/pyspark-how-do-i-convert-an-array-i-e-list-column-to-vector
list_to_vec = udf(lambda x: Vectors.dense(x),VectorUDT())


df_with_vectors = T1_df4.select('B1err','ratio','T1', list_to_vec(T1_df4["B1Knots"]).alias("B1Knots"), list_to_vec(T1_df4["RatioKnots"]).alias("RatioKnots"))

vec = VectorAssembler(inputCols=["B1err","ratio","B1Knots","RatioKnots"],outputCol="features")

T1_df5 = vec.transform(df_with_vectors)

#Polynomial Exapnsion with interactions

polyExpansion = PolynomialExpansion(degree=2, inputCol="features", outputCol="Interaction")
polyDF = polyExpansion.transform(T1_df5)

#Regression Time!
lr = LinearRegression(labelCol="T1",featuresCol="Interaction")
model = lr.fit(polyDF)

#Now we want to interpolate data onto 100*100 grid:
x1=np.linspace(0.1,2,100) #B1err
x2=np.linspace(0.0005,2.5,100) #Ratio
x1_2 = np.zeros([100,100])
x2_2 = np.zeros([100,100])
for i in range(0,len(x1)):
    for j in range(0,len(x2)):
        x1_2[i, j] = x1[i]
        x2_2[i, j] = x2[j]


x1 = x1_2.flatten()
x2 = x2_2.flatten()


a_df = pd.DataFrame([x1,x2], index=["B1err","ratio"])
a_df = a_df.T #2 rows of 10000 (100 B1 * 100 ratio values)

#Feature vectors have to be piecewise
a_df2 = spark.createDataFrame(a_df)

a_df3 = a_df2.select('*',F.array([F.when(a_df2.B1err > c, a_df2.B1err - c).otherwise(0) for c in knotB1]).alias("B1Knots"))
a_df4 = a_df3.select('*',F.array([F.when(a_df2.ratio > c, a_df2.ratio - c).otherwise(0) for c in knotRatio]).alias("RatioKnots"))

#Using our wonderful list_to_vec
a_df5 = a_df4.select('B1err','ratio', list_to_vec(a_df4["B1Knots"]).alias("B1Knots"), list_to_vec(a_df4["RatioKnots"]).alias("RatioKnots"))
vec = VectorAssembler(inputCols=["B1err","ratio","B1Knots","RatioKnots"],outputCol="features")
a_df6 = vec.transform(a_df5)
a_polyDF = polyExpansion.transform(a_df6)
result = model.transform(a_polyDF)

#Back to pandas for plotting:
dd = result.toPandas()

#Plot Predictions and save:
fig = plt.figure()
ax = fig.gca(projection='3d')
ax.plot_trisurf(dd["B1err"].values,dd["ratio"].values,dd["prediction"].values, cmap=plt.cm.jet, linewidth=0.2, antialiased=True)
ax.set_xlabel('B1')
ax.set_ylabel('Ratio')
ax.set_zlabel('T1')
ax.view_init(azim=210)
fig.savefig("yo.png")
