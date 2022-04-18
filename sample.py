import os
import sys
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark import sql
from operator import add
#import pyspark.sql
#from pyspark.ml.regression import LinearRegression




conf = SparkConf()
conf.setAppName("epl_trial_v2")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)



data = sc.textFile("/user/asr658/proj/epl.txt")

a = sc.accumulator(0)

values = data.collect()
goals = [""]
for i in range(len(values)):
    if(i != 0):
        temp = values[i].split(",")
        a += int(temp[4])
        den = float(temp[5])
        num = float(temp[4])
        if(den != 0):
            ratio = str((float(temp[4])/(float(temp[5]))))
            goals.append(temp[4] + ", " + temp[5] + ", " + ratio)
        
        if(den == 0):
            ratio = str(-1.0)
            goals.append(temp[4] + ", " + temp[5] + ", " + ratio)
        
        
        

homeGoals = sc.parallelize(goals)


homeGoals.saveAsTextFile("/user/asr658/proj/new")

focusedData = sc.textFile("/user/asr658/proj/new/*")

samp = focusedData.sample(True, .20, 201)

samp.saveAsTextFile("/user/asr658/proj/goalSample")
