#hw1 part1
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
sc = SparkContext()
input = sc.textFile("/Users/rainsunny/Desktop/epa-http.txt")
#Remove '-' line in bytes
lines_filt = input.filter(lambda x:x[-1] !='-')
#Select the first column and the last column: ip address, bytes
pairlines = lines_filt.map(lambda x: (x.split(" ")[0], int(x.split(" ")[-1])))
res = pairlines.reduceByKey(lambda x,y: x+y)
#Print first 25 
print(res.take(25))