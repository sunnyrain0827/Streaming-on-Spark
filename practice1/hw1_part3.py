#part3
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
sc = SparkContext()
#remove ':' to reach the value og hour
input = sc.textFile("epa-http.txt").map(lambda x: (x.replace(':', ' ').split(' ')[0], x.replace(':', ' ').split(' ')[1], x.replace(':', ' ').split(' ')[2], x.replace(':', ' ').split(' ')[len(x.replace(':', ' ').split(' ')) - 1]))
#Remove '-' line
input = input.filter(lambda x: x[3] != '-')

#we compute the summary of bytes in the hour of 6
#the time stamp [DD:HH:MM:SS]
rdd = input.filter(lambda x: x[1] == '[29' and x[2] == '23')
rdd = rdd.map(lambda x: (x[0], x[3]))
#Do the summary job
rdd = rdd.reduceByKey(lambda x, y: x + y)
print(rdd.collect())
rdd = input.filter(lambda x: x[1] == '[30' and x[2] == '23')
for i in range(1, 23):
    string = '0' + str(i)
    print('day 30' string)
    rdd = input.filter(lambda x: x[2] == string)
    rdd = rdd.map(lambda x: (x[0], x[3]))
    rdd = rdd.reduceByKey(lambda x, y: x + y)
    print(rdd.collect())
   

