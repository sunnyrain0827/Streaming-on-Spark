import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
#sc.stop()
sc = SparkContext()
input = sc.textFile('/Users/rainsunny/Desktop/epa-http.txt').map(lambda x: (x.replace('.',' ').split(' ')[0], x.replace('.',' ').split(' ')[1], x.replace('.',' ').split(' ')[2], x.replace('.',' ').split(' ')[len(x.replace('.',' ').split(' ')) - 1]))
#Remove the '-' line
input = input.filter(lambda x: x[3] != '-')
#Do the aggregation for IP address of digits
input_1 = input.filter(lambda x: x[0].isdigit() == True and x[1].isdigit() == True and x[2].isdigit() == True)
#First 2 elements are aggregated
input_1 = input_1.map(lambda x: (str(x[0]) + '.' + str(x[1]) + '.' + str(x[2]), x[3]))

input_1 = input_1.reduceByKey(lambda x, y: x + y)

#Do the aggregation for IP address of non-digits
input_2 = input.filter(lambda x: x[0].isdigit() == False or x[1].isdigit() == False or x[2].isdigit() == False)
#First three elements are aggregated
input_2 = input_2.map(lambda x: (str(x[0]) + '.' + str(x[1]), x[3]))
#summarization
input_2 = input_2.reduceByKey(lambda x, y: x + y)
print(data2.take(25))