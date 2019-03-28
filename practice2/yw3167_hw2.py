import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark import SparkConf, SparkContext
from datetime import datetime
import matplotlib.pyplot as plt 
conf = SparkConf("local").setAppName("myApp") 
sc = SparkContext(conf = conf)
input=sc.textFile("Book2.txt")
input_map = input.map(lambda x:int(x))
X=[0 for a in range(100)]
Y=[0 for a in range(100)]
X_reorder=[0 for a in range(100)]
Y_reorder=[0 for a in range(100)]
i=1

while i<=100:
    s=float(i)/100    
    a1=datetime.now()
    a_operator = input_map.filter(lambda x: x % 2==1)
    b1=datetime.now()
    #microseconds
    micro1=0.000001*(b1-a1).microseconds
    #seconds
    second1=(b1-a1).seconds
    #overall time passing A 1
    cost_a=second1+micro1
    
    #Operator B 2
    a2=datetime.now()
    b_operator = a_operator.filter(lambda x:x<=i)
    b2=datetime.now()
    #microseconds passing B
    micro2=0.000001*(b2-a2).microseconds
    #seconds passing B
    second2=(b2-a2).seconds
    #overall time passing B
    cost_b=second2+micro2
    #throughput of A
    throu_AB = 1/(cost_a+cost_b*0.5)
    X[i-1]=s
    Y[i-1]=1 # We directly set this throughput to 1
    
    #reordering
    #operator B first
    a1_reorder=datetime.now()
    a_operator_reorder = input_map.filter(lambda x: x<=ix)
    b1_reorder=datetime.now()
    micro1_reorder=0.000001*(b1_reorder-a1_reorder).microseconds
    second1_reorder=(b1_reorder-a1_reorder).seconds
    cost_a_reorder=second1_reorder+micro1_reorder
    #operator A then
    a2_reorder=datetime.now()
    b_operator = a_operator_reorder.filter(lambda x: x % 2==1)
    b2_reorder=datetime.now()
    micro2_reorder=0.000001*(b2_reorder-a2_reorder).microseconds
    second2_reorder=(b2_reorder-a2_reorder).seconds
    cost_b_reorder=second2_reorder+micro2_reorder
    #reorder throughput
    throu_BA = 1/(cost_b_reorder*s+cost_a_reorder)
    X_reorder[i-1]=s
    Y_reorder[i-1]=throu_BA/throu_AB #normalization this throughput with T(A to B)=1
    i=i+1

plt.figure()  
plt.plot(X,Y,'b',X_reorder,Y_reorder,'g')   
plt.ylabel('Throughput')
plt.xlabel('Selectivity of B')  
plt.axis([0, 1, 0, 2])
plt.show()