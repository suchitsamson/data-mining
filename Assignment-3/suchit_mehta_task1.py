from pyspark import SparkContext
from pyspark import SparkConf
from itertools import combinations
from collections import defaultdict
import sys

conf = SparkConf()
conf.setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
sc = SparkContext(conf = conf)

filePathCSV = sys.argv[1]
filePathOutput = sys.argv[2]

data = sc.textFile(filePathCSV)
first = data.first()

read=data.filter(lambda line : line != first).map(lambda line: (line.split(',')[0], line.split(',')[1])).distinct()
csvRDD = read.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y)
jaccardRDD=dict(read.map(lambda x: (x[1],[x[0]])).reduceByKey(lambda x,y: x+y).collect())

totalHashFunctions=100
signatureMatrix = defaultdict(list)
hashFunc=list()
userMap=dict()
userCount=1

for x in csvRDD.sortByKey().collect():
    userMap[userCount]=[x[0],x[1]]
    userCount+=1
    for i in range(0,len(x[1])):
        if not signatureMatrix[x[1][i]]:
            signatureMatrix[x[1][i]]=[sys.maxsize]*totalHashFunctions

#for i in range(totalHashFunctions):
#    hashFunc.append((random.randint(1,userCount),random.randint(1,userCount)))
#print(hashFunc)
hashFunc=[(4451, 4738), (7002, 6902), (585, 3097), (3542, 10048), (11235, 6147), (3815, 4606), (2035, 8970), (8617, 11056), (7510, 4982), (1809, 7515), (10147, 2325), (2914, 5689), (2826, 2480), (5151, 5501), (6864, 2389), (2865, 2312), (1184, 10168), (4009, 5611), (9719, 9002), (548, 11232), (631, 10617), (5953, 107), (2476, 3211), (7760, 9688), (7346, 4287), (6461, 4757), (212, 6596), (2989, 2838), (3423, 3875), (11056, 10760), (6068, 4447), (1586, 5937), (273, 4751), (4800, 973), (10004, 1463), (10899, 4446), (4624, 7884), (8377, 8524), (9222, 9559), (9884, 1761), (3347, 83), (9753, 10611), (8443, 6022), (5170, 7696), (2915, 2944), (1794, 8269), (3318, 3066), (5980, 9172), (1324, 1304), (2644, 2475), (2362, 8245), (9900, 10659), (9349, 1145), (8534, 10493), (10769, 4364), (206, 7169), (7783, 7274), (2154, 10776), (1412, 6276), (7716, 9473), (5367, 3286), (4746, 4441), (5560, 9876), (6803, 1896), (5972, 4871), (6810, 2419), (6736, 9796), (5284, 2419), (10106, 10719), (9483, 9459), (10736, 1517), (476, 10703), (10650, 1005), (3603, 7612), (4751, 7577), (708, 9529), (3951, 8566), (1652, 4597), (10910, 9972), (6532, 4992), (8888, 10606), (4303, 8556), (4072, 2118), (8790, 4327), (8928, 1695), (3945, 5450), (10469, 4264), (2562, 1688), (7383, 4627), (10823, 104), (6316, 10125), (3534, 4651), (3939, 10413), (3473, 8239), (5812, 7588), (1723, 3074), (2270, 10033), (5544, 5515), (7766, 889), (5857, 6735)]

for i in range(1,len(userMap)+1):
    businessList=userMap[i][1]
    for business in businessList:
        for j in range(0,len(hashFunc)):
            a=hashFunc[j][0]
            b=hashFunc[j][1]
            v=(a*i+b)%userCount
            if v< signatureMatrix[business][j]:
                signatureMatrix[business][j]=v

bands=35
rows=int(totalHashFunctions/bands)
start=0
candidates=set()
while start<len(hashFunc):
    d=defaultdict(list)
    for k,v in signatureMatrix.items():
        key=str(v[start:start+rows])
        if d[key]:
            d[key]+=[k]
        else:
            d[key] = [k]
    start+=rows
    for k,v in d.items():
        if len(v)>1:
            candidates.add(tuple(sorted(v)))

#find jaccard similarity from candidates
count=0
answer=defaultdict(int)
for x in candidates:
    combi=list(combinations(x,2))
    for i in combi:
        a=i[0]
        b=i[1]
        similarity = len(set(jaccardRDD[a]).intersection(set(jaccardRDD[b])))/len(set(jaccardRDD[a]).union(set(jaccardRDD[b])))
        if similarity>=0.5:
            if answer[a+","+b]:
                answer[a + "," + b]=similarity
            else:
                answer[a + "," + b] = similarity
                count+=1

s="business_id_1, business_id_2, similarity\n"
for x in sorted(answer.keys()):
    s+=x+","+str(answer[x])+"\n"

outputFile = open(filePathOutput, "w")
outputFile.write(s)
