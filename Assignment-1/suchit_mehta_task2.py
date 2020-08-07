from pyspark import SparkContext
import sys
import json
import time

sc = SparkContext("local[*]","suchit_mehta_task2")

filePathReview = sys.argv[1]
filePathBusiness = sys.argv[2]
filePathOutputT1 = sys.argv[3]
filePathOutputT2 = sys.argv[4]

reviewRDD = sc.textFile(filePathReview).map(json.loads)
businessRDD = sc.textFile(filePathBusiness).map(json.loads)

statesRDD = businessRDD.map(lambda x: (x['business_id'], x['state']))
starsRDD = reviewRDD.map(lambda x: (x['business_id'], int(x['stars'])))

stateAndStarsRDD = statesRDD.join(starsRDD)
avgStarsPerState = stateAndStarsRDD.map(lambda x: (x[1][0], x[1][1])).groupByKey().mapValues(lambda a: sum(a)/len(a))
avgStarsPerStateOrder = avgStarsPerState.sortBy(lambda a: (-a[1],a[0]))

outputFile = open(filePathOutputT1, "w")
outputString = "state,stars\n"
for i in avgStarsPerStateOrder.collect():
    outputString+=str(i[0])+","+str(i[1])+"\n"
outputFile.write(outputString)

outputFile2 = open(filePathOutputT2, "w")
m1_start = time.time()
ctr=0
for i in avgStarsPerStateOrder.collect():
    print(i)
    ctr=ctr+1
    if(ctr==5):
        break
m1_end = time.time()
for i in avgStarsPerStateOrder.take(5):
    print(i)
m2_end = time.time()
json.dump({"m1": str(m1_end-m1_start), "m2": str(m2_end-m1_end), "explanation": "m1>m2 because for m1 additional time is required to collect all the items while for m2 only mandatory(5 items) are collected. Hence time is saved"},outputFile2,indent=2)
