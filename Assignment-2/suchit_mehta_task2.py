from pyspark import SparkContext
from itertools import combinations
from collections import defaultdict
import time
import sys

start = time.time()

sc = SparkContext("local[*]","suchit_mehta_task2")

k = int(sys.argv[1])
s = int(sys.argv[2])
filePathCSV = sys.argv[3]
filePathOutput = sys.argv[4]

data = sc.textFile(filePathCSV)
first = data.first()
csvRDD = data.filter(lambda line : line != first).map(lambda line: (line.split(',')[0], line.split(',')[1])).map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).filter(lambda x : len(x[1]) > k)
totalBaskets = csvRDD.count()

def findFrequent(iterator):

    for item in iterator:
        for key in frequentItems:
            if isinstance(key,str) and set([key]).issubset(set(item[1])):
                frequentItems[key] += 1
            elif set(key).issubset(set(item[1])):
                frequentItems[key] += 1

    return ((i, frequentItems[i]) for i in frequentItems)

def findCandidates(itr):

    permuteDict = {}
    sets = set()
    count=0
    for item in itr:
        sets.add(tuple(item[1]))
        count+=1
        for i in set(item[1]):
            if i in permuteDict:
                permuteDict[i] += 1
            else:
                permuteDict[i] = 1
    t=s*(float(count)/float(totalBaskets))
    candidates={k for k, v in permuteDict.items() if v >= t}
    current = {k for k  in candidates}
    size = 2
    while (current):

        combination = set()
        temp=list(combinations(current,2))
        for z in temp:
            x=z[0]
            y=z[1]
            if size < 3:
                x1 = set([x])
                y1 = set([y])
            else:
                x1 = set(x)
                y1 = set(y)

            if len(x1.difference(y1)) == 1:
                union1 = sorted(x1.union(y1))
                if size > 2:
                    combiList = list(combinations(union1, size - 1))
                    if set(combiList).issubset(current):
                        combination.add(tuple(union1))
                else:
                    combination.add(tuple(union1))

        permuteDict1 = {}
        if size>2:
            for value in sets:
                for item in combination:
                    if set(item).issubset(value):
                        if item in permuteDict1:
                            permuteDict1[item] += 1
                        else:
                            permuteDict1[item] = 1

        else:
            for v in sets:
                pairs=list(combinations(sorted(set(v)),2))
                for p in pairs:
                    if p in combination:
                        if p in permuteDict1:
                            permuteDict1[p] += 1
                        else:
                            permuteDict1[p] = 1

        current = set()
        for k, v in permuteDict1.items():
            if v >= t:
                current.add(k)

        candidates=candidates.union(current)
        size += 1

    return candidates

def writeFile(keys, ctr, of):

    singleton=set()
    for x in keys:
        if isinstance(x, str):
            singleton.add(x)
    for i in singleton:
        keys.remove(i)

    orderDict = defaultdict(list)
    orderDict[1] = singleton
    for x in sorted(keys):
        if orderDict[len(x)]:
            orderDict[len(x)]+=x
        else:
            orderDict[len(x)]=x

    finalString=""
    for i in range(1,len(orderDict)+1):
        tup = orderDict[i]
        ss=""
        if i ==1:
            temp=str(sorted(tup))
            temp=temp.replace(' \'','(\'')
            temp=temp.replace('\',', '\'),')
            temp=temp.replace('[\'', '(\'')
            temp=temp.replace('\']', '\'),')
            ss+=temp[0:len(temp)-1]+"\n"
        else:
            for j in range(0,len(tup)):
                if j%i==0:
                    ss+="(\'"+str(tup[j])+"\', \'"
                elif j%i == i-1:
                    ss += str(tup[j])+"\'),"
                else:
                    ss += str(tup[j]) + "\', \'"

        finalString+=ss[0:len(ss)-1]+"\n\n"

    if ctr == 1:
        of.write("Candidates:\n"+finalString)
    if ctr == 2:
        of.write("Frequent Itemsets:\n" + finalString)

candidateKeys = csvRDD.mapPartitions(findCandidates).distinct().collect()
frequentItems=defaultdict(int)
for x in candidateKeys:
    frequentItems[x] = 0

frequent = csvRDD.mapPartitions(findFrequent).reduceByKey(lambda a,b:a+b).collect()
frequentKeys = set()
for x in frequent:
    if x[1]>=s:
        frequentKeys.add(x[0])

outputFile = open(filePathOutput, "w")
writeFile(candidateKeys, 1, outputFile)
writeFile(frequentKeys, 2, outputFile)

end = time.time()
print("Duration: "+str(end-start))
