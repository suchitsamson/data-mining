from pyspark import SparkContext
from collections import defaultdict
from pyspark.mllib.recommendation import ALS
import math
import sys

sc = SparkContext("local[*]", "suchit_mehta_task2")

trainCSV = sys.argv[1]
valCSV = sys.argv[2]
case = int(sys.argv[3])
filePathOutput = sys.argv[4]

def predict(user):

    activeUserBusinessDict = defaultdict(float)

    if user[0][0] in userBusinessDict:
        businessListActiveUser = userBusinessDict[user[0][0]]
    else:
        businessListActiveUser = []

    activeUserBusinessDict=dict(businessListActiveUser)

    if user[0][0] not in userRatingSum:
        userRatingSum[user[0][0]] = sum(activeUserBusinessDict.values())

    businessListActiveUserSet = set(activeUserBusinessDict.keys())

    ctr = -1
    if len(businessListActiveUser) > 0:
        activeUserAvg = userRatingSum[user[0][0]] / len(businessListActiveUser)
    else:
        # activeUserAvg should be avg of the predictedBusinesses
        activeUserAvg = 0.0
        ctr = 0

    predictionNumerator = 0.0
    pearsonModSum = 0.0
    if user[0][1] in businessUserDict:
        for userRatedPredictionBusiness in businessUserDict[user[0][1]]:
            # all users who have rated the prediction business
            key = (str(user[0][0]) + ',' + str(userRatedPredictionBusiness[0]),
                   str(userRatedPredictionBusiness[0]) + ',' + str(user[0][0]))[
                userRatedPredictionBusiness[0] < user[0][0]]

            currentUserBusinessDict=dict(userBusinessDict[userRatedPredictionBusiness[0]])
            currentUserBusinessSet = set(currentUserBusinessDict.keys())
            currentUserSum = sum(currentUserBusinessDict.values())

            if len(currentUserBusinessDict)>1:
                currentUserAvg = (currentUserSum - float(userRatedPredictionBusiness[1])) / (len(currentUserBusinessDict) - 1)
            if len(currentUserBusinessDict)==1:
                currentUserAvg = currentUserSum
            if len(currentUserBusinessDict)==0:
                currentUserAvg =0.0

            if key not in pearsonCorelationUsers:

                intersect = businessListActiveUserSet.intersection(currentUserBusinessSet)

                if len(intersect) > 0:
                    activeUserIntersectSum = 0.0
                    currentUserIntersectSum = 0.0
                    for x in intersect:
                        activeUserIntersectSum += activeUserBusinessDict[x]
                        currentUserIntersectSum += currentUserBusinessDict[x]
                    activeAvg = activeUserIntersectSum / len(intersect)
                    userAvg = currentUserIntersectSum / len(intersect)
                else:
                    activeAvg = 0.0
                    userAvg = 0.0

                # calculate pearson corelation
                numerator = 0.0
                activeElements = 0.0
                userElements = 0.0

                for x in intersect:
                    numerator += (currentUserBusinessDict[x] - userAvg) * (activeUserBusinessDict[x] - activeAvg)
                    userElements += (currentUserBusinessDict[x] - userAvg) ** 2
                    activeElements += (activeUserBusinessDict[x] - activeAvg) ** 2

                if userElements != 0 and activeElements != 0:
                    pearsonCorelationUsers[key] = numerator / (math.sqrt(userElements) * math.sqrt(activeElements))
                else:
                    pearsonCorelationUsers[key] = 0.1

            # calculate prediction
            if pearsonCorelationUsers[key] != 0.0:
                predictionNumerator += (float(userRatedPredictionBusiness[1]) - currentUserAvg) * pearsonCorelationUsers[key]
                pearsonModSum += abs(pearsonCorelationUsers[key])
            if ctr > -1:
                activeUserAvg += float(userRatedPredictionBusiness[1])
                ctr += 1
    else:
        # no one has rated the prediction business
        pearsonModSum = 1.0

    if case==2:
        k = user[0][0] + ',' + user[0][1]
    if case==3:
        k = user[0][1] + ',' + user[0][0]

    if pearsonModSum == 0:
        pearsonModSum = 1

    if ctr > 0:
        activeUserAvg = activeUserAvg / ctr

    prediction = activeUserAvg + (predictionNumerator / pearsonModSum)
    if prediction > 5:
        prediction = 5.0
    if prediction < 1:
        prediction = 1.0

    return (k, prediction)

data = sc.textFile(trainCSV)
first = data.first()

valdata = sc.textFile(valCSV)
f = valdata.first()

userCFstring = "user_id, business_id, prediction\n"

if case == 2 or case==3:

    userRatingSum = defaultdict(int)
    pearsonCorelationUsers = defaultdict(float)
    read = data.filter(lambda line: line != first).map(
        lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2]))

    if case==2:
        userBusinessDict = dict(read.map(lambda x: (x[0], [(x[1], float(x[2]))])).reduceByKey(lambda x, y: x + y).collect())
        businessUserDict = dict(read.map(lambda x: (x[1], [(x[0], float(x[2]))])).reduceByKey(lambda x, y: x + y).collect())
        predictionList = valdata.filter(lambda line: line != f).map(
            lambda line: ((line.split(',')[0], line.split(',')[1]), float(line.split(',')[2]))).map(predict).collect()
    if case==3:
        businessUserDict = dict(read.map(lambda x: (x[0], [(x[1], float(x[2]))])).reduceByKey(lambda x, y: x + y).collect())
        userBusinessDict = dict(read.map(lambda x: (x[1], [(x[0], float(x[2]))])).reduceByKey(lambda x, y: x + y).collect())
        predictionList = valdata.filter(lambda line: line != f).map(
            lambda line: ((line.split(',')[1], line.split(',')[0]), float(line.split(',')[2]))).map(predict).collect()

    for value in predictionList:
        userCFstring+=value[0] + ',' + str(value[1]) +'\n'

if case==1:

    userCodeIndex = 0
    businessCodeIndex = 0
    userCode = dict()
    businessCode = dict()
    userCodeRev = dict()
    businessCodeRev = dict()

    ratings = data.filter(lambda line: line != first).map(
        lambda line: (line.split(',')[0], line.split(',')[1], float(line.split(',')[2])))

    for x in ratings.collect():
        if x[0] not in userCode:
            userCode[x[0]] = userCodeIndex
            userCodeRev[userCodeIndex] = x[0]
            userCodeIndex += 1
        if x[1] not in businessCode:
            businessCode[x[1]] = businessCodeIndex
            businessCodeRev[businessCodeIndex] = x[1]
            businessCodeIndex += 1

    ratingsForModel = ratings.map(lambda x: (userCode[x[0]], businessCode[x[1]], x[2]))

    testData = valdata.filter(lambda line: line != f).map(
        lambda line: (line.split(',')[0], line.split(',')[1], float(line.split(',')[2])))

    for x in testData.collect():

        if x[0] not in userCode:
            userCode[x[0]] = userCodeIndex
            userCodeRev[userCodeIndex] = x[0]
            userCodeIndex += 1
        if x[1] not in businessCode:
            businessCode[x[1]] = businessCodeIndex
            businessCodeRev[businessCodeIndex] = x[1]
            businessCodeIndex += 1

    testDataForPrediction = testData.map(lambda x: (userCode[x[0]], businessCode[x[1]]))

    rank = 2
    numIterations = 20
    lambd = 0.5
    model = ALS.train(ratingsForModel, rank, numIterations, lambd)
    predictionsDict = dict(model.predictAll(testDataForPrediction).map(lambda r: ((r[0], r[1]), r[2])).collect())

    avg=sum(predictionsDict.values())/len(predictionsDict)
    
    for x in testData.collect():
        key = (userCode[x[0]], businessCode[x[1]])
        if key in predictionsDict:
            userCFstring += x[0] + ',' + x[1] + ',' + str(predictionsDict[key]) + '\n'
        else:
            userCFstring += x[0] + ',' + x[1] + ',' + str(avg) + '\n'


outputFile = open(filePathOutput, "w")
outputFile.write(userCFstring)