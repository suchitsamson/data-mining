from pyspark import SparkContext
import sys
import json

sc = SparkContext("local[*]","suchit_mehta_task1")

filePathJSON = sys.argv[1]
filePathOutputT1 = sys.argv[2]
jsonRDD = sc.textFile(filePathJSON).map(json.loads)

usefulCount = jsonRDD.map(lambda x: 1 if int(x['useful'])>0 else 0).reduce(lambda a,b: a+b)
fiveStarRating = jsonRDD.map(lambda x: 1 if int(x['stars'])==5 else 0).reduce(lambda a,b: a+b)
longestReview = jsonRDD.map(lambda x: len(x['text'])).max()
userReviewsRDD = jsonRDD.map(lambda x: (x['user_id'],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: (-a[1],a[0]))
top20userReviewsList = userReviewsRDD.take(20)
businessReviewsRDD = jsonRDD.map(lambda x: (x['business_id'],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: (-a[1],a[0]))
top20businessReviewsList = businessReviewsRDD.take(20)

outputFile = open(filePathOutputT1, "w")

json.dump({'n_review_useful': usefulCount, 'n_review_5_star': fiveStarRating, 'n_characters': longestReview,
                             'n_user': userReviewsRDD.count(), 'top20_user': top20userReviewsList, 'n_business': businessReviewsRDD.count(),
                          'top20_business': top20businessReviewsList},outputFile,indent=2)
