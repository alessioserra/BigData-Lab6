import sys
from pyspark import SparkConf, SparkContext
    
#Initialize Spark application
conf = SparkConf().setAppName("Lab_6")
sc = SparkContext(conf = conf)

inputPath  = sys.argv[1]
outputPath = sys.argv[2] 

'''TASK 1:
1. Transposes the original Amazon food dataset, obtaining an RDD of pairs (tuples) of
the type:
(user_id, list of the product_ids reviewed by user_id)
The returned RDD contains one pair/tuple for each user, which contains the user_id
and the complete list of (distinct) products reviewed by that user. If user user_id
reviewed more times the same product, that product must occurs only one time in the
returned list of the product_ids reviewed by user_id;'''

#Read the file
inputRDD = sc.textFile(inputPath)

#Discard the header
inputRDDnoHead = inputRDD.filter(lambda line: line.startswith("Id,")==False)

#Create pair RDD
pairsRDD = inputRDD.map(lambda line: (line.split(",")[2] , line.split(",")[1]))

#Remove dublicate pairs (user reviews the same product more than 1 time)
groupedRDDDistinct = pairsRDD.distinct()

#Group by userID
groupedRDD = pairsRDD.groupByKey()

'''2. Counts the frequencies of all the pairs of products reviewed together (the frequency
of a pair of products is given by the number of users who reviewed both products);'''

#Now we need only the list of values
pairValues = groupedRDD.values()

def extractPairs(pairs):
    products = list(pairs)
    returnedPairs = []
    for p1 in products:
        for p2 in products:
            if p1<p2: # no combination of same values
                returnedPairs.append( ((p1, p2), 1) )
    return returnedPairs

pairsOfProductsRDD = pairValues.flatMap(extractPairs)

# Count the frequency (i.e., number of occurrences) of each key (= pair of products)
pairsFrequenciesRDD = pairsOfProductsRDD.reduceByKey(lambda count1, count2: count1 + count2)

'''
3. Writes on the output folder all the pairs of products that appear more than once and
their frequencies. The pairs of products must be sorted by decreasing frequency. 
'''
pairsMoreOneRDD = pairsFrequenciesRDD.filter(lambda line: line[1] > 1)

# Decreasing frequency
resultRDD = pairsMoreOneRDD.sortBy(lambda Ituple: Ituple[1], False).cache() #to retrieve immediately

# Store the result in the output folder
resultRDD.saveAsTextFile(outputPath)