import sys
from pyspark import SparkConf, SparkContext
    
#Initialize Spark application
conf = SparkConf().setAppName("Lab_6")
sc = SparkContext(conf = conf)

'''TASK 1:
1. Transposes the original Amazon food dataset, obtaining an RDD of pairs (tuples) of
the type:
(user_id, list of the product_ids reviewed by user_id)
The returned RDD contains one pair/tuple for each user, which contains the user_id
and the complete list of (distinct) products reviewed by that user. If user user_id
reviewed more times the same product, that product must occurs only one time in the
returned list of the product_ids reviewed by user_id;
2. Counts the frequencies of all the pairs of products reviewed together (the frequency
of a pair of products is given by the number of users who reviewed both products);
3. Writes on the output folder all the pairs of products that appear more than once and
their frequencies. The pairs of products must be sorted by decreasing frequency. 
'''