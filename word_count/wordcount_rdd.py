import findspark
findspark.init()

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":


    ss: SparkSession = SparkSession.builder.\
        master("local").\
        appName("wordCount Rdd")\
        .getOrCreate()

    sc: SparkContext = ss.sparkContext

    #load
    text_file: RDD[str] = sc.textFile("data/words.txt")
    #transform
    counts = text_file.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda count1, count2: count1 + count2)

    #action
    output = counts.collect()

    for (word, count) in output:
        print(f"{word}: {count}")
