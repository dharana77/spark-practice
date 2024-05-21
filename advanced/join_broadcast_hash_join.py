from random import random

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import broadcast

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("Join broadcast hash join") \
        .getOrCreate()

    # big
    big_list = [[random.randint(1, 10)] for _ in range(100000)]
    big_df = ss.createDataFrame(big_list).toDF("id")

    # small
    small_list = [[1, "A"], [2, "B"], [3, "C"],]
    small_df = ss.createDataFrame(small_list).toDF("id", "name")

    joined_df = big_df.join(broadcast(small_df), on="id")

    joined_df.show()

    while True:
        pass
