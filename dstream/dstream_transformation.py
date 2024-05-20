import os
from collections import namedtuple

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext, DStream

columns = ["Ticker", "Date", "Open", "High",
           "Low", "Close", "AdjClose", "Volume"]
Finance = namedtuple("Finance",
                     columns)

if __name__ == "__main__":
    sc: SparkContext = SparkSession.builder \
        .master("local[2]") \
        .appName("DStream transformations ex") \
        .getOrCreate().sparkContext

    ssc = StreamingContext(sc, 5)

    def read_finance() -> DStream[Finance]:
        # 1. map
        def parse(line: str):
            arr = line.split(",")
            return Finance(*arr)

        return ssc.socketTextStream("localhost", 12345)\
                .map(parse)

    finance_stream: DStream[Finance] = read_finance()
    # finance_stream.pprint()


    # filter
    def filter_nvidia():
        finance_stream.filter(lambda f: f.Ticker == "NVDA").pprint()

    # filter_nvidia()

    def filter_volume():
        finance_stream.filter(lambda f: f.Volume > 1000000).pprint()

    # filter_volume()

    # reduce by, group by
    # 주의 할점은 Dstream에서 리듀스바이 그룹바이를 할때는 이 그룹핑을 하는 단위가 각 마이크로 배치가 되는 특성이 있음
    # 나머지는 그냥 RDD 했던 것과 동일
    def count_dates_ticker():
        finance_stream.map(lambda f: (f.Ticker, 1))\
            .reduceByKey(lambda a, b: a + b).pprint()

    def group_by_dates_volume():
        finance_stream.map(lambda f: (f.Date, int(f.Volume)))\
            .groupByKey().mapValues(sum).pprint()

    # count_dates_ticker()
    # group_by_dates_volume()

    def save_to_json():
        def foreach_func(rdd: RDD):
            if rdd.isEmpty():
                print("RDD is empty")
                return
            df = rdd.toDF(columns)
            dir_path = "data/stocks/outputs"
            n_files = len(os.listdir(dir_path))
            full_path = f"{dir_path}/finance-{n_files}.json"
            df.write.json(full_path)
            print(f"num-partitions: {rdd.getNumPartitions()}")
            print("write completed")

        finance_stream.foreachRDD(foreach_func)

    save_to_json()

    ssc.start()
    ssc.awaitTermination()