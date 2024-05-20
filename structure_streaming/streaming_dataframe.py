from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("streaming dataframe examples") \
        .getOrCreate()

    # define schema
    schemas = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    def hello_streaming():
        lines = ss.readStream\
            .format("socket")\
            .option("host", "localhost")\
            .option("port", 12345)\
            .load()

        lines.writeStream\
            .format("console")\
            .outputMode("append")\
            .trigger(processingTime="2 seconds")\
            .start()\
            .awaitTermination()

    # hello_streaming()

    def read_from_socket():
        lines = ss.readStream\
            .format("socket")\
            .option("host", "localhost")\
            .option("port", 12345)\
            .load()

        cols = ["ip", "timestamp", "method", "endpoint", "status_code", "latency"]

        splited_line = F.split(lines['value'], ",")
        df = \
            lines.withColumn(cols[0], splited_line.getItem(0))\
                .withColumn(cols[1], splited_line.getItem(1))\
                .withColumn(cols[2], splited_line.getItem(2))\
                .withColumn(cols[3], splited_line.getItem(3))\
                .withColumn(cols[4], splited_line.getItem(4))\
                .withColumn(cols[5], splited_line.getItem(5))

        # filter : status_code = 400, endpoint = "/users"
        df = df.filter(df["status_code"] == "400").filter(df["endpoint"] == "/users")

        # group by : method, endpoint 별 레이턴시 최댓값, 최솟값, 평균값
        group_cols = ["method", "endpoint"]

        df = df.groupby(group_cols)\
            .agg(F.max("latency").alias("max_latency"),
                 F.min("latency").alias("min_latency"),
                 F.avg("latency").alias("mean_latency")
             )
        #무한히 데이터 들어오는 것 가정, 집계가 무한하다면 스파크에 부하가
        #심할수 있으므로 워터마크 같은 특정 시점에 대한 제약을 걸음


        df.writeStream\
            .format("console")\
            .outputMode("append")\
            .trigger(processingTime="2 seconds")\
            .start()\
            .awaitTermination()

    # read_from_socket()

    def read_from_files():
        logs_df = ss.readStream\
                    .format("csv")\
                    .option("header", "false")\
                    .schema(schemas)\
                    .load("data/logs")

        logs_df.writeStream\
                .format("console")\
                .outputMode("append")\
                .trigger(processingTime="2 seconds")\
                .start()\
                .awaitTermination()

    read_from_files()