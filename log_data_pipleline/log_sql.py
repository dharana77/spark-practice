from pyspark.sql import SparkSession
from pyspark.sql.types import *


def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        return ss.read.schema(schema).csv("data/log.csv")

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24],
    ]

    return ss.createDataFrame(log_data_inmemory, schema)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log sql ex") \
        .getOrCreate()

    from_file = True

    # define schema
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    table_name = "log_data"

    load_data(ss, from_file, fields) \
        .createOrReplaceTempView(table_name)

    # 데이터 확인
    # ss.sql(f"SELECT * FROM {table_name}").show()
    # 스키마 확인
    # ss.sql(f"SELECT * FROM {table_name}").printSchema()

    # a) 컬럼 변환
    # 1) 현재 레이턴시 컬럼의 단위는 밀리세컨드인데 seconds 단위인
    # latency_sec 컬럼을 새로 만들기
    # ss.sql(f"""
    #     SELECT *, latency / 1000 AS latency_sec
    #     FROM {table_name}
    #     """).show()

    # 2) StringType 로 받은 timestamp 컬럼을 TimestampType으로 변경
    # ss.sql(f"""
    #     SELECT ip, Timestamp(timestamp) AS timestamp, method, endpoint, status_code, latency
    #     FROM {table_name}
    #     """).printSchema()

    # b) filter
    # 1) status_code가 400, endpoint = "/users" 인 로우만 필터링

    # ss.sql(f"""
    #     SELECT *
    #     FROM {table_name}
    #     WHERE status_code = "400" AND endpoint = "/users"
    #     """).show()

    # c) group by
    # 1) method, endpoint 별 레이턴시 최댓값, 최솟값, 평균값
    ss.sql(f"""
        SELECT method, endpoint, MAX(latency) AS max_latency, MIN(latency) AS min_latency, AVG(latency) AS avg_latency
        FROM {table_name}
        GROUP BY method, endpoint
        """).show()

    # 2) 분 단위의, 중복을 제거한 ip 리스트, 개수 뽑기
    ss.sql(f"""
        select
            hour(date_trunc("hour", timestamp)) as hour,
            minute(date_trunc("minute", timestamp)) as minute,
            collect_set(ip) as ip_list,
            count(ip) as ip_count
        FROM {table_name}
        GROUP BY hour, minute
        order by hour, minute
        """).show()