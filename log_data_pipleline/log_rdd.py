import findspark

findspark.init()

# RDD 트랜스포메이션 연산 연습

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[*]") \
        .appName("rdd example ver") \
        .getOrCreate()

    sc: SparkContext = ss.sparkContext

    log_rdd: RDD[str] = sc.textFile("data/log.txt")

    # check count
    # print(f"count of RDD ==> {log_rdd.count()}")


    # line print test
    # log_rdd.foreach(lambda line: print(line))

    # a) map
    # - log.txt의 각 행 list[str]로 받기
    def parse_line(row: str):
        return row.strip().split(" | ")


    parsed_log_rdd: RDD[list[str]] = log_rdd.map(parse_line)


    # parsed_log_rdd.foreach(print)

    # b) filter
    # 1) status code가 404인 로그만 필터
    def filter_404(row: list[str]):
        status_code = row[3]
        return status_code == "404"

    # rdd_404 = parsed_log_rdd.filter(filter_404)
    #rdd_404.foreach(print)

    # 2) status code가 정상인 경우 (2xx)만 필터
    def filter_2xx(row: list[str]):
        status_code = row[3]
        return status_code.startswith("2")

    # rdd_normal = parsed_log_rdd.filter(filter_2xx)
    # rdd_normal.foreach(print)

    #3) post 요청이고 playbooks api의 로그인 필터링
    def get_post_request_and_playbooks_api(row: list[str]):
        log = row[2].replace("\"", "")
        return log.startswith("POST") and  "/playbooks" in log

    # rdd_post_playbooks = parsed_log_rdd.filter(get_post_request_and_playbooks_api)
    # rdd_post_playbooks.foreach(print)

    # c reduce
    def get_counts_group_by_http_method(row: list[str]):
        log = row[2].replace("\"", "")
        api_method = log.split(" ")[0]
        return api_method, 1

    # rdd_http_method_count = parsed_log_rdd.map(
    #     get_counts_group_by_http_method
    # ).reduceByKey(lambda count1, count2: count1 + count2)

    # rdd_http_method_count.foreach(print)

    # 2) 분 단위 별 요청 횟수
    def extract_hour_and_minute(row: list[str]) -> tuple[str, int]:
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_time_obj = datetime.strptime(timestamp, date_format)

        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1

    # rdd_count_by_hour_minute = parsed_log_rdd.map(extract_hour_and_minute)\
    #     .reduceByKey(lambda count1, count2: count1 + count2)\
    #     .sortByKey()

    # rdd_count_by_hour_minute.foreach(print)

    # d) group by
    # 1) status, api method 별 ip 리스트
    def get_ip_list_by_status_and_api_method(row: list[str]):
        ip = row[0]
        status_code = row[3]
        log = row[2].replace("\"", "")
        api_method = log.split(" ")[0]

        return status_code, api_method, ip

    # result = parsed_log_rdd.map(get_ip_list_by_status_and_api_method)\
    #     .map(lambda row: ((row[0], row[1]), row[2]))\
    #     .groupByKey().mapValues(list)

    # result.foreach(print)

    # reduceByKey

    result2 = parsed_log_rdd.map(get_ip_list_by_status_and_api_method)\
        .map(lambda row: ((row[0], row[1]), row[2]))\
        .reduceByKey(lambda ip_list1, ip_list2: f"{ip_list1},{ip_list2}")\
        .map(lambda row: (row[0], row[1].split(",")))

    # result.foreach(print)
    # 작은 데이터의 경우 큰 상관없는데 큰 데이터의 경우 gropu by 보다
    # reduceByKey가 더 효율적이다. (더 성능이 좋다)
    # groupByKey와 reduceByKey 모두 와이드 트랜스포메이션인데
    # 이 와이드 트랜스포메이션은 각 익스큐터 간의 데이터 교환이 발생하는 트랜스포메이션
    # 그런데 groupByKey는 모든 데이터를 다 익스큐터끼리 교환을 하게 되는 반면
    # reduceByKey는 익스큐터들끼리 데이터를 교환하기 전에 한 익스큐터 내부에서 어느정도 reduce 연산을
    # 수행한 이후 다른 익스큐터 들과 데이터 교환을 하기 때문에
    # 일반적으로 더 큰 데이터셋의 상황에서는 reduceByKey가 더 성능적으로 효율적이다.


    result2.collect()

    while True:
        pass
