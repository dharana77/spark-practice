from pyspark import SparkContext
from pyspark.sql import SparkSession


def load_data(from_file: bool, sc: SparkContext):
    if (from_file):
        return load_data_from_file(sc)
    return load_data_from_in_memory(sc)


def load_data_from_file(sc: SparkContext):
    return sc.textFile("data/user_visits.txt").map(lambda v: v.split(",")), \
        sc.textFile("data/user_names.txt").map(lambda v: v.split(","))


def load_data_from_in_memory(sc: SparkContext):
    # [user_id, visits]
    user_visits = [
        (1, 10),
        (2, 27),
        (3, 2),
        (4, 5),
        (5, 88),
        (6, 1),
        (7, 5)
    ]
    # [userid, name]
    user_names = [
        (1, "Andrew"),
        (2, "Chris"),
        (3, "John"),
        (4, "Bob"),
        (6, "Ryan"),
        (7, "Mali"),
        (8, "Tony"),
    ]

    return sc.parallelize(user_visits), sc.parallelize(user_names)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.\
        master("local").\
        appName("rdd join test")\
        .getOrCreate()

    sc: SparkContext = ss.sparkContext

    user_visits_rdd, user_names_rdd = load_data(True, sc)

    # print(user_visits_rdd.take(5))
    # print(user_names_rdd.take(5))

    # 크리스의 방문 횟수 출력
    joined_rdd = user_names_rdd.join(user_visits_rdd).sortByKey()
    # print(joined_rdd.take(5))

    result = joined_rdd.filter(lambda row: row[1][0] == "Chris").collect()
    # print(result)

    # inner, left out , right outer, full outer join
    inner = user_names_rdd.join(user_visits_rdd).sortByKey()
    print(f"inner ---> {inner.collect()}")

    left_outer = user_names_rdd.leftOuterJoin(user_visits_rdd).sortByKey()
    print(f"left --> {left_outer.collect()}")

    right_outer = user_names_rdd.rightOuterJoin(user_visits_rdd).sortByKey()
    print(f"right --> {right_outer.collect()}")

    full_outer = user_names_rdd.fullOuterJoin(user_visits_rdd).sortByKey()
    print(f"full --> {full_outer.collect()}")

