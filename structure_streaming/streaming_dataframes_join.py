from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("streaming dataframe join examples") \
        .getOrCreate()

    authors = ss.read \
        .option("inferSchema", True).json("data/authors.json")

    books = ss.read \
        .option("inferSchema", True).json("data/books.json")

    # 1. join (static, static)
    authors_books_df = authors.join(books,
                                    authors["book_id"] == books["id"],
                                    "inner")

    # authors_books_df.show()


    # 2. join (static, stream)
    def join_stream_with_static():
        streamed_books = ss \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345) \
            .load().select(F.from_json(F.col("value"),
                                       books.schema).alias("book")) \
            .selectExpr("book.id as id",
                        "book.name as name",
                        "book.year as year"
                        )

        authors_books_df = authors.join(streamed_books,
                                        authors["book_id"] == streamed_books["id"],
                                        "inner")

        authors_books_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()

    # join_stream_with_static()
    # 조인 연산은 마이크로 배치가 이루어질때마다 이루어진다는 것을 확인가능

    # left out join 을 할때 왼쪽이 스태틱이고 오른쪽이 스트리밍이면 에러가 발생하고
    # right out join 할때는 이상이 없음 -> 스트리밍 기준으로 맞춰야 하는듯
    # 스트리밍은 무한한 데이터이기 때문에 잘못 조인 했을때 엄청난 성능 부담이 발생할 수 있기 때문에 제약조건 걸어놓음
    # full outer를 넣으면?


    # 3. join (stream, stream)
    def join_stream_with_stream():
        streamed_authors = ss \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12346) \
            .load().select(F.from_json(F.col("value"),
                                       authors.schema).alias("author")) \
            .selectExpr("author.id as id",
                        "author.name as name",
                        "author.book_id as book_id"
                        )

        streamed_books = ss \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345) \
            .load().select(F.from_json(F.col("value"),
                                       books.schema).alias("book")) \
            .selectExpr("book.id as id",
                        "book.name as name",
                        "book.year as year"
                        )

        authors_books_df = streamed_authors.join(streamed_books,
                                                 streamed_authors["book_id"] == streamed_books["id"],
                                                 "inner")

        authors_books_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()