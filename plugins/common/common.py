from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

# localhost
ip = "127.0.0.1"
# port
port = "5432" 
# username
user = "hjyoo"
# password
passwd = "hjyoo"
# database
db = "TbLottoAdd_bulk"

# 위에서 다운 받은 jdbc driver 경로 입력
# SparkSession
def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "8g")

    spark_session = SparkSession\
    .builder\
    .master("local[4]")\
    .config(conf=conf)\
    .appName("Read from JDBC tutorial")\
    .config("spark.jars", "postgresql-42.5.2.jar")\
    .getOrCreate()

    return spark_session

def read_data_from_db(spark: SparkSession, connection_str: str, username: str, password: str, table: str) -> DataFrame:
    properties = {
        "user": "hjyoo",
        "password": "hjyoo",
        "driver": "org.postgresql.Driver"
    }
    data_df = spark.read.jdbc(
        url=connection_str,
        properties=properties,
        table=table
    )

    return data_df