from pyspark.sql import DataFrame

from common.common import read_data_from_db, create_spark_session


if __name__ == '__main__':
    spark = create_spark_session()

    src_connection_str = "jdbc:postgresql://localhost:5432/hjyoo"
    src_username = "hjyoo"
    src_password = "hjyoo"

    sales_df = read_data_from_db(
        spark=spark,
        connection_str=src_connection_str,
        username=src_username,
        password=src_password,
        table="TbLotto_Add_bulk"
    ).limit(100)
