import pytest
from glue_spark_etl.driver import SparkDriver


@pytest.fixture(scope="session")
def spark_session():
    """Fixture que inicializa la SparkSession en modo test y lo comparte entre los tests"""
    spark_driver = SparkDriver(spark_config_path="tests/resources/conf/properties.conf")

    spark_session = spark_driver.spark_session
    yield spark_session

    spark_driver.stop()
