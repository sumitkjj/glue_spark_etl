import pytest
from glue_spark_etl.driver import SparkDriver


# Definir un fixture para inicializar y compartir la sesión de Spark
@pytest.fixture
def spark_driver():
    """Fixture que inicializa el SparkDriver en modo test y lo comparte entre los tests"""
    # Inicializamos el SparkDriver en modo test
    spark_driver = SparkDriver(spark_config_path="tests/resources/conf/properties.conf")

    # Esto devuelve el driver y permite usarlo en los tests
    # Al finalizar el test, se puede agregar un cleanup si es necesario
    # Aunque SparkDriver se encarga de cerrar la sesión al llamar a stop()
    yield spark_driver


def test_spark_driver_creation(spark_driver):
    """Prueba que `SparkDriver` se inicializa correctamente en modo test (entorno local)"""

    # Verificamos que se ha creado el SparkSession
    assert spark_driver.spark_session is not None

    # Verificamos que las configuraciones esperadas estén presentes en la sesión de Spark
    assert spark_driver.spark_session.conf.get("spark.master") == "local[*]"
    assert spark_driver.spark_session.conf.get("spark.local.dir") == "/tmp/spark"
    assert spark_driver.spark_session.conf.get("spark.sql.warehouse.dir")[5:] == "/tmp/spark/warehouse"
    assert spark_driver.spark_session.conf.get("spark.sql.catalogImplementation") == "in-memory"
    assert spark_driver.spark_session.conf.get("spark.sql.constraintPropagation.enabled") == "false"
    assert spark_driver.spark_session.conf.get("spark.sql.codegen.wholestage") == "false"
    assert spark_driver.spark_session.conf.get("spark.sql.shuffle.partitions") == "1"


def test_spark_driver_stop(spark_driver):
    """Prueba que el método `stop()` detiene la sesión de Spark correctamente y reinicia la instancia del SparkDriver"""

    # Aseguramos que la sesión está activa antes de detenerla
    assert spark_driver.spark_session is not None

    # Llamamos al método stop
    spark_driver.stop()

    # Verificamos que la instancia de SparkDriver se reinicia a None
    assert SparkDriver._instance is None

    # Verificamos que una nueva instancia de SparkDriver se crea al llamar de nuevo
    spark_driver_new = SparkDriver(spark_config_path="tests/resources/conf/properties.conf")
    assert spark_driver_new is not spark_driver  # Deberían ser instancias diferentes
    assert spark_driver_new.spark_session is not None  # La nueva sesión debería estar activa
    spark_driver_new.stop()
