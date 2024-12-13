import pytest
from glue_spark_etl.transformer import Transformer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def test_transformer_sql_query(spark_session):
    """Prueba que la transformación SQL funcione correctamente."""

    # Crear un DataFrame simulado para representar 'source_table'
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    columns = ["name", "age"]
    df = spark_session.createDataFrame(data, columns)

    # Registrar el DataFrame como una vista temporal
    df.createOrReplaceTempView("transformer_source_table")

    # Paso de transformación
    step_options = {
        "name": "transformed_table",
        "sql": "SELECT * FROM transformer_source_table WHERE age > 25"
    }

    # Crear una instancia del Transformer
    transformer = Transformer(spark_session, step_options)

    # Ejecutar la transformación
    transformer.transform()

    # Verificar si la vista temporal fue creada y contiene los datos correctos
    transformed_df = spark_session.sql("SELECT * FROM transformed_table")

    # Comprobar que el resultado contiene las filas esperadas
    transformed_data = transformed_df.collect()
    assert len(transformed_data) == 2
    assert "name" in transformed_df.columns
    assert "age" in transformed_df.columns


def test_transformer_no_data(spark_session):
    """Prueba que la transformación funcione con una tabla vacía."""

    # Crear un DataFrame vacío para representar 'source_table'

    df = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(),
        schema=StructType([
            StructField('name', StringType(), True),
            StructField('age', IntegerType(), True)
        ])
    )
    # Registrar el DataFrame como una vista temporal
    df.createOrReplaceTempView("transformer_source_table")

    # Paso de transformación
    step_options = {
        "name": "transformed_table",
        "sql": "SELECT * FROM transformer_source_table WHERE age > 10"
    }

    # Crear una instancia del Transformer
    transformer = Transformer(spark_session, step_options)

    # Ejecutar la transformación
    transformer.transform()

    # Verificar si la vista temporal fue creada y no contiene datos
    transformed_df = spark_session.sql("SELECT * FROM transformed_table")
    assert transformed_df.count() == 0  # No debería haber resultados


def test_transformer_invalid_sql(spark_session):
    """Prueba que se lance un error con una consulta SQL inválida."""

    # Crear un DataFrame simulado para representar 'source_table'
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    columns = ["name", "age"]
    df = spark_session.createDataFrame(data, columns)

    # Registrar el DataFrame como una vista temporal
    df.createOrReplaceTempView("transformer_source_table")

    # Paso de transformación con una consulta SQL inválida
    step_options = {
        "name": "transformed_table",
        "sql": "SELECT * FROM non_existent_table WHERE age > 10"
    }

    # Crear una instancia del Transformer
    transformer = Transformer(spark_session, step_options)

    # Verificar que se lanza un error
    with pytest.raises(Exception):
        transformer.transform()
