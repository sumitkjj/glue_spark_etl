import os
import pytest
from glue_spark_etl.executor import Executor


def test_executor_with_local_config(tmp_path):
    """
    Prueba la clase Executor con una configuración local y verifica que los datos se procesen correctamente.
    """
    # Configuración de prueba
    config_path = "tests/resources/conf/test_executor_config.yml"
    spark_config_path = "tests/resources/conf/properties.conf"

    # Crear un executor
    executor = Executor(config_path=config_path, spark_config_path=spark_config_path)

    # Ejecutar el flujo
    executor.execute()

    # Verificar la salida
    output_path = os.path.join("tests/output", "join_output_table")
    assert os.path.exists(output_path), "La carpeta de salida no se creó."

    # Verificar que los datos procesados estén en el archivo de salida
    output_files = [f for f in os.listdir(output_path) if f.endswith(".csv")]
    assert len(output_files) > 0, "No se encontraron archivos de salida."

    # Leer uno de los archivos de salida y verificar el contenido
    with open(os.path.join(output_path, output_files[0]), "r") as f:
        lines = f.readlines()
    assert len(lines) > 1, "El archivo de salida no contiene datos."
