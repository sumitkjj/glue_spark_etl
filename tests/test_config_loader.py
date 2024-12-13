import pytest
from unittest.mock import patch, mock_open
from pathlib import Path
from glue_spark_etl.config_loader import ConfigLoader


# Tests
def test_load_valid_config():
    # YAML válido de ejemplo
    valid_yaml = """
    steps:
      - step: extract
        type: jdbc
        name: source_table_db
        options:
          host: example.com
          db_type: mysql
          db_name: my_database
          table: my_table
          user_key: user_key_1
          password_key: password_key_1 
      - step: extract
        type: s3
        name: source_table_s3
        options:
          path: s3://example/file.csv
          format: csv
          delimiter: ","
      - step: transform
        name: transformed_table
        sql: SELECT * FROM source_table WHERE column_x > 10
      - step: load
        type: s3
        name: final_output_s3
        origin_view: s3_table
        options:
          path: s3://my-bucket/output/
          format: parquet
          mode: overwrite
    """
    """Prueba que `load_config` carga correctamente un archivo válido."""
    with patch.object(Path, "open", mock_open(read_data=valid_yaml)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        config = loader.load_config()
        assert isinstance(config, dict), "La configuración cargada debe ser un diccionario."
        assert "steps" in config, "La configuración debe contener la clave 'steps'."
        assert len(config["steps"]) == 4, "Debe haber exactamente un paso definido en la configuración."


def test_load_nonexistent_file():
    """Prueba que `load_config` lanza un error cuando el archivo no existe."""
    with patch("pathlib.Path.is_file", return_value=False):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(FileNotFoundError, match="El archivo de configuración 'config.yaml' no existe."):
            loader.load_config()


def test_load_invalid_yaml():
    """Prueba que `load_config` lanza un error con un archivo YAML mal formado."""
    # YAML malformado de ejemplo
    invalid_yaml = "steps: - step: extract"
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="Formato de archivo de configuración no válido"):
            loader.load_config()


def test_load_no_steps_config():
    """Prueba que `load_config` lanza un error si no hay pasos definidos"""
    invalid_yaml_no_steps = """other_field: value"""
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_no_steps)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="La configuración no contiene ningún paso definido."):
            loader.load_config()


def test_load_no_extraction_step_config():
    """Prueba que `load_config` lanza un error si no hay paso de extracción ('extract')"""
    invalid_yaml_no_extraction = """
    steps:
      - step: transform
        name: transformed_table
        sql: SELECT * FROM source_table WHERE column_x > 10
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_no_extraction)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="La configuración debe contener al menos un paso de extracción"):
            loader.load_config()


def test_load_invalid_step_type_config():
    """Prueba que `load_config` lanza un error si hay un paso con tipo inválido"""
    invalid_yaml_invalid_step = """
    steps:
      - step: invalid_step
        name: invalid_step_name
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_invalid_step)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="El paso #1 tiene un tipo no válido: 'invalid_step'"):
            loader.load_config()


def test_load_no_name_in_step_config():
    """Prueba que `load_config` lanza un error si un paso no tiene el campo 'name'"""
    invalid_yaml_no_name = """
    steps:
      - step: extract
        type: local
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_no_name)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="El paso #1 no tiene el campo 'name' definido."):
            loader.load_config()


def test_invalid_extractor_step_missing_option():
    """Prueba que se lanza un error si falta una opción requerida en el paso de extracción"""
    invalid_yaml_missing_option = """
    steps:
      - step: extract
        type: s3
        name: source_table_s3
        options:
          path: s3://example/file.csv
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_missing_option)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="Falta la opción 'format'"):
            loader.load_config()


def test_invalid_extractor_step_unknown_source_type():
    """Prueba que se lanza un error si el tipo de origen no es válido"""
    invalid_yaml_unknown_type = """
    steps:
      - step: extract
        type: unknown
        name: source_table_unknown
        options:
          path: /path/to/file.csv
          format: csv
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_unknown_type)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="Tipo de origen desconocido en el paso de extracción"):
            loader.load_config()


def test_invalid_transform_step_missing_sql():
    """Prueba que un paso de transformación sin el campo 'sql' lanza un error."""
    invalid_yaml_transform = """
    steps:
      - step: transform
        name: invalid_transform_step
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_yaml_transform)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(
                ValueError, match="El paso de transformación 'invalid_transform_step' no tiene el campo 'sql' definido."
        ):
            loader.load_config()


def test_load_config_missing_loader_keys():
    """Prueba que se lanza un error si faltan campos obligatorios en un paso 'load'."""
    invalid_config = """
    steps:
      - step: load
        type: s3
        name: final_output_s3
        options:
          path: "s3://my-bucket/output/"
          format: parquet
          mode: overwrite
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_config)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(
                ValueError, match="El paso de carga 'final_output_s3' tiene parámetros faltantes: origin_view"
        ):
            loader.load_config()


def test_load_config_invalid_type():
    """Prueba que se lanza un error si el 'type' en un paso 'load' es inválido."""
    invalid_config = """
    steps:
      - step: load
        type: ftp
        name: final_output_ftp
        origin_view: s3_table
        options:
          path: "s3://my-bucket/output/"
          format: parquet
          mode: overwrite
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_config)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="Tipo de destino inválido 'ftp' en el paso de carga 'final_output_ftp'"):
            loader.load_config()


def test_load_config_invalid_format():
    """Prueba que se lanza un error si el 'format' en un paso 'load' es inválido."""
    invalid_config = """
    steps:
      - step: load
        type: s3
        name: final_output_s3
        origin_view: s3_table
        options:
          path: "s3://my-bucket/output/"
          format: xml
          mode: overwrite
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_config)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(ValueError, match="Formato inválido 'xml' en el paso de carga 'final_output_s3'"):
            loader.load_config()


def test_load_config_invalid_mode():
    """Prueba que se lanza un error si el 'mode' en un paso 'load' es inválido."""
    invalid_config = """
    steps:
      - step: load
        type: s3
        name: final_output_s3
        origin_view: s3_table
        options:
          path: "s3://my-bucket/output/"
          format: parquet
          mode: invalid_mode
    """
    with patch.object(Path, "open", mock_open(read_data=invalid_config)), \
            patch("pathlib.Path.is_file", return_value=True):
        loader = ConfigLoader("config.yaml")
        with pytest.raises(
                ValueError, match="Modo de escritura inválido 'invalid_mode' en el paso de carga 'final_output_s3'"
        ):
            loader.load_config()
