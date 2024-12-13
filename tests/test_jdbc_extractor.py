import pytest
from glue_spark_etl.extractors.jdbc_extractor import JDBCExtractor


def test_invalid_db_type(spark_session):
    """Prueba que se lanza un error al pasar un db_type no admitido."""

    step_options_invalid_db_type = {
        "name": "test_step",
        "options": {
            "host": "localhost",
            "db_type": "unsupported_db",  # Tipo de base de datos no soportado
            "db_name": "test_db",
            "table": "test_table",
            "user_key": "user",
            "password_key": "password"
        }
    }

    with pytest.raises(ValueError, match="Tipo de base de datos no soportada: 'unsupported_db'."):
        JDBCExtractor(spark_session, step_options_invalid_db_type).extract()
