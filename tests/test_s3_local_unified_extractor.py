import pytest
from glue_spark_etl.extractors.s3_local_unified_extractor import S3LocalUnifiedExtractor


def test_csv_extraction(spark_session):
    """Test de extracción de un archivo CSV."""

    step_options_csv = {
        "name": "csv_persons_table",
        "options": {
            "path": "tests/resources/files/persons.csv",
            "format": "csv",
            "delimiter": ","
        }
    }

    extractor = S3LocalUnifiedExtractor(spark_session, step_options_csv)
    extractor.extract()

    # Verificar si la vista temporal fue creada
    df = spark_session.sql("SELECT * FROM csv_persons_table")
    assert df.count() == 5
    assert "name" in df.columns


def test_parquet_extraction(spark_session):
    """Test de extracción de un archivo Parquet."""
    step_options_parquet = {
        "name": "parquet_persons_table",
        "options": {
            "path": "tests/resources/files/persons.parquet",
            "format": "parquet"
        }
    }

    extractor = S3LocalUnifiedExtractor(spark_session, step_options_parquet)
    extractor.extract()

    # Verificar si la vista temporal fue creada
    df = spark_session.sql("SELECT * FROM parquet_persons_table")
    assert df.count() == 5
    assert len(df.columns) == 4


def test_excel_extraction(spark_session):
    """Prueba que se lanza un error al intentar leer un archivo Excel."""

    step_options_excel = {
        "name": "excel_persons_table",
        "options": {
            "path": "tests/resources/files/persons.xlsx",
            "format": "excel"
        }
    }
    extractor = S3LocalUnifiedExtractor(spark_session, step_options_excel)
    with pytest.raises(NotImplementedError, match="El formato 'excel' no está soportado actualmente."):
        extractor.extract()
