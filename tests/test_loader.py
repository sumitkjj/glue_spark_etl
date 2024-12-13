import pytest
from glue_spark_etl.loader import Loader


@pytest.fixture
def sample_data(spark_session):
    """Fixture para crear datos de ejemplo."""
    data = [("Alice", 25), ("Bob", 30)]
    columns = ["name", "age"]
    return spark_session.createDataFrame(data, columns)


def test_load_csv_success(tmp_path, spark_session, sample_data):
    """Prueba que un DataFrame se guarde correctamente como CSV."""
    sample_data.createOrReplaceTempView("load_view")

    step_options = {
        "name": "output_table",
        "origin_view": "load_view",
        "options": {
            "path": str(tmp_path),
            "format": "csv",
            "mode": "overwrite",
            "delimiter": ","
        }
    }

    loader = Loader(spark_session, step_options)
    loader.load()

    # Verificar que los archivos CSV fueron creados
    output_files = list(tmp_path.glob("output_table/*.csv"))
    assert len(output_files) > 0


def test_load_parquet_success(tmp_path, spark_session, sample_data):
    """Prueba que un DataFrame se guarde correctamente como Parquet."""
    sample_data.createOrReplaceTempView("load_view")

    step_options = {
        "name": "output_table",
        "origin_view": "load_view",
        "options": {
            "path": str(tmp_path),
            "format": "parquet",
            "mode": "overwrite"
        }
    }

    loader = Loader(spark_session, step_options)
    loader.load()

    # Verificar que los archivos Parquet fueron creados
    output_files = list(tmp_path.glob("output_table/*.parquet"))
    assert len(output_files) > 0


def test_load_missing_view_error(spark_session):
    """Prueba que se lance un error si la vista de origen no existe."""
    step_options = {
        "name": "output_table",
        "origin_view": "non_existent_view",
        "options": {
            "path": "/some/path",
            "format": "csv",
            "mode": "overwrite",
            "delimiter": ","
        }
    }

    loader = Loader(spark_session, step_options)

    with pytest.raises(ValueError, match="La vista de origen 'non_existent_view' no existe"):
        loader.load()


def test_load_unsupported_format_error(spark_session, sample_data):
    """Prueba que se lance un error para formatos no soportados."""
    sample_data.createOrReplaceTempView("load_view")

    step_options = {
        "name": "output_table",
        "origin_view": "load_view",
        "options": {
            "path": "/some/path",
            "format": "unsupported_format",
            "mode": "overwrite"
        }
    }

    loader = Loader(spark_session, step_options)

    with pytest.raises(ValueError, match="Formato desconocido: unsupported_format"):
        loader.load()


def test_load_excel_not_implemented_error(spark_session, sample_data):
    """Prueba que se lance un error para el formato Excel."""
    sample_data.createOrReplaceTempView("load_view")

    step_options = {
        "name": "output_table",
        "origin_view": "load_view",
        "options": {
            "path": "/some/path",
            "format": "excel",
            "mode": "overwrite"
        }
    }

    loader = Loader(spark_session, step_options)

    with pytest.raises(NotImplementedError, match="El formato 'excel' no está soportado actualmente"):
        loader.load()
