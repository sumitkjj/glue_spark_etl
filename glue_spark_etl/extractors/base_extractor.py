from pyspark.sql import SparkSession


class BaseExtractor:
    """Clase base para los extractores."""
    def __init__(self, spark: SparkSession, step_options: dict):
        self.spark = spark
        self.step_options = step_options

    def extract(self):
        raise NotImplementedError("Este método debe ser implementado por las subclases.")
