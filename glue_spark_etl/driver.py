from pathlib import Path
from pyspark.sql import SparkSession
from utils.s3_utils import S3Utils


class SparkDriver:
    # Variable estática para asegurar que el driver es singleton
    _instance = None
    _spark_session = None

    def __new__(cls, spark_config_path: str):
        if cls._instance is None:
            cls._instance = super(SparkDriver, cls).__new__(cls)
            cls._instance.test = not spark_config_path.startswith("s3://")
            cls._instance.spark_config_path = spark_config_path
            cls._instance._initialize_spark_session()
        return cls._instance

    def _initialize_spark_session(self):
        builder = SparkSession.builder

        if self.test:
            # Configuración para entorno local (hardcoded)
            builder = builder.appName("LocalSparkApplication")

            # Configuración para entorno local
            with Path(self.spark_config_path).open("r") as file:
                content = file.read()
        else:
            if not S3Utils.file_exists(self.spark_config_path):
                raise ValueError(f"El fichero: '{self.spark_config_path}' no existe en S3.")

            # Configuración para entorno AWS Glue (desde S3)
            content = S3Utils.get_file_content(self.spark_config_path)
            builder = builder.appName("AWSSparkApplication")

        config = {}
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()

        # set builder properties
        for key, value in config.items():
            builder = builder.config(key, value)

        self._spark_session = builder.getOrCreate()

    @property
    def spark_session(self):
        return self._spark_session

    def stop(self):
        """
        Detiene la SparkSession y reinicia la instancia.
        """
        if self.spark_session:
            self.spark_session.stop()
            # Reinicia la instancia
            SparkDriver._instance = None
