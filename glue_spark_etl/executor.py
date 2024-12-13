from glue_spark_etl.config_loader import ConfigLoader
from glue_spark_etl.driver import SparkDriver
from glue_spark_etl.extractors.s3_local_unified_extractor import S3LocalUnifiedExtractor
from glue_spark_etl.extractors.jdbc_extractor import JDBCExtractor
from glue_spark_etl.transformer import Transformer
from glue_spark_etl.loader import Loader


class Executor:
    def __init__(self, config_path: str, spark_config_path: str):
        """
        Inicializa el Executor.

        Args:
            config_path (str): Ruta al archivo de configuración del flujo.
            spark_config_path (str): Ruta al archivo de configuración de spark del flujo.
        """
        self.config_path = config_path
        self.spark_config_path = spark_config_path

    def __initialize_spark_session(self):
        """
        Inicializa el SparkSession.
        """
        self.spark_driver = SparkDriver(self.spark_config_path)
        self.spark_session = self.spark_driver.spark_session

    def __load_config(self):
        """
        Carga la configuración desde el archivo especificado.
        """
        self.config = ConfigLoader(self.config_path).load_config()

    def __execute_extraction(self, step: dict):
        """
        Ejecuta un paso de extracción.

        Args:
            step (dict): Configuración del paso de extracción.
        """
        step_type = step.get("type")
        if step_type == "jdbc":
            extractor = JDBCExtractor(self.spark_session, step)
        elif step_type in {"s3", "local"}:
            extractor = S3LocalUnifiedExtractor(self.spark_session, step)
        else:
            raise ValueError(f"Tipo de extracción desconocido: {step_type}")
        extractor.extract()

    def __execute_transformation(self, step: dict):
        """
        Ejecuta un paso de transformación.

        Args:
            step (dict): Configuración del paso de transformación.
        """
        transformer = Transformer(self.spark_session, step)
        transformer.transform()

    def __execute_loading(self, step: dict):
        """
        Ejecuta un paso de carga.

        Args:
            step (dict): Configuración del paso de carga.
        """
        loader = Loader(self.spark_session, step)
        loader.load()

    def execute(self):
        """
        Orquesta la ejecución de los pasos definidos en la configuración.
        """
        # Cargar configuración
        self.__load_config()

        # Inicializar Sesion Spark
        self.__initialize_spark_session()

        # Ejecutar los pasos en orden
        for step in self.config.get("steps", []):
            step_type = step.get("step")
            if step_type == "extract":
                self.__execute_extraction(step)
            elif step_type == "transform":
                self.__execute_transformation(step)
            elif step_type == "load":
                self.__execute_loading(step)
            else:
                raise ValueError(f"Tipo de paso desconocido: {step_type}")

        # parar sesion de spark
        self.spark_driver.stop()