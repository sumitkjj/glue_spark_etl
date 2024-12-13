from pyspark.sql import SparkSession


class Loader:
    """
    Clase para cargar datos desde una vista de Spark a local o S3 en diferentes formatos.
    """

    def __init__(self, spark: SparkSession, step_options: dict):
        self.spark = spark
        self.step_options = step_options

    def load(self):
        """
        Carga los datos desde una vista registrada en Spark a un destino especificado.
        """
        # Extraer configuraciones
        step_name = self.step_options.get("name")
        origin_view = self.step_options.get("origin_view")

        # Verificar que la vista de origen existe
        if not self.spark.catalog.tableExists(origin_view):
            raise ValueError(
                f"La vista de origen '{origin_view}' no existe. No se puede cargar la tabla '{step_name}'."
            )

        # Leer datos desde la vista
        df = self.spark.sql(f"SELECT * FROM {origin_view}")

        # Escribir datos en el destino
        self.__write_data(df)

    def __write_data(self, df):
        """
        Escribe un DataFrame en el destino especificado según el formato.

        Args:
            df (DataFrame): El DataFrame a guardar.
        """
        step_name = self.step_options.get("name")
        options = self.step_options.get("options", {})

        path = options.get("path")
        data_format = options.get("format")
        mode = options.get("mode", "overwrite")  # Modo predeterminado: overwrite

        # Concatenar path y name para el destino final
        destination = f"{path.rstrip('/')}/{step_name}"

        if data_format == "csv":
            delimiter = options.get("delimiter", ",")  # Delimitador para CSV
            df.write \
                .mode(mode) \
                .option("header", "true") \
                .option("delimiter", delimiter) \
                .csv(destination)
        elif data_format == "parquet":
            df.write \
                .mode(mode) \
                .parquet(destination)
        elif data_format == "excel":
            raise NotImplementedError("El formato 'excel' no está soportado actualmente.")
        else:
            raise ValueError(f"Formato desconocido: {data_format}")
