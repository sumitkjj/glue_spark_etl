from pyspark.sql import SparkSession


class Transformer:
    def __init__(self, spark: SparkSession, step_options: dict):
        self.spark = spark
        self.step_options = step_options

    def transform(self):
        """
        Ejecuta la transformación especificada en el step_options y guarda el resultado en una vista temporal.

        Returns:
            None: Los datos transformados se cargan en una vista temporal registrada en Spark.
        """
        step_name = self.step_options.get("name")
        sql_query = self.step_options.get("sql")

        # Ejecutar la consulta SQL
        df = self.spark.sql(sql_query)

        # Registrar el DataFrame resultante como una vista temporal
        df.createOrReplaceTempView(step_name)
