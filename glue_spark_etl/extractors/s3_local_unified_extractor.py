from glue_spark_etl.extractors.base_extractor import BaseExtractor


class S3LocalUnifiedExtractor(BaseExtractor):
    """
    Extractor unificado para orígenes de datos de S3 y local.
    """
    def extract(self):
        step_name = self.step_options.get("name")
        path = self.step_options.get("options").get("path")
        data_format = self.step_options.get("options").get("format").lower()

        if data_format == "csv":
            delimiter = self.step_options.get("options").get("delimiter")

            # Leer archivo CSV
            self.spark.read\
                .format("csv")\
                .option("header", "true") \
                .option("sep", delimiter)\
                .load(path)\
                .createOrReplaceTempView(step_name)
        elif data_format == "excel":
            # Lanzar un error porque Excel no está soportado
            raise NotImplementedError("El formato 'excel' no está soportado actualmente.")
        elif data_format == "parquet":
            # Leer archivo Parquet
            self.spark.read.parquet(path).createOrReplaceTempView(step_name)
        else:
            raise ValueError(f"Formato '{data_format}' no soportado para S3.")
