from glue_spark_etl.extractors.base_extractor import BaseExtractor


class JDBCExtractor(BaseExtractor):
    """
    Extractor para orígenes de datos JDBC.
    """
    def extract(self):
        step_name = self.step_options.get("name")
        options = self.step_options.get("options", {})

        # Extraer opciones específicas
        host = options.get("host")
        db_type = options.get("db_type")
        db_name = options.get("db_name")
        table = options.get("table")
        user_key = options.get("user_key")
        password_key = options.get("password_key")

        # Construir la URL de conexión JDBC usando la función privada
        jdbc_url = self.__build_jdbc_url(db_type, host, db_name)

        # Leer datos desde JDBC
        self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", user_key) \
            .option("password", password_key) \
            .load() \
            .createOrReplaceTempView(step_name)

    @staticmethod
    def __build_jdbc_url(db_type: str, host: str, db_name: str) -> str:
        """
        Construye la URL JDBC basada en el tipo de base de datos.

        Args:
            db_type (str): Tipo de base de datos (mysql, postgresql, oracle, sqlserver).
            host (str): Dirección del host donde está la base de datos.
            db_name (str): Nombre de la base de datos.

        Returns:
            str: URL de conexión JDBC construida.

        Raises:
            ValueError: Si el tipo de base de datos no es soportado.
        """
        if db_type == "mysql":
            return f"jdbc:mysql://{host}/{db_name}?useSSL=false"
        elif db_type == "postgresql":
            return f"jdbc:postgresql://{host}/{db_name}"
        elif db_type == "oracle":
            return f"jdbc:oracle:thin:@{host}:1521:{db_name}"
        elif db_type == "sqlserver":
            return f"jdbc:sqlserver://{host};databaseName={db_name}"
        else:
            raise ValueError(f"Tipo de base de datos no soportada: '{db_type}'.")
