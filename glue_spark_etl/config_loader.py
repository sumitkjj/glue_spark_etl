from pathlib import Path
from utils.s3_utils import S3Utils
import yaml


class ConfigLoader:
    def __init__(self, config_path: str):
        """
        Inicializa el ConfigLoader para manejar archivos de configuración.

        Args:
            config_path (str): Ruta del archivo de configuración (local o S3).
        """
        self.config_path = config_path
        self.config = {}
        self.test = not config_path.startswith("s3://")

    def load_config(self) -> dict:
        """
        Carga el archivo de configuración desde la ruta especificada (local o S3).

        Returns:
            dict: Configuración cargada como un diccionario.

        Raises:
            FileNotFoundError: Si el archivo de configuración no existe.
        """
        if not self.test:
            if not S3Utils.file_exists(self.config_path):
                raise FileNotFoundError(f"El archivo de configuración '{self.config_path}' no existe en S3.")

            # lectura contenido desde S3
            file_content = S3Utils.get_file_content(self.config_path)
        else:
            # Comprobar si el archivo existe en el sistema de archivos local
            file_path = Path(self.config_path)
            if not file_path.is_file():
                raise FileNotFoundError(f"El archivo de configuración '{self.config_path}' no existe.")

            # Leer el contenido del archivo
            with file_path.open("r") as file:
                file_content = file.read()

        # Parsear el contenido como YAML o JSON
        self.config = self.__parse_config(file_content)

        # Validar la configuración cargada
        self.__validate_config()
        return self.config

    @staticmethod
    def __parse_config(content: str) -> dict:
        """
        Parsea el contenido de la configuración a un diccionario.

        Args:
            content (str): Contenido del archivo de configuración.

        Returns:
            dict: Configuración como diccionario.
        """
        try:
            return yaml.safe_load(content)  # Intentar cargar como YAML
        except yaml.YAMLError:
            raise ValueError("Formato de archivo de configuración no válido. Use YAML.")

    def __validate_config(self):
        """
        Valida que la configuración cargada cumpla con los requisitos mínimos.
        """
        if not self.config:
            raise ValueError("La configuración no se pudo cargar o está vacía.")

        steps = self.config.get("steps", [])
        if not steps:
            raise ValueError("La configuración no contiene ningún paso definido.")

        # Validaciones
        for i, step in enumerate(steps):
            # Validaciones comunes
            self.__validate_common_step(step, i)

            # Validaciones específicas por tipo de step
            step_type = step.get("step")
            if step_type == "extract":
                self.__validate_extractor_step(step)
            elif step_type == "transform":
                self.__validate_transform_step(step)
            elif step_type == "load":
                self.__validate_loader_step(step)

        # Validar que existe al menos un paso de extracción
        has_extraction_step = any(step.get("step") == "extract" for step in steps)
        if not has_extraction_step:
            raise ValueError("La configuración debe contener al menos un paso de extracción ('extract').")

    @staticmethod
    def __validate_common_step(step: dict, index: int):
        """
        Valida las propiedades comunes de cada step.

        Args:
            step (dict): Paso a validar.
            index (int): Índice del paso en la configuración.

        Raises:
            ValueError: Si el paso no cumple con las validaciones comunes.
        """
        valid_steps = ["extract", "transform", "load"]
        step_type = step.get("step")

        if step_type not in valid_steps:
            raise ValueError(
                f"El paso #{index + 1} tiene un tipo no válido: '{step_type}'. Los tipos permitidos son {valid_steps}."
            )

        if not step.get("name"):
            raise ValueError(f"El paso #{index + 1} no tiene el campo 'name' definido.")

    def __validate_extractor_step(self, step: dict):
        """
        Valida que las opciones de configuración para un paso de extracción sean correctas.

        Args:
            step (dict): Paso de extracción.

        Raises:
            ValueError: Si falta alguna opción requerida en el paso de extracción o si el path no existe.
        """

        source_type = step.get("type")
        options = step.get("options", {})

        if not source_type:
            raise ValueError(f"El paso de extracción '{step.get('name')}' no tiene definido el campo 'type'.")

        # Validar las opciones necesarias según el tipo de origen
        if source_type == "jdbc":
            required_keys = ["host", "db_type", "db_name", "table", "user_key", "password_key"]
        elif source_type in {"s3", "local"}:
            required_keys = ["path", "format"]
        else:
            raise ValueError(
                f"Tipo de origen desconocido en el paso de extracción '{step.get('name')}': '{source_type}'."
            )

        for key in required_keys:
            if key not in options:
                raise ValueError(
                    f"Falta la opción '{key}' en el paso de extracción '{step.get('name')}' del tipo '{source_type}'."
                )

        # Validar que el path existe para s3 o local
        if source_type in ["s3", "local"]:
            path = options["path"]
            if source_type == "local":
                # Validar que el path local existe
                if not Path(path).is_file():
                    raise ValueError(
                        f"El path especificado '{path}' en el paso de extracción '{step.get('name')}' no existe."
                    )
            elif source_type == "s3" and not self.test:
                if not S3Utils.file_exists(path):
                    raise ValueError(
                        f"El path especificado '{path}' en el paso de extracción '{step.get('name')}' no existe en S3."
                    )

            # Validar que el formato es válido
            valid_formats = ["parquet", "csv", "excel"]
            if options["format"] not in valid_formats:
                raise ValueError(
                    f"Formato inválido '{options['format']}' en el paso de extracción '{step.get('name')}'. "
                    f"Formatos válidos: {', '.join(valid_formats)}"
                )

            # Si el formato es 'csv', validar que se haya especificado el delimitador
            if options["format"] == "csv":
                if "delimiter" not in options:
                    raise ValueError(
                        f"El delimitador es obligatorio para el formato 'csv' en el paso de extracción '{step.get('name')}'."
                    )
        else:
            # Validar el campo db_type
            db_type = options.get("db_type")
            if db_type not in {"mysql", "postgresql", "oracle", "sqlserver"}:
                raise ValueError(
                    f"El valor de 'db_type' en el paso de extracción '{step.get('name')}' es inválido. "
                    "Valores válidos: 'mysql', 'postgresql', 'oracle', 'sqlserver'."
                )


    @staticmethod
    def __validate_transform_step(step: dict):
        """
        Valida que los pasos de transformación tengan el campo 'sql' definido.

        Args:
            step (dict): Paso de transformación.

        Raises:
            ValueError: Si falta el campo 'sql' en el paso de transformación.
        """
        if not step.get("sql"):
            raise ValueError(f"El paso de transformación '{step.get('name')}' no tiene el campo 'sql' definido.")

    def __validate_loader_step(self, step: dict):
        """
        Valida que la configuración de un paso de tipo 'load' cumpla con los requisitos mínimos.

        Args:
            step (dict): Paso de configuración a validar.

        Raises:
            ValueError: Si falta algún parámetro obligatorio o si no es válido.
        """

        # Validar que 'type' y 'origin_view' están presentes
        required_keys = ["type", "origin_view"]
        missing_keys = [key for key in required_keys if key not in step]
        if missing_keys:
            raise ValueError(
                f"El paso de carga '{step.get('name')}' tiene parámetros faltantes: {', '.join(missing_keys)}"
            )

        # Validar que el tipo es válido
        valid_types = ["s3", "local"]
        if step["type"] not in valid_types:
            raise ValueError(
                f"Tipo de destino inválido '{step['type']}' en el paso de carga '{step.get('name')}'. "
                f"Tipos válidos: {', '.join(valid_types)}"
            )

        # Validar que 'options' existe y es un diccionario
        if "options" not in step or not isinstance(step["options"], dict):
            raise ValueError(f"El paso de carga '{step.get('name')}' debe incluir un campo 'options' con parámetros.")

        options = step["options"]

        # Validar que los parámetros requeridos dentro de 'options' están presentes
        required_options_keys = ["path", "format", "mode"]
        missing_options_keys = [key for key in required_options_keys if key not in options]
        if missing_options_keys:
            raise ValueError(
                f"El paso de carga '{step.get('name')}' tiene parámetros faltantes en 'options': "
                f"{', '.join(missing_options_keys)}"
            )

        # Validar que el formato es válido
        valid_formats = ["parquet", "csv", "excel"]
        if options["format"] not in valid_formats:
            raise ValueError(
                f"Formato inválido '{options['format']}' en el paso de carga '{step.get('name')}'. "
                f"Formatos válidos: {', '.join(valid_formats)}"
            )

        # Si el formato es 'csv', validar que se haya especificado el delimitador
        if options["format"] == "csv":
            if "delimiter" not in options:
                raise ValueError(
                    f"El delimitador es obligatorio para el formato 'csv' en el paso de carga '{step.get('name')}'."
                )

        # Validar que el modo de escritura es válido
        valid_modes = ["overwrite", "append"]
        if options["mode"] not in valid_modes:
            raise ValueError(
                f"Modo de escritura inválido '{options['mode']}' en el paso de carga '{step.get('name')}'. "
                f"Modos válidos: {', '.join(valid_modes)}"
            )

        # Validar que el path existe para s3 o local
        path = options["path"]
        source_type = step["type"]
        if source_type == "local":
            # Validar que el path local existe
            if not Path(path).is_file():
                raise ValueError(
                    f"El path: '{path}' especificado en las opciones del paso de extracción '{step.get('name')}' "
                    f"no existe."
                )
        elif source_type == "s3" and not self.test:
            if not S3Utils.file_exists(path):
                raise ValueError(
                    f"El path: '{path}' especificado en las opciones del paso de extracción '{step.get('name')}' "
                    f"no existe en S3."
                )
