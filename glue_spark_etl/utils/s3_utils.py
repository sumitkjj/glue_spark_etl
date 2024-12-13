import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse


class S3Utils:
    # Atributo estático que contiene el cliente de S3
    s3_client = boto3.client('s3')

    @staticmethod
    def parse_s3_path(path: str):
        """
        Dado un path S3 en formato 's3://bucket_name/file_key', separa el nombre del bucket y el file key.

        Args:
            path (str): Ruta completa del archivo en S3.

        Returns:
            tuple: Un tuple con el nombre del bucket y la clave del archivo.
        """
        parsed_url = urlparse(path)

        # El nombre del bucket estará en el host, y el file key en el path (eliminando el '/')
        bucket_name = parsed_url.netloc
        file_key = parsed_url.path.lstrip('/')
        return bucket_name, file_key

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """
        Verifica si un archivo existe en un bucket de S3.

        Args:
            file_path (str): Ruta del archivo en S3.

        Returns:
            bool: True si el archivo existe, False si no.
        """
        try:
            # Extraemos bucket y ruta del archivo
            bucket_name, file_key = S3Utils.parse_s3_path(file_path)

            # Realiza una solicitud HEAD para verificar si el archivo existe
            S3Utils.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    @staticmethod
    def get_file_content(file_path: str) -> str:
        """
        Devuelve el contenido de un archivo de S3.

        Args:
            file_path (str): Ruta del archivo en S3.

        Returns:
            str: Contenido del archivo en S3.
        """
        try:
            # Extraemos bucket y ruta del archivo
            bucket_name, file_key = S3Utils.parse_s3_path(file_path)

            # Obtener el objeto (archivo) desde S3
            response = S3Utils.s3_client.get_object(Bucket=bucket_name, Key=file_key)

            # Leer el contenido del archivo (como texto)
            file_content = response['Body'].read().decode('utf-8')

            return file_content
        except Exception as e:
            raise Exception(f"Error al leer el archivo desde S3: {e}")
