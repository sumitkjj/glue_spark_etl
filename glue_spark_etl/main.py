from config_loader import ConfigLoader
from driver import SparkDriver

if __name__ == "__main__":
    """
    config_path = "s3://skj-datalake/configs/trial.yml"
    config = ConfigLoader(config_path).load_config()
    print(f"Config cargada: {config}")
    """

    """
    spark_config_path = "s3://skj-datalake/configs/properties.conf"
    spark_driver = SparkDriver(test=False, spark_config_path=spark_config_path)
    for item in spark_driver.spark_session.sparkContext.getConf().getAll(): print(item)
    """
    print("Hello World!")





