from setuptools import setup, find_packages

setup(
    name="glue_spark_etl",
    version="0.1.0",
    description="Glue Spark ETL",
    author="Sumit Kumar Jethani Jethani",
    author_email="sumitkumar_jethani@hotmail.com",
    license="MIT",
    packages=find_packages(exclude=("tests", "tests.*")),
    install_requires=[
        "PyYAML==6.0.2",
        "boto3",
        "pyspark==3.5.2"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)
