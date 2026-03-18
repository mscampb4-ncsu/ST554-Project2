from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    
    def __init__(self, df):
        """
        Initialize SparkDataCheck with one attribute:
        df - a dataframe
        """
        self.df = df

    @classmethod
    def load_pyspark(self, spark, filePath: str):
        """
        Loads the class via spark using the following parameters:
        self - this instance of SparkDataCheck
        spark - a SparkSession instance
        filePath - a string file path to the destination file.
        """
        df = spark.read.format("csv").load(filePath)
        sdc = self(df)
        return sdc
    
    @classmethod
    def load_pandas(self, spark, pandasDf):
        """
        Loads the class via spark using the following parameters:
        self - this instance of SparkDataCheck
        spark - a SparkSession instance
        pandasDf - a pandas DataFrame
        """
        df = spark.createDataFrame(pandasDf)
        sdc = self(df)
        return sdc
    
        