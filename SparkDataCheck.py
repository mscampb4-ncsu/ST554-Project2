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
        Loads the class via spark. Note that headers are assumed to be
        true.
        self - this instance of SparkDataCheck
        spark - a SparkSession instance
        filePath - a string file path to the destination file.
        Returns an instance of this class.
        """
        df = spark.read.format("csv").option("header", "true").load(filePath)
        sdc = self(df)
        return sdc
    
    @classmethod
    def load_pandas(self, spark, pandasDf):
        """
        Loads the class via spark using the following parameters:
        self - this instance of SparkDataCheck
        spark - a SparkSession instance
        pandasDf - a pandas DataFrame
        Returns an instance of this class.
        """
        df = spark.createDataFrame(pandasDf)
        sdc = self(df)
        return sdc
    
    def validate_numeric(self, col: str, lower = None, upper = None):
        """
        Validates a specified numeric numeric column
        col - a string representing the name of the relevant column
        lower - a specified lower bound for acceptable values
        upper - a specified upper bound for acceptable values
        Returns a dataframe with a column of boolean values appended.
        """
        df = self.df
        column = F.col(col)
        
        #Check if the supplied column is an eligible type
        eligibleTypes = ["float", "int", "longint", "bigint", "double", "integer"]
        dataTypes = dict(df.dtypes)
        if not dataTypes[col] in (eligibleTypes):
            print("Please provide a numeric column.")
            return df
        
        #Check if at least one of the lower or upper bound is provided
        #If so, perform the requisite validation
        if lower is None:
            if upper is None:
                print("Please provide at least one bound.")
                return df
            else:
                return df.withColumn("passes_validation", column <= upper)
        elif upper is None:
            if lower is None:
                print("Please provide at least one bound.")
                return df
            else:   
                return df.withColumn("passes_validation", column >= lower)
        else:
            return df.withColumn("passes_num_validation", column.between(lower, upper))
            
    def validate_string(self, col, levels):
        """
        Validates a specified string column against a list of levels
        col - a string representing the name of the relevant column
        levels - a list of strings representing each level to check against
        Returns a dataframe with a column of boolean values appended.
        """
        df = self.df
        column = F.col(col)
        
        #Check if the supplied column is an eligible type
        eligibleTypes = ["str", "string"]    
        dataTypes = dict(df.dtypes)
        if not dataTypes[col] in (eligibleTypes):
            print("Please provide a string column.")
            return df
        
        #Check the column against the user-specified levels and return true if an entry matches a level
        return df.withColumn("passes_str_validation", column.isin(levels))
                  
    def denote_null_values(self, col):
        """
        Checks a specified column for null values
        col - a string representing the name of the relevant column
        """
        df = self.df
        column = F.col(col)
                  
        return df.withColumn("has_null_value", column.isNull())
                  
    def get_min_max(self, col = None, groupCol = None):
        """
        Gets the minimum and maximum of a numeric column.
        col - (optional) the string name of the column of which to get the minimum and maximum.
        groupCol - (optional) the string name of the column of which to group by.
        Returns a pandas dataframe.
        """
        df = self.df
                  
        #Check if the supplied column is an eligible type
        eligibleTypes = ["float", "int", "longint", "bigint", "double", "integer"]    
        dataTypes = dict(df.dtypes)
        if not dataTypes[col] in (eligibleTypes):
            print("Please provide a numeric column.")
            return df
        
        #Check if a column is supplied, if not, return the minimum and maximum of all numeric columns
        if col == None:
            #Select all the numeric columns and get the requisite summaries
            colToSelect = [x for x in df.columns if df[x].dtype in eligibleTypes]
            if group_col == None:
                pdDf = df.select(colToSelect) \
                .min() \
                .max() \
                .toPandas()
            else:
                pdDf = df.select(colToSelect) \
                .groupBy(groupCol) \
                .min() \
                .max() \
                .toPandas()
        else:
            if group_col == None:
                pdDf = df.select(col) \
                .min() \
                .max() \
                .toPandas()
            else:
                pdDf = df.select(col) \
                .groupBy(groupCol) \
                .min() \
                .max() \
                .toPandas()
         
        return pdDf
    
    def get_count(self, col, col2 = None):
                  
        df = self.df
        
        eligibleTypes = ["str"]    
        dataTypes = dict(df.dtypes)
        if not dataTypes[col] in (eligibleTypes):
            print("Please provide a string column.")
            return df
        
        if col2 == None:
            pdDf = df.select(col) \
            .agg(count(col)) \
            .toPandas()
        else:
            if not dataTypes[col2] in (eligibleTypes):
                print("Please provide a string column for the grouping variable.")
                return df
            pdDf = df.select([col, col2]) \
            .agg(count(col), count(col2)) \
            .toPandas()
        
        return pdDf