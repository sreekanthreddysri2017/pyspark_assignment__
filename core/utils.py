from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import re
from functools import reduce
from core.utils import *

def SparkSession():
    spark = SparkSession.builder.appName("Pyspark Assignment").getOrCreate()
    return spark

#creating a table and referred as  table 1
def create_table(spark):
    table_schema = StructType([
                        StructField("Product_Name", StringType(), True),
                        StructField("Issue_Date", StringType(), True),
                        StructField("Price", IntegerType(), True),
                        StructField("Brand", StringType(), True),
                        StructField("Country", StringType(), True),
                        StructField("Product_Number", StringType(), True) ])
    table_data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001"),
              ("Refrigerator", 1648770999000 , 35000, "    LG", None, "0002"),
              ("Air Cooler", 1648770948000, 45000, "     Voltas", None, "0003")]

#creating the dataframe with table data and schema
    table_df = spark.createDataFrame(data=table_data, schema=table_schema)
    return table_df

def timestamp_to_unixTime(table_df):
#unix_timestamp is a string which is in seconds format
#to convert the given milli-seconds(Issue_Date column) to unix_timestamp (seconds) we divide it by 1000
#this divided value is passed to fom_unixtime function to convert it into date and time stamp.
    df1=table_df.withColumn("issue_date_timestamp", from_unixtime( (col("Issue_Date")/1000) ))
    return df1

def convert_date(table_df):
#converting timestamp into date and time format separate columns
    df2=table_df.withColumn("Date",to_date(col("issue_date_timestamp")))\
    .withColumn("Time", date_format("issue_date_timestamp", 'HH:mm:ss'))
    return df2

def trim_spaces(df2):
#removing the spaces found in "Brand" column values
    df3=df2.withColumn("Brand",trim(col("Brand")))
    return df3

def replace_null_with_empty_values(df3):
#replacing the null values in the column "Country" by empty value
    df4=df3.na.fill(value="")
    return df4

#creating another table named table 2
def creating_table_2(spark):
    table2_schema = StructType([
                        StructField("SourceId", IntegerType(), True),
                        StructField("TransactionNumber", IntegerType(), True),
                        StructField("Language", StringType(), True),
                        StructField("ModelNumber",IntegerType(),True),
                        StructField("StartTime", StringType(), True),
                        StructField("Product_Number", StringType(), True)])
    table2_data = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),
                (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000","0002"),
                (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000","0003")]
    table2_df=spark.createDataFrame(data=table2_data,schema=table2_schema)
    return table2_df

#converting column header of table 2 from camel case to snake case
def column_case_conversion(table2_df):
    column_name_list = table2_df.columns
    df_column_name = reduce(lambda table2_df, i: table2_df.withColumnRenamed(i, re.sub(r'(?<!^)(?=[A-Z])', '_', i).lower()),column_name_list, table2_df)
    return df_column_name

#converting timestamp in table 2 to unix_timestamp
def timestamp_to_unix_timestamp(table2_df):
    df3=table2_df.withColumn("timestamp",to_timestamp(col("start_time")))\
            .withColumn("start_time_ms",unix_timestamp(col("timestamp")))
    return df3

#joining table 1 and table 2 using nner join
def join_table(table_df,table2_df):
    df4=table_df.join(table2_df, table_df.Product_Number==table2_df.Product_Number, "inner")
    return df4

#filtering records in join table based on language "EN"
def filter_records(df4):
    df5=df4.filter(df4.Language=="EN")
    return df5