from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import re
from functools import reduce
from core.utils import *

#spark = SparkSession.builder.appName("Pyspark Assignment").getOrCreate()

#creating the table1

input_Schema = StructType([
            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand", StringType(), True),
            StructField("user_Country", StringType(), True),
            StructField("Product_number  ", StringType(), True)
        ])
input_data = [("Washing Machine", "1648770933000",20000, "Samsung", "india","001"),
                      ("Refrigerator", "1648770999000",35000, " LG", "null","002"),
                      ("Air Cooler", "1648770948000",45000, "  Voltas", "null","003")]
spark=SparkSession.builder.appName("Pyspark Assignment").getOrCreate()
table1_df=create_table(spark)
#table1_df = spark.createDataFrame(data=input_data, schema=input_Schema)
table1_df.show()

#creating the table2
expected_schema = StructType([

            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand ", StringType(), True),
            StructField("user_Country", StringType(), True),
            StructField("Product_number  ", StringType(), True),
            StructField("equal_time", StringType(), True),
            StructField("date  ", StringType(), True),
        ])
expected_data = [
            ("Washing Machine", "1648770933000",20000, "Samsung", "india", "001", "2022-04-01 05:25:33",
             "04-01-2022"),
            ("Refrigerator", "1648770999000",35000, " LG", "null", "002", "2022-04-01 05:26:39", "04-01-2022"),
            ("Air Cooler", "1648770948000",45000, "  Voltas", "null", "003", "2022-04-01 05:25:48", "04-01-2022")]

table2_df=creating_table_2(spark)
#table2_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
table2_df.show()

#function to convert timestamp to unix_timestamp format
df1=timestamp_to_unixTime(table1_df)
df1.show()

#function to convert timestamp in table to date format
df2=convert_date(table1_df)
df2.show()

#removing the white spaces in the "Brand" column values
df3=trim_spaces(table1_df)
df3.show()

#replacing the null values in the table with empty values
df4=replace_null_with_empty_values(table1_df)
df4.show()

#converting the column case from camel case to snake case
df_column_name=column_case_conversion(table2_df)
df_column_name.show()

#converting the timestamp format to unix_timestamp format
df3=timestamp_to_unix_timestamp(table2_df)
df3.show()

#joining the two tables
df4=join_table(table1_df,table2_df)
df4.show()

#filtering records in join table based on language "EN"
df5=filter_records(df4)
df5.show()