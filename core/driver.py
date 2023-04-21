from core.utils import *

#creating a Spark Session
spark=SparkSession()

#creating a table referred as table 1
table1=create_table(spark)

#converting timestamp to unix timestamp format
time_to_unix=timestamp_to_unixTime(table1)

#converting timestamp to date format
timestamp_to_date=convert_date(table1)

#triming the white spaces present in a "Brand" column
trim_white_spaces=trim_spaces(table1)

#replacing null values of columns with empty values
null_with_empty_values=replace_null_with_empty_values(table1)

#creating another table and referred as table 2
table2=creating_table_2(spark)

#converting column names with camel case with snake case
column_case_conversion(table2)

#converting timestamp to unix_time format
timestamp_to_unix=timestamp_to_unix_timestamp(table2)

#joining two tables table 1 and table 2 based on inner join
join_tables=join_table(table1,table2)

#filtering the join table records based on language "EN"
filter_EN_records=filter_records(join_tables)