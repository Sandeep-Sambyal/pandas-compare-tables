import pandas as pd
"""
Creating database connection to connect to 2 tables. Source and destination tables.
"""
source_connection = "Create Source Database conection"
destination_connection = "Create destination Object connection"

source_query = "Select * from SOURCE_DATABASE.SCHEMA.TABLE;"
destination_query = "Select * from DESTINATION_DATABASE.SCHEMA.TABLE;"

# Fetching data from query and putting it in pandas dataframe.
source_df = pd.read_sql_query(source_query, source_connection)
destination_df = pd.read_sql_query(destination_query, destination_connection)

#In case if you want to ignore few columns. 
exclude_columns = ["name", "id", "class"]
destination_df = destination_df.drop(exclude_columns, axis=1)

#Check if columns matches in both the tables:
if len(destination_df.columns) != len(source_df.columns):
    print("FAIL COLUMN MISMATCH")


#If you have Primary Key, then follow below steps to get report generated.
comp = datacompy.Compare(source_df, destination_df, join_columns=primary_key)
with open("datacompy_report.txt", "a", encoding="utf-8") as report_file:
    report_file.write(comp.report())

#If you dont have the primary key, you will have to find mismtaches manually by comparing.
merge_df = pd.merge(source_df, destination_df, how="outer", indicator="Exist")
compare_results = merge_df.loc[merge_df["Exist"] != "both"]
compare_results.to_csv("merge_report.csv", index=False)
