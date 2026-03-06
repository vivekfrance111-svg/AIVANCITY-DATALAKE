# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eef36401-a484-4685-9d60-a68331b698a3",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "31e37be4-cc4d-4ea0-94e0-e4d50b24aa64",
# META       "known_lakehouses": [
# META         {
# META           "id": "eef36401-a484-4685-9d60-a68331b698a3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

dictionary_data = []
tables = spark.catalog.listTables()

# Loop through all 272 tables
for table in tables:
    if table.tableType != "VIEW": 
        
        # This checks if the table has at least 1 row of data
        if spark.table(table.name).limit(1).count() > 0:
            
            # If it has data, grab the column names and data types
            schema = spark.table(table.name).schema
            for field in schema.fields:
                dictionary_data.append({
                    "Table_Name": table.name,
                    "Column_Name": field.name,
                    "Data_Type": field.dataType.typeName()
                })

# Display the final, clean data dictionary (No empty tables!)
df_dictionary = spark.createDataFrame(pd.DataFrame(dictionary_data))
display(df_dictionary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F
import re

# --- FIX 1: Prevent crashes from "Ancient" OData Dates ---
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# Function to create human readable descriptions
def create_human_description(table_name, column_name):
    clean_col = re.sub(r'(?<!^)(?=[A-Z])', ' ', column_name).replace('_', ' ')
    clean_tbl = re.sub(r'(?<!^)(?=[A-Z])', ' ', table_name).replace('_', ' ')
    return f"Contains {clean_col.lower()} information for the {clean_tbl.lower()}."

dictionary_data = []
tables = spark.catalog.listTables()

print("Scanning tables, profiling data, and generating descriptions... (Optimized Mode)")

for table in tables:
    if table.tableType != "VIEW": 
        try:
            df = spark.table(table.name)
            
            # Fast check: Does the table have data?
            if df.limit(1).count() > 0:
                schema = df.schema
                
                # Grab a sample of 1000 rows
                sample_df = df.limit(1000).cache()
                
                # --- FIX 2: SUPER FAST OPTIMIZATION ---
                # Instead of checking columns one by one, we ask Spark to check 
                # ALL columns for this table in ONE single lightning-fast action.
                exprs = [F.max(F.when(F.col(field.name).isNotNull(), 1).otherwise(0)).alias(field.name) for field in schema.fields]
                
                # This returns a single row with 1s (Has Data) and 0s (Null) for every column
                has_data_row = sample_df.agg(*exprs).collect()[0]
                
                for field in schema.fields:
                    col_name = field.name
                    
                    # Read the instant 1 or 0 result we just generated
                    # (Fallback to 0 if something goes weird with the column name)
                    is_active = has_data_row.asDict().get(col_name, 0)
                    data_status = "Has Data" if is_active == 1 else "Mostly/All Null"
                    
                    # Generate the description
                    human_desc = create_human_description(table.name, col_name)
                    
                    # Build the row
                    dictionary_data.append({
                        "Table_Name": table.name,
                        "Column_Name": col_name,
                        "Data_Type": field.dataType.typeName(),
                        "Data_Status": data_status,
                        "Description": human_desc
                    })
                
                # Clear memory to keep the cluster fast
                sample_df.unpersist()
                
        except Exception as e:
            # If a table is entirely broken, skip it but don't crash the whole notebook!
            print(f"Skipping table {table.name} due to an unexpected error.")

# Display the massive, upgraded data dictionary
df_dictionary = spark.createDataFrame(pd.DataFrame(dictionary_data))
display(df_dictionary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
