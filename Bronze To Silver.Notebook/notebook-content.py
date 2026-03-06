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
# META         },
# META         {
# META           "id": "47677fc2-1732-41c4-9b74-fc56f489847f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================================
# BRONZE SHELL 2 — PARALLEL PROCESSOR (ULTRA-SAFE VERSION)
# Fix: Removed dropna and groupBy. Silver now acts purely as 
# a sanitizer, trusting the Bronze row counts 100%.
# ==============================================================
import concurrent.futures
from datetime import datetime
from pyspark.sql import functions as F

bronze_lh = "Bronze"
silver_lh = "Silver"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Bronze -> Silver Pipeline")
print(f"Run ID : {run_id}")

# ==============================================================
# 1. THE SAFE CLEANING ENGINE
# ==============================================================
def apply_silver_cleaning(df, primary_key_col, table_name):
    # Step 1: Standardize column names (lowercase + underscores)
    for col in df.columns:
        clean_col_name = col.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col, clean_col_name)

    # Step 2: Drop ONLY 100% exact duplicate rows (clones)
    df = df.dropDuplicates()

    # Step 3: Text and date cleaning (The Vacuum Cleaner)
    clean_exprs = []
    for field in df.schema.fields:
        c_name = field.name
        
        if str(field.dataType) == "StringType()":
            # Trim invisible characters and turn empty text boxes into real NULLs
            clean_text = F.trim(F.regexp_replace(F.col(c_name), r"[\n\r\t]", " "))
            clean_exprs.append(
                F.when(clean_text == "", F.lit(None))
                 .otherwise(clean_text).alias(c_name)
            )
            
        elif str(field.dataType) in ["DateType()", "TimestampType()"]:
            # Neutralize ancient dates (pre-1900)
            clean_exprs.append(
                F.when(F.year(F.col(c_name)) < 1900, F.lit(None))
                 .otherwise(F.col(c_name)).alias(c_name)
            )
            
        else:
            # Leave numbers and booleans exactly as they are
            clean_exprs.append(F.col(c_name))
    
    # Apply all column rules simultaneously for speed
    df = df.select(*clean_exprs)

    # Step 4: Add Audit Metadata
    df = df.withColumn("silver_processed_at", F.current_timestamp())
    df = df.withColumn("pipeline_run_id", F.lit(run_id))
    
    return df

# ==============================================================
# 2. DISCOVER ALL BRONZE TABLES
# ==============================================================
print(f"\nScanning Bronze for tables...")
try:
    tables_df       = spark.sql(f"SHOW TABLES IN {bronze_lh}.dbo")
    all_table_names = [row.tableName for row in tables_df.collect()]
    print(f"   Found {len(all_table_names)} tables in Bronze")
except Exception as e:
    print(f"   ERROR: {e}")
    all_table_names = []

tables_to_process = {}
for table in all_table_names:
    try:
        cols_df   = spark.sql(f"SHOW COLUMNS IN {bronze_lh}.dbo.{table}")
        first_col = cols_df.collect()[0].col_name
        tables_to_process[table] = first_col
    except:
        pass

print(f"   Processing {len(tables_to_process)} tables...")

# ==============================================================
# 3. MULTITHREADED PROCESSING LOGIC
# ==============================================================
def process_single_table(table_name, primary_key):
    try:
        bronze_df    = spark.table(f"{bronze_lh}.dbo.{table_name}")
        bronze_count = bronze_df.count()

        if bronze_count == 0:
            # Empty table — write empty Silver to keep schema consistent
            bronze_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"{silver_lh}.dbo.{table_name}")
            return f"   EMPTY  {table_name:55s} (0 rows)"

        # Clean the data using the safe function
        silver_df    = apply_silver_cleaning(bronze_df, primary_key_col=primary_key, table_name=table_name)
        silver_count = silver_df.count()

        # Save to Silver Lakehouse
        silver_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{silver_lh}.dbo.{table_name}")

        ratio  = silver_count / bronze_count if bronze_count > 0 else 0

        # We set the warning threshold much higher now (95%) because we expect 100% retention
        if ratio < 0.95:
            status = "WARNING"
        else:
            status = "OK    "

        return (f"   {status}  {table_name:55s} "
                f"Bronze={bronze_count:,}  Silver={silver_count:,}  ({ratio:.0%})")

    except Exception as e:
        return f"   FAILED {table_name} — {str(e)[:80]}"

# ==============================================================
# 4. EXECUTION ENGINE
# ==============================================================
ok_count    = 0
warn_count  = 0
empty_count = 0
fail_count  = 0

# max_workers=4 protects your free tier limits!
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(process_single_table, t, pk): t
        for t, pk in tables_to_process.items()
    }
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        print(result)
        if "FAILED"  in result: fail_count  += 1
        elif "EMPTY"  in result: empty_count += 1
        elif "WARNING" in result: warn_count += 1
        else:                    ok_count    += 1

print(f"\nBronze -> Silver Complete")
print(f"   OK      : {ok_count}")
print(f"   WARNING : {warn_count}  (If any, these are 100% exact clones deleted)")
print(f"   EMPTY   : {empty_count}")
print(f"   FAILED  : {fail_count}")
print(f"   Run ID  : {run_id}")

# ==============================================================
# 5. FINAL SPOT CHECK
# ==============================================================
print(f"\nSpot check on critical tables:")
critical_tables = [
    "Planification_SeanceApprenant2526_S1",
    "Planification_SeanceApprenant2526_S2",
    "Pedagogie_Note2526",
    "Pedagogie_NotePeriode_2526",
    "Candidature_DossierCandidature",
    "Inscription_ApprenantInscription",
]
print(f"{'Table':55s} {'Bronze':>10} {'Silver':>10} {'Ratio':>8}")
print("-" * 90)
for table in critical_tables:
    try:
        b = spark.table(f"{bronze_lh}.dbo.{table}").count()
        s = spark.table(f"{silver_lh}.dbo.{table}").count()
        r = s / b if b > 0 else 0
        flag = "OK" if r >= 0.95 else "WARNING LOW"
        print(f"   {table:55s} {b:>10,} {s:>10,} {r:>7.0%}  {flag}")
    except Exception as e:
        print(f"   {table:55s} ERROR: {str(e)[:40]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 2 — DATA QUALITY AUDITOR
# ==============================================================
import random

bronze_lh = "Bronze"
silver_lh = "Silver"

print("Data Quality Auditor")

try:
    tables_df         = spark.sql(f"SHOW TABLES IN {silver_lh}.dbo")
    all_silver_tables = [row.tableName for row in tables_df.collect()]

    if not all_silver_tables:
        print("No tables found in Silver.")
    else:
        sample_size  = min(5, len(all_silver_tables))
        random_tables = random.sample(all_silver_tables, sample_size)

        print(f"   Randomly auditing {sample_size} of {len(all_silver_tables)} tables\n")
        print("=" * 60)

        for table in random_tables:
            print(f"AUDIT: {table.upper()}")

            silver_df    = spark.table(f"{silver_lh}.dbo.{table}")
            silver_count = silver_df.count()

            try:
                bronze_df    = spark.table(f"{bronze_lh}.dbo.{table}")
                bronze_count = bronze_df.count()
                dropped_rows = bronze_count - silver_count
            except:
                bronze_count = "Unknown"
                dropped_rows = "Unknown"

            distinct_count      = silver_df.dropDuplicates().count()
            remaining_dupes     = silver_count - distinct_count
            has_timestamp       = "silver_processed_at" in silver_df.columns
            has_run_id          = "pipeline_run_id" in silver_df.columns

            print(f"   Bronze rows (raw)  : {bronze_count}")
            print(f"   Silver rows (clean): {silver_count}")

            if isinstance(dropped_rows, int) and dropped_rows > 0:
                print(f"   Rows removed       : {dropped_rows}")
            elif isinstance(dropped_rows, int) and dropped_rows == 0:
                print(f"   Rows removed       : 0 (already clean)")

            print(f"   Duplicates         : {remaining_dupes} {'OK' if remaining_dupes == 0 else 'WARNING'}")
            print(f"   Timestamp column   : {'Present' if has_timestamp else 'Missing'}")
            print(f"   Run ID column      : {'Present' if has_run_id else 'Missing'}")
            print("-" * 60)

except Exception as e:
    print(f"Auditor error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
