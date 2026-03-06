# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c4e03707-a69b-4aa7-a8b5-28703d513509",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "31e37be4-cc4d-4ea0-94e0-e4d50b24aa64",
# META       "known_lakehouses": [
# META         {
# META           "id": "c4e03707-a69b-4aa7-a8b5-28703d513509"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install groq

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from groq import Groq

# ==========================================
# 1. SETUP (PASTE YOUR GROQ KEY HERE)
# ==========================================
os.environ["GROQ_API_KEY"] = "gsk_your_groq_api_key_here"
client = Groq()
lakehouse_db = "Gold.dbo"

# ==========================================
# 2. DYNAMIC SCHEMA SCANNER
# ==========================================
print("🔍 Scanning Gold Lakehouse schema...")
try:
    tables = spark.sql(f"SHOW TABLES IN {lakehouse_db}").collect()
    schema_text = "Here is the schema for the university database:\n\n"
    for row in tables:
        t_name = row.tableName
        cols = spark.sql(f"SHOW COLUMNS IN {lakehouse_db}.{t_name}").collect()
        col_names = [c.col_name for c in cols]
        schema_text += f"Table: {lakehouse_db}.{t_name}\nColumns: {', '.join(col_names)}\n\n"
    print("✅ System Ready!")
except Exception as e:
    print(f"❌ Could not read schema: {e}")

# ==========================================
# 3. THE INSTANT CHAT LOOP
# ==========================================
print("\n" + "="*50)
print("🎓 AIVANCITY GROQ AGENT IS ONLINE")
print("Type 'quit' or 'exit' to stop.")
print("="*50 + "\n")

while True:
    # 1. Get user input
    user_question = input("You: ")
    
    if user_question.lower() in ['quit', 'exit', 'stop']:
        print("Agent: Goodbye! Have a great day. 👋")
        break
        
    if not user_question.strip():
        continue
        
    try:
        # 2. Ask Groq for the PySpark SQL
        prompt_1 = f"""
        You are a PySpark SQL expert. 
        {schema_text}
        
        Write a Spark SQL query to answer this user question: "{user_question}"
        
        RULES:
        1. Return ONLY the raw SQL query.
        2. Do not include markdown formatting like ```sql
        3. Do not include any explanations.
        """
        
        sql_response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "user", "content": prompt_1}],
            temperature=0
        )
        
        raw_sql = sql_response.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        print(f"   [⚙️ Running SQL: {raw_sql}]")
        
        # 3. Run the SQL on your Gold Lakehouse
        df_result = spark.sql(raw_sql)
        data_string = df_result.limit(10).toPandas().to_string()
        
        # 4. Ask Groq to turn the data into a human answer
        prompt_2 = f"""
        The user asked: "{user_question}"
        
        You ran a database query and got this data back:
        {data_string}
        
        Write a friendly, professional response answering the user's question using ONLY this data. Do not mention the SQL query itself.
        """
        
        final_response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "user", "content": prompt_2}],
            temperature=0.3
        )
        
        # 5. Print the final answer
        print(f"🤖 Agent: {final_response.choices[0].message.content}\n")
        
    except Exception as e:
        print(f"⚠️ Error: I couldn't process that. It might be a tricky question! (Details: {str(e)[:150]})\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
