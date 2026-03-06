# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "47677fc2-1732-41c4-9b74-fc56f489847f",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "31e37be4-cc4d-4ea0-94e0-e4d50b24aa64",
# META       "known_lakehouses": [
# META         {
# META           "id": "47677fc2-1732-41c4-9b74-fc56f489847f"
# META         },
# META         {
# META           "id": "c4e03707-a69b-4aa7-a8b5-28703d513509"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================================
# SHELL 1 — DIM_STUDENT
# ==============================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("DIM_STUDENT")

contact_df    = spark.table(f"{silver_lh}.dbo.Candidature_Contact")
dossier_df    = spark.table(f"{silver_lh}.dbo.Candidature_DossierCandidature")
enrollment_df = spark.table(f"{silver_lh}.dbo.Inscription_ApprenantInscription")

print(f"   Candidature_Contact           : {contact_df.count():,} rows")
print(f"   Candidature_DossierCandidature: {dossier_df.count():,} rows")
print(f"   Inscription_ApprenantInscription: {enrollment_df.count():,} rows")

dim_student = (
    contact_df.alias("c")
    .join(dossier_df.alias("d"),
          F.col("c.candidature_id") == F.col("d.candidature_id"), "left")
    .join(enrollment_df.alias("e"),
          F.col("d.candidature_inscription_apprenant_code") == F.col("e.apprenant_code"), "left")
    .select(
        F.col("c.candidature_id")                          .alias("student_key"),
        F.col("c.candidature_code")                        .alias("candidature_code"),
        F.col("e.apprenant_code")                          .alias("student_code"),
        F.col("e.apprenant_numeroine")                     .alias("ine_number"),
        F.col("c.candidat_nom_officiel")                   .alias("last_name"),
        F.col("c.candidat_prenom_officiel")                .alias("first_name"),
        F.col("c.candidat_nom_usage")                      .alias("last_name_usage"),
        F.col("c.candidat_prenom_usage")                   .alias("first_name_usage"),
        F.col("c.candidat_civilite_libelle")               .alias("title"),
        F.col("d.candidat_mailpersonnel")                  .alias("email_personal"),
        F.col("e.apprenant_mailpersonnel")                 .alias("email_personal_enrolled"),
        F.col("e.apprenant_mailecole")                     .alias("email_school"),
        F.col("d.candidat_telephonemobile")                .alias("phone_mobile"),
        F.col("d.candidat_telephonefixe")                  .alias("phone_landline"),
        F.col("c.candidat_naissance_date")                 .alias("birth_date"),
        F.col("c.candidat_naissance_ville")                .alias("birth_city"),
        F.col("c.candidat_naissance_pays")                 .alias("birth_country"),
        F.col("c.candidat_nationalite1")                   .alias("nationality"),
        F.col("d.candidat_adresse_adresse1")               .alias("address_line1"),
        F.col("d.candidat_adresse_codepostal")             .alias("address_postcode"),
        F.col("d.candidat_adresse_ville")                  .alias("address_city"),
        F.col("d.candidat_adresse_pays")                   .alias("address_country"),
        F.col("c.programme_id")                            .alias("programme_id"),
        F.col("c.programme_libelle")                       .alias("programme_name"),
        F.col("c.programme_acronyme")                      .alias("programme_acronym"),
        F.col("c.entite_libelle")                          .alias("school_entity"),
        F.col("c.campus_libelle")                          .alias("campus"),
        F.col("c.candidature_etat_code")                   .alias("candidature_status_code"),
        F.col("c.candidature_etat_libelle")                .alias("candidature_status_label"),
        F.col("c.candidature_voieadmission_libelle")       .alias("admission_track"),
        F.col("d.candidature_decision")                    .alias("admission_decision"),
        F.col("d.candidature_avancement")                  .alias("candidature_progress"),
        F.col("e.inscription_etatinscription_libelle")     .alias("enrollment_status"),
        F.col("e.inscription_dateinscription")             .alias("enrollment_date"),
        F.col("e.inscription_dateadmission")               .alias("admission_date"),
        F.col("e.inscription_datesortie")                  .alias("exit_date"),
        F.col("e.inscription_regime_libelle")              .alias("study_regime"),
        F.col("e.inscription_voieentree_libelle")          .alias("entry_track"),
        F.col("e.inscription_baccalaureat_libelle")        .alias("baccalaureate_type"),
        F.col("e.inscription_baccalaureat_annee")          .alias("baccalaureate_year"),
        F.col("e.inscription_baccalaureat_mention_libelle").alias("baccalaureate_mention"),
        F.col("d.paiementfrais_montant")                   .alias("registration_fee_amount"),
        F.col("d.paiementfrais_date")                      .alias("registration_fee_date"),
        F.col("d.candidature_inscription_acompte_montant") .alias("deposit_amount"),
        F.current_timestamp()                              .alias("gold_updated_at"),
        F.lit(run_id)                                      .alias("pipeline_run_id"),
    )
    .dropDuplicates(["student_key"])
)

# ==============================================================
# MERGE (upsert) — update changed rows, insert new ones
# ==============================================================
output_path = f"{gold_lh}.dbo.DIM_STUDENT"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(dim_student.alias("source"),
               "target.student_key = source.student_key") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    dim_student.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

# Row count validation
silver_count = contact_df.count()
gold_count   = spark.table(output_path).count()
ratio        = gold_count / silver_count if silver_count > 0 else 0
print(f"\nSUCCESS: DIM_STUDENT")
print(f"   Records     : {gold_count:,}")
print(f"   Columns     : {len(dim_student.columns)}")
print(f"   Silver/Gold : {ratio:.0%} {'OK' if ratio >= 0.8 else 'WARNING low ratio'}")
print(f"   Run ID      : {run_id}")
display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 2 — FACT_FINANCE
# ==============================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("FACT_FINANCE")

facture_df = spark.table(f"{silver_lh}.dbo.Finance_FactureNonLettree")
client_df  = spark.table(f"{silver_lh}.dbo.Finance_Client")

print(f"   Finance_FactureNonLettree: {facture_df.count():,} rows")
print(f"   Finance_Client           : {client_df.count():,} rows")

fact_finance = (
    facture_df.alias("f")
    .join(client_df.alias("c"),
          F.col("f.client_code") == F.col("c.client_code"), "left")
    .select(
        F.col("f.facture_id")                          .alias("facture_key"),
        F.col("f.facture_numero")                      .alias("invoice_number"),
        F.col("c.client_id")                           .alias("client_key"),
        F.col("f.client_code")                         .alias("client_code"),
        F.col("c.individu_id")                         .alias("student_key"),
        F.col("f.apprenant_code")                      .alias("student_code"),
        F.col("f.client_nomusage")                     .alias("client_last_name"),
        F.col("f.client_prenomusage")                  .alias("client_first_name"),
        F.col("f.client_nature")                       .alias("client_nature"),
        F.col("c.client_type")                         .alias("client_type"),
        F.col("c.client_civilitelibelle")              .alias("client_title"),
        F.col("f.client_mail")                         .alias("client_email"),
        F.col("f.client_telephonemobile")              .alias("client_phone"),
        F.col("c.clientdouteux")                       .alias("is_doubtful_client"),
        F.col("f.apprenant_nom_usage")                 .alias("student_last_name"),
        F.col("f.apprenant_prenom_usage")              .alias("student_first_name"),
        F.col("f.inscription_etatinscription_code")    .alias("enrollment_status_code"),
        F.col("f.inscription_etatinscription_libelle") .alias("enrollment_status"),
        F.col("f.inscription_voieentree_libelle")      .alias("entry_track"),
        F.col("f.programme_id")                        .alias("programme_id"),
        F.col("f.programme_code")                      .alias("programme_code"),
        F.col("f.programme_libelle")                   .alias("programme_name"),
        F.col("f.campus_id")                           .alias("campus_id"),
        F.col("f.campus_libelle")                      .alias("campus_name"),
        F.col("f.campus_code")                         .alias("campus_code"),
        F.col("f.marque_libelle")                      .alias("brand"),
        F.col("f.societefacturation_id")               .alias("billing_entity_id"),
        F.col("f.societefacturation_libelle")          .alias("billing_entity_name"),
        F.col("f.societefacturation_code")             .alias("billing_entity_code"),
        F.col("f.facture_date")                        .alias("invoice_date"),
        F.col("f.facture_montant")                     .alias("invoice_amount"),
        F.current_timestamp()                          .alias("gold_updated_at"),
        F.lit(run_id)                                  .alias("pipeline_run_id"),
    )
    .dropDuplicates(["facture_key"])
)

output_path = f"{gold_lh}.dbo.FACT_FINANCE"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(fact_finance.alias("source"),
               "target.facture_key = source.facture_key") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    fact_finance.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

silver_count = facture_df.count()
gold_count   = spark.table(output_path).count()
ratio        = gold_count / silver_count if silver_count > 0 else 0
print(f"\nSUCCESS: FACT_FINANCE")
print(f"   Records     : {gold_count:,}")
print(f"   Columns     : {len(fact_finance.columns)}")
print(f"   Silver/Gold : {ratio:.0%} {'OK' if ratio >= 0.8 else 'WARNING low ratio'}")
print(f"   Run ID      : {run_id}")
display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 3 — DIM_COURSE (derived from FACT_ATTENDANCE)
# Must run AFTER Shell 7 (FACT_ATTENDANCE)
# ==============================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

gold_lh = "Gold"
run_id  = datetime.now().strftime("%Y%m%d_%H%M%S")

print("DIM_COURSE")

fa = spark.table(f"{gold_lh}.dbo.FACT_ATTENDANCE") \
    .filter(F.col("course_id").isNotNull()) \
    .select(
        "course_id", "course_code", "course_name",
        "sequence_id", "sequence_name",
        "programme_id", "programme_name", "programme_acronym",
        "entity_name", "entity_name_alt",
        "campus_id", "campus_name", "campus_code",
        "session_type_code", "session_type",
        "delivery_mode", "is_face_to_face", "is_in_person", "is_remote",
        "academic_year",
    )

try:
    fg = spark.table(f"{gold_lh}.dbo.FACT_GRADES") \
        .filter(F.col("course_id").isNotNull()) \
        .select(
            "course_id", "course_code", "course_name",
            F.lit(None).cast("string").alias("sequence_id"),
            F.lit(None).cast("string").alias("sequence_name"),
            "programme_id", "programme_name", "programme_acronym",
            F.lit(None).cast("string").alias("entity_name"),
            F.lit(None).cast("string").alias("entity_name_alt"),
            F.lit(None).cast("string").alias("campus_id"),
            F.lit(None).cast("string").alias("campus_name"),
            F.lit(None).cast("string").alias("campus_code"),
            F.lit(None).cast("string").alias("session_type_code"),
            F.lit(None).cast("string").alias("session_type"),
            F.lit(None).cast("string").alias("delivery_mode"),
            F.lit(None).cast("string").alias("is_face_to_face"),
            F.lit(None).cast("string").alias("is_in_person"),
            F.lit(None).cast("string").alias("is_remote"),
            "academic_year",
        )
    combined = fa.unionByName(fg)
    print("   Merged from FACT_ATTENDANCE + FACT_GRADES")
except Exception as e:
    combined = fa
    print(f"   Using FACT_ATTENDANCE only: {str(e)[:60]}")

window = Window.partitionBy("course_id").orderBy(
    F.col("course_name").isNotNull().desc(),
    F.col("course_code").isNotNull().desc(),
    F.col("programme_name").isNotNull().desc()
)

dim_course = combined \
    .withColumn("_rank", F.row_number().over(window)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank") \
    .withColumn("gold_updated_at", F.current_timestamp()) \
    .withColumn("pipeline_run_id", F.lit(run_id))

output_path = f"{gold_lh}.dbo.DIM_COURSE"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(dim_course.alias("source"),
               "target.course_id = source.course_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    dim_course.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

gold_count = spark.table(output_path).count()
print(f"\nSUCCESS: DIM_COURSE")
print(f"   Records : {gold_count:,}")
print(f"   Columns : {len(dim_course.columns)}")
print(f"   Run ID  : {run_id}")

print("\nNULL check:")
spark.table(output_path).select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in ["course_id", "course_code", "course_name", "programme_id", "programme_name"]
]).show(truncate=False)

display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 4 — FACT_GRADES (All Years - Fully Automated)
# ==============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, LongType
from delta.tables import DeltaTable
from functools import reduce
from datetime import datetime

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("FACT_GRADES")

# ==============================================================
# AUTO-DISCOVER GRADE TABLES
# ==============================================================
print("\nScanning Silver for grade tables...")
all_silver_tables = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {silver_lh}.dbo").collect()
]

grade_tables = sorted([
    t for t in all_silver_tables
    if t.lower().startswith("pedagogie_note")
])

print(f"   Found {len(grade_tables)} grade tables:")
for t in grade_tables:
    print(f"      - {t}")

# ==============================================================
# COLUMN DEFINITIONS
# ==============================================================
column_definitions = [
    ("apprenantevaluation_id",       "grade_key",             "string"),
    ("evaluation_id",                "evaluation_key",         "string"),
    ("apprenant_code",               "student_code",           "string"),
    ("cours_code",                   "course_code",            "string"),
    ("cours_id",                     "course_id",              "string"),
    ("programme_id",                 "programme_id",           "string"),
    ("programme_acronyme",           "programme_acronym",      "string"),
    ("programme_libelle",            "programme_name",         "string"),
    ("apprenant_nom_officiel",       "student_last_name",      "string"),
    ("apprenant_prenom_officiel",    "student_first_name",     "string"),
    ("cours_libelle",                "course_name",            "string"),
    ("module_libelle",               "module_name",            "string"),
    ("entite_libelle",               "entity_name",            "string"),
    ("sequence_libelle",             "exam_session",           "string"),
    ("sequencemere_libelle",         "parent_session",         "string"),
    ("sequence_datedebut",           "session_start_date",     "string"),
    ("sequence_datefin",             "session_end_date",       "string"),
    ("evaluation_libelle",           "evaluation_name",        "string"),
    ("evaluation_devoir",            "is_homework",            "string"),
    ("evaluation_ponderation",       "evaluation_weight",      "string"),
    ("sequencecours_ponderation",    "course_session_weight",  "string"),
    ("evaluation_excusable",         "is_excusable",           "string"),
    ("evaluation_notepubliee",       "grade_published",        "string"),
    ("evaluation_note",              "grade_value",            "string"),
    ("cours_moyenne",                "course_average",         "string"),
    ("ectscredit",                   "ects_credits",           "string"),
    ("cours_valide",                 "course_passed",          "string"),
    ("apprenantevaluation_absent",   "is_absent",              "string"),
    ("apprenantevaluation_deux",     "is_resit",               "string"),
    ("apprenantevaluation_dispense", "is_exempted",            "string"),
    ("evaluation_excuse",            "is_excused",             "string"),
    ("evaluation_excusemotif",       "excuse_reason",          "string"),
    ("groupes_codes",                "group_codes",            "string"),
]

type_map = {"string": StringType(), "double": DoubleType(), "long": LongType()}

def extract_year_tag(table_name):
    t = table_name.lower()
    t = t.replace("pedagogie_noteperiode_", "")
    t = t.replace("pedagogie_noteperiode", "periode")
    t = t.replace("pedagogie_note", "")
    return t if t else "unknown"

# ==============================================================
# LOAD AND STANDARDIZE
# ==============================================================
print("\nLoading grade tables...")
all_dfs    = []
ok_count   = 0
fail_count = 0
skipped    = 0

for table in grade_tables:
    try:
        df        = spark.table(f"{silver_lh}.dbo.{table}")
        row_count = df.count()

        # Skip empty tables
        if row_count == 0:
            print(f"   SKIP   {table:55s} (0 rows)")
            skipped += 1
            continue

        df_cols    = set(df.columns)
        year_tag   = extract_year_tag(table)
        is_periode = "periode" in table.lower()

        select_exprs = [
            F.lit(table)                     .alias("source_table"),
            F.lit(year_tag)                  .alias("academic_year"),
            F.lit(is_periode).cast("string") .alias("is_period_grade"),
        ]

        for src_col, out_alias, cast_type in column_definitions:
            if src_col in df_cols:
                select_exprs.append(
                    F.col(src_col).cast(type_map[cast_type]).alias(out_alias))
            else:
                select_exprs.append(
                    F.lit(None).cast(type_map[cast_type]).alias(out_alias))

        select_exprs.append(F.current_timestamp().alias("gold_updated_at"))
        select_exprs.append(F.lit(run_id).alias("pipeline_run_id"))

        all_dfs.append(df.select(*select_exprs))
        print(f"   OK     {table:55s} ({row_count:,} rows)  year={year_tag}")
        ok_count += 1

    except Exception as e:
        print(f"   FAILED {table:55s} {str(e)[:80]}")
        fail_count += 1

if not all_dfs:
    raise RuntimeError("No grade tables loaded — aborting.")

# ==============================================================
# UNION + DEDUP
# ==============================================================
print("\nUnioning all grade tables...")
fact_grades_all = reduce(lambda a, b: a.unionByName(b), all_dfs)

fact_grades_all = fact_grades_all.withColumn(
    "row_id",
    F.concat_ws("_",
        F.col("source_table"),
        F.col("student_code"),
        F.col("course_code"),
        F.col("exam_session"),
        F.coalesce(F.col("grade_key"), F.lit("NULL"))
    )
).dropDuplicates(["row_id"])
# Keep row_id in table — needed for future MERGE operations

# ==============================================================
# MERGE (upsert)
# ==============================================================
output_path = f"{gold_lh}.dbo.FACT_GRADES"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(fact_grades_all.alias("source"),
               "target.row_id = source.row_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    fact_grades_all.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

final_count = spark.table(output_path).count()
print(f"\nSUCCESS: FACT_GRADES")
print(f"   Records      : {final_count:,}")
print(f"   Columns      : {len(fact_grades_all.columns)}")
print(f"   Sources OK   : {ok_count}")
print(f"   Sources SKIP : {skipped}")
print(f"   Sources FAIL : {fail_count}")
print(f"   Run ID       : {run_id}")

print("\nBreakdown by academic year:")
spark.table(output_path) \
    .groupBy("academic_year", "is_period_grade") \
    .count().orderBy("academic_year").show(30, truncate=False)

print("\nDuplicate check (should be empty):")
spark.table(output_path) \
    .groupBy("row_id").count() \
    .filter(F.col("count") > 1) \
    .show(10, truncate=False)

display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 5 — FACT_CANDIDATURE
# ==============================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("FACT_CANDIDATURE")

dossier_df = spark.table(f"{silver_lh}.dbo.Candidature_DossierCandidature")
print(f"   Candidature_DossierCandidature: {dossier_df.count():,} rows")

fact_candidature = (
    dossier_df.select(
        F.col("candidature_id")                                    .alias("candidature_key"),
        F.col("candidature_code")                                  .alias("candidature_code"),
        F.col("candidat_id")                                       .alias("candidate_id"),
        F.col("candidat_code")                                     .alias("candidate_code"),
        F.col("candidature_inscription_apprenant_code")            .alias("student_code"),
        F.col("candidature_inscription_id")                        .alias("inscription_id"),
        F.col("candidat_nom_officiel")                             .alias("last_name"),
        F.col("candidat_prenom_officiel")                          .alias("first_name"),
        F.col("candidat_civilite_libelle")                         .alias("title"),
        F.col("candidat_mailpersonnel")                            .alias("email"),
        F.col("candidat_telephonemobile")                          .alias("phone_mobile"),
        F.col("candidat_naissance_date")                           .alias("birth_date"),
        F.col("candidat_naissance_pays")                           .alias("birth_country"),
        F.col("candidat_nationalite1")                             .alias("nationality"),
        F.col("candidat_nationalite1_continent")                   .alias("nationality_continent"),
        F.col("candidat_paysresidence_libelle")                    .alias("country_of_residence"),
        F.col("candidat_adresse_adresse1")                         .alias("address_line1"),
        F.col("candidat_adresse_codepostal")                       .alias("address_postcode"),
        F.col("candidat_adresse_ville")                            .alias("address_city"),
        F.col("candidat_adresse_pays")                             .alias("address_country"),
        F.col("programme_id")                                      .alias("programme_id"),
        F.col("programme_libelle")                                 .alias("programme_name"),
        F.col("programme_acronyme")                                .alias("programme_acronym"),
        F.col("entite_id")                                         .alias("entity_id"),
        F.col("entite_libelle")                                    .alias("entity_name"),
        F.col("campus_id")                                         .alias("campus_id"),
        F.col("campus_libelle")                                    .alias("campus_name"),
        F.col("candidature_voieadmission_id")                      .alias("admission_track_id"),
        F.col("candidature_voieadmission_libelle")                 .alias("admission_track"),
        F.col("candidature_voieadmission_periode")                 .alias("admission_period"),
        F.col("candidature_niveauetuderecrutement_libelle")        .alias("recruitment_level"),
        F.col("candidature_session_libelle")                       .alias("session_name"),
        F.col("candidature_sessionselection_libelle")              .alias("selection_session"),
        F.col("candidature_etat_code")                             .alias("status_code"),
        F.col("candidature_etat_libelle")                          .alias("status_label"),
        F.col("candidature_avancement")                            .alias("progress"),
        F.col("candidature_decision")                              .alias("decision"),
        F.col("candidature_evaluation")                            .alias("evaluation_score"),
        F.col("candidature_piecemanquante")                        .alias("missing_documents"),
        F.col("candidature_datedebut")                             .alias("application_start_date"),
        F.col("candidature_datefin")                               .alias("application_end_date"),
        F.col("candidature_datedecision")                          .alias("decision_date"),
        F.col("candidature_dateentretien")                         .alias("interview_date"),
        F.col("candidature_dateevaluation")                        .alias("evaluation_date"),
        F.col("candidat_datecreation")                             .alias("candidate_created_date"),
        F.col("candidature_diplomedusecondaire_baccalaureat_libelle")        .alias("bac_type"),
        F.col("candidature_diplomedusecondaire_baccalaureat_annee")          .alias("bac_year"),
        F.col("candidature_diplomedusecondaire_baccalaureat_mention_libelle").alias("bac_mention"),
        F.col("candidature_diplomedusecondaire_baccalaureat_note")           .alias("bac_grade"),
        F.col("candidature_diplomedusecondaire_baccalaureat_pays_libelle")   .alias("bac_country"),
        F.col("paiementfrais_montant")                             .alias("registration_fee_amount"),
        F.col("paiementfrais_date")                                .alias("registration_fee_date"),
        F.col("paiementfrais_type")                                .alias("registration_fee_type"),
        F.col("candidature_inscription_acompte_montant")           .alias("deposit_amount"),
        F.col("candidature_inscription_acompte_datepaiement")      .alias("deposit_date"),
        F.col("candidature_inscription_acompte_typepaiement")      .alias("deposit_type"),
        F.col("agent_nom")                                         .alias("agent_last_name"),
        F.col("agent_prenom")                                      .alias("agent_first_name"),
        F.col("evaluateur_nom_officiel")                           .alias("evaluator_name"),
        F.col("evaluateur_mail")                                   .alias("evaluator_email"),
        F.col("evaluateur_entreprise")                             .alias("evaluator_company"),
        F.col("candidat_canalacquisition_libelle")                 .alias("acquisition_channel"),
        F.current_timestamp()                                      .alias("gold_updated_at"),
        F.lit(run_id)                                              .alias("pipeline_run_id"),
    )
    .dropDuplicates(["candidature_key"])
)

output_path = f"{gold_lh}.dbo.FACT_CANDIDATURE"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(fact_candidature.alias("source"),
               "target.candidature_key = source.candidature_key") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    fact_candidature.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

silver_count = dossier_df.count()
gold_count   = spark.table(output_path).count()
ratio        = gold_count / silver_count if silver_count > 0 else 0
print(f"\nSUCCESS: FACT_CANDIDATURE")
print(f"   Records     : {gold_count:,}")
print(f"   Columns     : {len(fact_candidature.columns)}")
print(f"   Silver/Gold : {ratio:.0%} {'OK' if ratio >= 0.8 else 'WARNING low ratio'}")
print(f"   Run ID      : {run_id}")
display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 6 — DIM_PROGRAMME
# ==============================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("DIM_PROGRAMME")

contact_df = spark.table(f"{silver_lh}.dbo.Candidature_Contact")
print(f"   Candidature_Contact: {contact_df.count():,} rows")

dim_programme = (
    contact_df.select(
        F.col("programme_id")            .alias("programme_id"),
        F.col("programme_libelle")       .alias("programme_name"),
        F.col("programme_libelleexterne").alias("programme_name_external"),
        F.col("programme_acronyme")      .alias("programme_acronym"),
        F.col("programme_codedroit")     .alias("programme_legal_code"),
        F.col("programme_marque")        .alias("brand"),
        F.col("entite_id")               .alias("entity_id"),
        F.col("entite_libelle")          .alias("entity_name"),
        F.col("entite_acronyme")         .alias("entity_acronym"),
        F.col("entite_code")             .alias("entity_code"),
        F.col("campus_id")               .alias("campus_id"),
        F.col("campus_libelle")          .alias("campus_name"),
        F.col("campus_code")             .alias("campus_code"),
        F.col("campus_codedroit")        .alias("campus_legal_code"),
        F.current_timestamp()            .alias("gold_updated_at"),
        F.lit(run_id)                    .alias("pipeline_run_id"),
    )
    .dropDuplicates(["programme_id"])
    .filter(F.col("programme_id").isNotNull())
    .orderBy("programme_name")
)

output_path = f"{gold_lh}.dbo.DIM_PROGRAMME"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(dim_programme.alias("source"),
               "target.programme_id = source.programme_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    dim_programme.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

gold_count = spark.table(output_path).count()
print(f"\nSUCCESS: DIM_PROGRAMME")
print(f"   Records : {gold_count:,}")
print(f"   Columns : {len(dim_programme.columns)}")
print(f"   Run ID  : {run_id}")
display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 7 — FACT_ATTENDANCE (All Years - Fully Automated)
# ==============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from functools import reduce
from collections import defaultdict
from datetime import datetime
import re

silver_lh = "Silver"
gold_lh   = "Gold"
run_id    = datetime.now().strftime("%Y%m%d_%H%M%S")

print("FACT_ATTENDANCE")

# ==============================================================
# AUTO-DISCOVER + ROLLUP DETECTION
# ==============================================================
print("\nScanning Silver for attendance tables...")

all_silver_tables = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {silver_lh}.dbo").collect()
]

raw_attendance = sorted([
    t for t in all_silver_tables
    if t.lower().startswith("planification_seanceapprenant")
    or t.lower() == "planification_apprenantseancesanscours"
])

print(f"   Found {len(raw_attendance)} raw attendance tables")

def get_base_name(table_name):
    return re.sub(r'_S\d+$', '', table_name, flags=re.IGNORECASE)

groups = defaultdict(list)
for t in raw_attendance:
    if t.lower() == "planification_apprenantseancesanscours":
        groups[t].append(t)
    else:
        groups[get_base_name(t)].append(t)

attendance_tables = []
excluded_rollups  = []

for base, members in groups.items():
    has_splits  = any(re.search(r'_S\d+$', m, re.IGNORECASE) for m in members)
    base_table  = next((m for m in members if m.lower() == base.lower()), None)

    for t in sorted(members):
        is_sans_cours = (t.lower() == "planification_apprenantseancesanscours")

        if has_splits and base_table and t.lower() == base_table.lower():
            excluded_rollups.append(t)
            continue

        year_raw = (t.lower()
                    .replace("planification_seanceapprenant", "")
                    .replace("planification_apprenantseancesanscours", ""))
        year_tag = re.sub(r'_s\d+$', '', year_raw) if year_raw else "all"
        if not year_tag:
            year_tag = "all"

        attendance_tables.append((t, year_tag, is_sans_cours))

print(f"   Excluded rollups : {excluded_rollups}")
print(f"   Tables to process: {len(attendance_tables)}")

# ==============================================================
# COLUMN DEFINITIONS
# ==============================================================
column_definitions = [
    ("presence_id",                             "attendance_key"),
    ("seance_id",                               "session_id"),
    ("apprenant_code",                          "student_code"),
    ("cours_id",                                "course_id"),
    ("cours_code",                              "course_code"),
    ("sequence_id",                             "sequence_id"),
    ("apprenant_nom_officiel",                  "student_last_name"),
    ("apprenant_prenom_officiel",               "student_first_name"),
    ("apprenant_login",                         "student_login"),
    ("cours_libelle",                           "course_name"),
    ("sequence_libelle",                        "sequence_name"),
    ("seance_debut",                            "session_start"),
    ("seance_fin",                              "session_end"),
    ("seance_datedebut",                        "session_date"),
    ("seance_heuredebut",                       "session_time_start"),
    ("seance_heurefin",                         "session_time_end"),
    ("seance_duree",                            "session_duration_min"),
    ("seance_enseignant",                       "teacher"),
    ("seance_enseignant_code",                  "teacher_code"),
    ("seance_salle",                            "room"),
    ("seance_groupe_nom",                       "group_name"),
    ("seance_groupe_code",                      "group_code"),
    ("seance_faceaface",                        "is_face_to_face"),
    ("seance_realiseon",                        "session_realized_on"),
    ("typeseance_code",                         "session_type_code"),
    ("typeseance_libelle",                      "session_type"),
    ("modalite_seance_libelle",                 "delivery_mode"),
    ("modalite_seance_presentiel",              "is_in_person"),
    ("modalite_seance_distanciel",              "is_remote"),
    ("presence_presencevraifaux",               "is_present"),
    ("presence_controle_realisevraifaux",       "attendance_checked"),
    ("presence_controle_provenance",            "check_source"),
    ("presence_valeurpresence",                 "presence_value"),
    ("presence_dureeabsence",                   "absence_duration_min"),
    ("presence_motifabsence_libelle",           "absence_reason"),
    ("presence_motifabsence_justifievraifaux",  "absence_justified"),
    ("programme_id",                            "programme_id"),
    ("programme_libelle",                       "programme_name"),
    ("programme_acronyme",                      "programme_acronym"),
    ("entitepedagogique_libelle",               "entity_name"),
    ("entite_libelle",                          "entity_name_alt"),
    ("campus_id",                               "campus_id"),
    ("campus_libelle",                          "campus_name"),
    ("campus_code",                             "campus_code"),
    ("inscription_etatinscription_libelle",     "enrollment_status"),
]

# ==============================================================
# LOAD AND STANDARDIZE
# ==============================================================
print("\nLoading attendance tables...")
all_dfs    = []
ok_count   = 0
fail_count = 0
skipped    = 0

for table_name, year_tag, is_sans_cours in attendance_tables:
    try:
        df        = spark.table(f"{silver_lh}.dbo.{table_name}")
        row_count = df.count()

        if row_count == 0:
            print(f"   SKIP   {table_name:55s} (0 rows)")
            skipped += 1
            continue

        df_cols = set(df.columns)

        select_exprs = [
            F.lit(table_name)               .alias("source_table"),
            F.lit(year_tag)                 .alias("academic_year"),
            F.lit(is_sans_cours).cast("string").alias("is_session_without_course"),
        ]

        for src_col, out_alias in column_definitions:
            if src_col in df_cols:
                select_exprs.append(F.col(src_col).cast(StringType()).alias(out_alias))
            else:
                select_exprs.append(F.lit(None).cast(StringType()).alias(out_alias))

        select_exprs.append(F.current_timestamp().alias("gold_updated_at"))
        select_exprs.append(F.lit(run_id).alias("pipeline_run_id"))

        all_dfs.append(df.select(*select_exprs))
        print(f"   OK     {table_name:55s} ({row_count:,} rows)")
        ok_count += 1

    except Exception as e:
        print(f"   FAILED {table_name:55s} {str(e)[:80]}")
        fail_count += 1

if not all_dfs:
    raise RuntimeError("No attendance tables loaded — aborting.")

# ==============================================================
# UNION + DEDUP
# ==============================================================
print("\nUnioning all attendance tables...")
fact_attendance = reduce(lambda a, b: a.unionByName(b), all_dfs)

fact_attendance = fact_attendance.withColumn(
    "row_id",
    F.concat_ws("_",
        F.col("source_table"),
        F.col("student_code"),
        F.col("session_id"),
        F.coalesce(F.col("attendance_key"), F.lit("NULL"))
    )
).dropDuplicates(["row_id"])
# Keep row_id in table — needed for future MERGE operations

# ==============================================================
# MERGE (upsert)
# ==============================================================
output_path = f"{gold_lh}.dbo.FACT_ATTENDANCE"

if DeltaTable.isDeltaTable(spark, output_path):
    DeltaTable.forName(spark, output_path).alias("target") \
        .merge(fact_attendance.alias("source"),
               "target.row_id = source.row_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    print(f"   MERGE complete")
else:
    fact_attendance.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(output_path)
    print(f"   First run — full write")

final_count = spark.table(output_path).count()
print(f"\nSUCCESS: FACT_ATTENDANCE")
print(f"   Records      : {final_count:,}")
print(f"   Columns      : {len(fact_attendance.columns)}")
print(f"   Sources OK   : {ok_count}")
print(f"   Sources SKIP : {skipped}")
print(f"   Sources FAIL : {fail_count}")
print(f"   Run ID       : {run_id}")

print("\nBreakdown by academic year:")
spark.table(output_path) \
    .groupBy("academic_year", "is_session_without_course") \
    .count().orderBy("academic_year").show(30, truncate=False)

print("\nPresence summary:")
spark.table(output_path) \
    .groupBy("is_present").count() \
    .orderBy("is_present").show(10, truncate=False)

print("\nDuplicate check (should be empty):")
spark.table(output_path) \
    .groupBy("row_id").count() \
    .filter(F.col("count") > 1) \
    .show(10, truncate=False)

display(spark.table(output_path).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# SHELL 8 (NEW) — MASTER ORCHESTRATOR
# Runs all Gold shells sequentially in one notebook
# No separate notebooks needed
# ==============================================================
import time
from datetime import datetime

run_id         = datetime.now().strftime("%Y%m%d_%H%M%S")
pipeline_start = time.time()
results        = {}

print(f"MASTER ORCHESTRATOR — Silver to Gold")
print(f"Run ID : {run_id}")
print("=" * 60)

# ==============================================================
# HELPER — wraps each shell in timing + error handling
# ==============================================================
def run_shell(name, fn):
    start = time.time()
    print(f"\nRunning: {name}...")
    try:
        fn()
        elapsed       = round(time.time() - start)
        results[name] = f"OK  ({elapsed}s)"
        print(f"   Done in {elapsed}s")
    except Exception as e:
        elapsed       = round(time.time() - start)
        results[name] = f"FAILED ({elapsed}s) — {str(e)[:80]}"
        print(f"   FAILED: {str(e)}")
        raise   # halts pipeline so downstream tables don't run on bad data

# ==============================================================
# PASTE EACH SHELL AS A FUNCTION BELOW
# ==============================================================

def run_dim_student():
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    silver_lh = "Silver"
    gold_lh   = "Gold"

    contact_df    = spark.table(f"{silver_lh}.dbo.Candidature_Contact")
    dossier_df    = spark.table(f"{silver_lh}.dbo.Candidature_DossierCandidature")
    enrollment_df = spark.table(f"{silver_lh}.dbo.Inscription_ApprenantInscription")

    dim_student = (
        contact_df.alias("c")
        .join(dossier_df.alias("d"),
              F.col("c.candidature_id") == F.col("d.candidature_id"), "left")
        .join(enrollment_df.alias("e"),
              F.col("d.candidature_inscription_apprenant_code") == F.col("e.apprenant_code"), "left")
        .select(
            F.col("c.candidature_id")                          .alias("student_key"),
            F.col("c.candidature_code")                        .alias("candidature_code"),
            F.col("e.apprenant_code")                          .alias("student_code"),
            F.col("e.apprenant_numeroine")                     .alias("ine_number"),
            F.col("c.candidat_nom_officiel")                   .alias("last_name"),
            F.col("c.candidat_prenom_officiel")                .alias("first_name"),
            F.col("c.candidat_nom_usage")                      .alias("last_name_usage"),
            F.col("c.candidat_prenom_usage")                   .alias("first_name_usage"),
            F.col("c.candidat_civilite_libelle")               .alias("title"),
            F.col("d.candidat_mailpersonnel")                  .alias("email_personal"),
            F.col("e.apprenant_mailpersonnel")                 .alias("email_personal_enrolled"),
            F.col("e.apprenant_mailecole")                     .alias("email_school"),
            F.col("d.candidat_telephonemobile")                .alias("phone_mobile"),
            F.col("d.candidat_telephonefixe")                  .alias("phone_landline"),
            F.col("c.candidat_naissance_date")                 .alias("birth_date"),
            F.col("c.candidat_naissance_ville")                .alias("birth_city"),
            F.col("c.candidat_naissance_pays")                 .alias("birth_country"),
            F.col("c.candidat_nationalite1")                   .alias("nationality"),
            F.col("d.candidat_adresse_adresse1")               .alias("address_line1"),
            F.col("d.candidat_adresse_codepostal")             .alias("address_postcode"),
            F.col("d.candidat_adresse_ville")                  .alias("address_city"),
            F.col("d.candidat_adresse_pays")                   .alias("address_country"),
            F.col("c.programme_id")                            .alias("programme_id"),
            F.col("c.programme_libelle")                       .alias("programme_name"),
            F.col("c.programme_acronyme")                      .alias("programme_acronym"),
            F.col("c.entite_libelle")                          .alias("school_entity"),
            F.col("c.campus_libelle")                          .alias("campus"),
            F.col("c.candidature_etat_code")                   .alias("candidature_status_code"),
            F.col("c.candidature_etat_libelle")                .alias("candidature_status_label"),
            F.col("c.candidature_voieadmission_libelle")       .alias("admission_track"),
            F.col("d.candidature_decision")                    .alias("admission_decision"),
            F.col("d.candidature_avancement")                  .alias("candidature_progress"),
            F.col("e.inscription_etatinscription_libelle")     .alias("enrollment_status"),
            F.col("e.inscription_dateinscription")             .alias("enrollment_date"),
            F.col("e.inscription_dateadmission")               .alias("admission_date"),
            F.col("e.inscription_datesortie")                  .alias("exit_date"),
            F.col("e.inscription_regime_libelle")              .alias("study_regime"),
            F.col("e.inscription_voieentree_libelle")          .alias("entry_track"),
            F.col("e.inscription_baccalaureat_libelle")        .alias("baccalaureate_type"),
            F.col("e.inscription_baccalaureat_annee")          .alias("baccalaureate_year"),
            F.col("e.inscription_baccalaureat_mention_libelle").alias("baccalaureate_mention"),
            F.col("d.paiementfrais_montant")                   .alias("registration_fee_amount"),
            F.col("d.paiementfrais_date")                      .alias("registration_fee_date"),
            F.col("d.candidature_inscription_acompte_montant") .alias("deposit_amount"),
            F.current_timestamp()                              .alias("gold_updated_at"),
            F.lit(run_id)                                      .alias("pipeline_run_id"),
        )
        .dropDuplicates(["student_key"])
    )

    output_path = f"{gold_lh}.dbo.DIM_STUDENT"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(dim_student.alias("source"), "target.student_key = source.student_key") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        dim_student.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   DIM_STUDENT: {count:,} records")


def run_fact_finance():
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    silver_lh = "Silver"
    gold_lh   = "Gold"

    facture_df = spark.table(f"{silver_lh}.dbo.Finance_FactureNonLettree")
    client_df  = spark.table(f"{silver_lh}.dbo.Finance_Client")

    fact_finance = (
        facture_df.alias("f")
        .join(client_df.alias("c"), F.col("f.client_code") == F.col("c.client_code"), "left")
        .select(
            F.col("f.facture_id")                          .alias("facture_key"),
            F.col("f.facture_numero")                      .alias("invoice_number"),
            F.col("c.client_id")                           .alias("client_key"),
            F.col("f.client_code")                         .alias("client_code"),
            F.col("c.individu_id")                         .alias("student_key"),
            F.col("f.apprenant_code")                      .alias("student_code"),
            F.col("f.client_nomusage")                     .alias("client_last_name"),
            F.col("f.client_prenomusage")                  .alias("client_first_name"),
            F.col("f.client_nature")                       .alias("client_nature"),
            F.col("c.client_type")                         .alias("client_type"),
            F.col("c.client_civilitelibelle")              .alias("client_title"),
            F.col("f.client_mail")                         .alias("client_email"),
            F.col("f.client_telephonemobile")              .alias("client_phone"),
            F.col("c.clientdouteux")                       .alias("is_doubtful_client"),
            F.col("f.apprenant_nom_usage")                 .alias("student_last_name"),
            F.col("f.apprenant_prenom_usage")              .alias("student_first_name"),
            F.col("f.inscription_etatinscription_code")    .alias("enrollment_status_code"),
            F.col("f.inscription_etatinscription_libelle") .alias("enrollment_status"),
            F.col("f.inscription_voieentree_libelle")      .alias("entry_track"),
            F.col("f.programme_id")                        .alias("programme_id"),
            F.col("f.programme_code")                      .alias("programme_code"),
            F.col("f.programme_libelle")                   .alias("programme_name"),
            F.col("f.campus_id")                           .alias("campus_id"),
            F.col("f.campus_libelle")                      .alias("campus_name"),
            F.col("f.campus_code")                         .alias("campus_code"),
            F.col("f.marque_libelle")                      .alias("brand"),
            F.col("f.societefacturation_id")               .alias("billing_entity_id"),
            F.col("f.societefacturation_libelle")          .alias("billing_entity_name"),
            F.col("f.societefacturation_code")             .alias("billing_entity_code"),
            F.col("f.facture_date")                        .alias("invoice_date"),
            F.col("f.facture_montant")                     .alias("invoice_amount"),
            F.current_timestamp()                          .alias("gold_updated_at"),
            F.lit(run_id)                                  .alias("pipeline_run_id"),
        )
        .dropDuplicates(["facture_key"])
    )

    output_path = f"{gold_lh}.dbo.FACT_FINANCE"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(fact_finance.alias("source"), "target.facture_key = source.facture_key") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_finance.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   FACT_FINANCE: {count:,} records")


def run_fact_grades():
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    from delta.tables import DeltaTable
    from functools import reduce

    silver_lh = "Silver"
    gold_lh   = "Gold"

    all_silver_tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {silver_lh}.dbo").collect()
    ]
    grade_tables = sorted([
        t for t in all_silver_tables
        if t.lower().startswith("pedagogie_note")
    ])

    column_definitions = [
        ("apprenantevaluation_id",       "grade_key"),
        ("evaluation_id",                "evaluation_key"),
        ("apprenant_code",               "student_code"),
        ("cours_code",                   "course_code"),
        ("cours_id",                     "course_id"),
        ("programme_id",                 "programme_id"),
        ("programme_acronyme",           "programme_acronym"),
        ("programme_libelle",            "programme_name"),
        ("apprenant_nom_officiel",       "student_last_name"),
        ("apprenant_prenom_officiel",    "student_first_name"),
        ("cours_libelle",                "course_name"),
        ("module_libelle",               "module_name"),
        ("entite_libelle",               "entity_name"),
        ("sequence_libelle",             "exam_session"),
        ("sequencemere_libelle",         "parent_session"),
        ("sequence_datedebut",           "session_start_date"),
        ("sequence_datefin",             "session_end_date"),
        ("evaluation_libelle",           "evaluation_name"),
        ("evaluation_devoir",            "is_homework"),
        ("evaluation_ponderation",       "evaluation_weight"),
        ("sequencecours_ponderation",    "course_session_weight"),
        ("evaluation_excusable",         "is_excusable"),
        ("evaluation_notepubliee",       "grade_published"),
        ("evaluation_note",              "grade_value"),
        ("cours_moyenne",                "course_average"),
        ("ectscredit",                   "ects_credits"),
        ("cours_valide",                 "course_passed"),
        ("apprenantevaluation_absent",   "is_absent"),
        ("apprenantevaluation_deux",     "is_resit"),
        ("apprenantevaluation_dispense", "is_exempted"),
        ("evaluation_excuse",            "is_excused"),
        ("evaluation_excusemotif",       "excuse_reason"),
        ("groupes_codes",                "group_codes"),
    ]

    def extract_year_tag(t):
        t = t.lower()
        t = t.replace("pedagogie_noteperiode_", "")
        t = t.replace("pedagogie_noteperiode", "periode")
        t = t.replace("pedagogie_note", "")
        return t if t else "unknown"

    all_dfs = []
    for table in grade_tables:
        try:
            df        = spark.table(f"{silver_lh}.dbo.{table}")
            row_count = df.count()
            if row_count == 0:
                continue
            df_cols    = set(df.columns)
            year_tag   = extract_year_tag(table)
            is_periode = "periode" in table.lower()

            select_exprs = [
                F.lit(table)                     .alias("source_table"),
                F.lit(year_tag)                  .alias("academic_year"),
                F.lit(is_periode).cast("string") .alias("is_period_grade"),
            ]
            for src_col, out_alias in column_definitions:
                if src_col in df_cols:
                    select_exprs.append(F.col(src_col).cast(StringType()).alias(out_alias))
                else:
                    select_exprs.append(F.lit(None).cast(StringType()).alias(out_alias))
            select_exprs.append(F.current_timestamp().alias("gold_updated_at"))
            select_exprs.append(F.lit(run_id).alias("pipeline_run_id"))
            all_dfs.append(df.select(*select_exprs))
        except Exception as e:
            print(f"   WARNING: {table} failed — {str(e)[:60]}")

    if not all_dfs:
        raise RuntimeError("No grade tables loaded")

    fact_grades = reduce(lambda a, b: a.unionByName(b), all_dfs)
    fact_grades = fact_grades.withColumn(
        "row_id",
        F.concat_ws("_",
            F.col("source_table"), F.col("student_code"),
            F.col("course_code"),  F.col("exam_session"),
            F.coalesce(F.col("grade_key"), F.lit("NULL"))
        )
    ).dropDuplicates(["row_id"])

    output_path = f"{gold_lh}.dbo.FACT_GRADES"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(fact_grades.alias("source"), "target.row_id = source.row_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_grades.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   FACT_GRADES: {count:,} records")


def run_fact_candidature():
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    silver_lh = "Silver"
    gold_lh   = "Gold"

    dossier_df = spark.table(f"{silver_lh}.dbo.Candidature_DossierCandidature")

    fact_candidature = (
        dossier_df.select(
            F.col("candidature_id")                                    .alias("candidature_key"),
            F.col("candidature_code")                                  .alias("candidature_code"),
            F.col("candidat_id")                                       .alias("candidate_id"),
            F.col("candidat_code")                                     .alias("candidate_code"),
            F.col("candidature_inscription_apprenant_code")            .alias("student_code"),
            F.col("candidature_inscription_id")                        .alias("inscription_id"),
            F.col("candidat_nom_officiel")                             .alias("last_name"),
            F.col("candidat_prenom_officiel")                          .alias("first_name"),
            F.col("candidat_civilite_libelle")                         .alias("title"),
            F.col("candidat_mailpersonnel")                            .alias("email"),
            F.col("candidat_telephonemobile")                          .alias("phone_mobile"),
            F.col("candidat_naissance_date")                           .alias("birth_date"),
            F.col("candidat_naissance_pays")                           .alias("birth_country"),
            F.col("candidat_nationalite1")                             .alias("nationality"),
            F.col("candidat_nationalite1_continent")                   .alias("nationality_continent"),
            F.col("candidat_paysresidence_libelle")                    .alias("country_of_residence"),
            F.col("candidat_adresse_adresse1")                         .alias("address_line1"),
            F.col("candidat_adresse_codepostal")                       .alias("address_postcode"),
            F.col("candidat_adresse_ville")                            .alias("address_city"),
            F.col("candidat_adresse_pays")                             .alias("address_country"),
            F.col("programme_id")                                      .alias("programme_id"),
            F.col("programme_libelle")                                 .alias("programme_name"),
            F.col("programme_acronyme")                                .alias("programme_acronym"),
            F.col("entite_id")                                         .alias("entity_id"),
            F.col("entite_libelle")                                    .alias("entity_name"),
            F.col("campus_id")                                         .alias("campus_id"),
            F.col("campus_libelle")                                    .alias("campus_name"),
            F.col("candidature_voieadmission_id")                      .alias("admission_track_id"),
            F.col("candidature_voieadmission_libelle")                 .alias("admission_track"),
            F.col("candidature_voieadmission_periode")                 .alias("admission_period"),
            F.col("candidature_niveauetuderecrutement_libelle")        .alias("recruitment_level"),
            F.col("candidature_session_libelle")                       .alias("session_name"),
            F.col("candidature_sessionselection_libelle")              .alias("selection_session"),
            F.col("candidature_etat_code")                             .alias("status_code"),
            F.col("candidature_etat_libelle")                          .alias("status_label"),
            F.col("candidature_avancement")                            .alias("progress"),
            F.col("candidature_decision")                              .alias("decision"),
            F.col("candidature_evaluation")                            .alias("evaluation_score"),
            F.col("candidature_piecemanquante")                        .alias("missing_documents"),
            F.col("candidature_datedebut")                             .alias("application_start_date"),
            F.col("candidature_datefin")                               .alias("application_end_date"),
            F.col("candidature_datedecision")                          .alias("decision_date"),
            F.col("candidature_dateentretien")                         .alias("interview_date"),
            F.col("candidature_dateevaluation")                        .alias("evaluation_date"),
            F.col("candidat_datecreation")                             .alias("candidate_created_date"),
            F.col("candidature_diplomedusecondaire_baccalaureat_libelle")        .alias("bac_type"),
            F.col("candidature_diplomedusecondaire_baccalaureat_annee")          .alias("bac_year"),
            F.col("candidature_diplomedusecondaire_baccalaureat_mention_libelle").alias("bac_mention"),
            F.col("candidature_diplomedusecondaire_baccalaureat_note")           .alias("bac_grade"),
            F.col("candidature_diplomedusecondaire_baccalaureat_pays_libelle")   .alias("bac_country"),
            F.col("paiementfrais_montant")                             .alias("registration_fee_amount"),
            F.col("paiementfrais_date")                                .alias("registration_fee_date"),
            F.col("paiementfrais_type")                                .alias("registration_fee_type"),
            F.col("candidature_inscription_acompte_montant")           .alias("deposit_amount"),
            F.col("candidature_inscription_acompte_datepaiement")      .alias("deposit_date"),
            F.col("candidature_inscription_acompte_typepaiement")      .alias("deposit_type"),
            F.col("agent_nom")                                         .alias("agent_last_name"),
            F.col("agent_prenom")                                      .alias("agent_first_name"),
            F.col("evaluateur_nom_officiel")                           .alias("evaluator_name"),
            F.col("evaluateur_mail")                                   .alias("evaluator_email"),
            F.col("evaluateur_entreprise")                             .alias("evaluator_company"),
            F.col("candidat_canalacquisition_libelle")                 .alias("acquisition_channel"),
            F.current_timestamp()                                      .alias("gold_updated_at"),
            F.lit(run_id)                                              .alias("pipeline_run_id"),
        )
        .dropDuplicates(["candidature_key"])
    )

    output_path = f"{gold_lh}.dbo.FACT_CANDIDATURE"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(fact_candidature.alias("source"),
                   "target.candidature_key = source.candidature_key") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_candidature.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   FACT_CANDIDATURE: {count:,} records")


def run_dim_programme():
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    silver_lh = "Silver"
    gold_lh   = "Gold"

    contact_df = spark.table(f"{silver_lh}.dbo.Candidature_Contact")

    dim_programme = (
        contact_df.select(
            F.col("programme_id")            .alias("programme_id"),
            F.col("programme_libelle")       .alias("programme_name"),
            F.col("programme_libelleexterne").alias("programme_name_external"),
            F.col("programme_acronyme")      .alias("programme_acronym"),
            F.col("programme_codedroit")     .alias("programme_legal_code"),
            F.col("programme_marque")        .alias("brand"),
            F.col("entite_id")               .alias("entity_id"),
            F.col("entite_libelle")          .alias("entity_name"),
            F.col("entite_acronyme")         .alias("entity_acronym"),
            F.col("entite_code")             .alias("entity_code"),
            F.col("campus_id")               .alias("campus_id"),
            F.col("campus_libelle")          .alias("campus_name"),
            F.col("campus_code")             .alias("campus_code"),
            F.col("campus_codedroit")        .alias("campus_legal_code"),
            F.current_timestamp()            .alias("gold_updated_at"),
            F.lit(run_id)                    .alias("pipeline_run_id"),
        )
        .dropDuplicates(["programme_id"])
        .filter(F.col("programme_id").isNotNull())
        .orderBy("programme_name")
    )

    output_path = f"{gold_lh}.dbo.DIM_PROGRAMME"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(dim_programme.alias("source"),
                   "target.programme_id = source.programme_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        dim_programme.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   DIM_PROGRAMME: {count:,} records")


def run_fact_attendance():
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    from delta.tables import DeltaTable
    from functools import reduce
    from collections import defaultdict
    import re

    silver_lh = "Silver"
    gold_lh   = "Gold"

    all_silver_tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {silver_lh}.dbo").collect()
    ]
    raw_attendance = sorted([
        t for t in all_silver_tables
        if t.lower().startswith("planification_seanceapprenant")
        or t.lower() == "planification_apprenantseancesanscours"
    ])

    def get_base_name(t):
        return re.sub(r'_S\d+$', '', t, flags=re.IGNORECASE)

    groups = defaultdict(list)
    for t in raw_attendance:
        if t.lower() == "planification_apprenantseancesanscours":
            groups[t].append(t)
        else:
            groups[get_base_name(t)].append(t)

    attendance_tables = []
    for base, members in groups.items():
        has_splits = any(re.search(r'_S\d+$', m, re.IGNORECASE) for m in members)
        base_table = next((m for m in members if m.lower() == base.lower()), None)
        for t in sorted(members):
            is_sans_cours = (t.lower() == "planification_apprenantseancesanscours")
            if has_splits and base_table and t.lower() == base_table.lower():
                continue
            year_raw = (t.lower()
                        .replace("planification_seanceapprenant", "")
                        .replace("planification_apprenantseancesanscours", ""))
            year_tag = re.sub(r'_s\d+$', '', year_raw) if year_raw else "all"
            if not year_tag:
                year_tag = "all"
            attendance_tables.append((t, year_tag, is_sans_cours))

    column_definitions = [
        ("presence_id",                             "attendance_key"),
        ("seance_id",                               "session_id"),
        ("apprenant_code",                          "student_code"),
        ("cours_id",                                "course_id"),
        ("cours_code",                              "course_code"),
        ("sequence_id",                             "sequence_id"),
        ("apprenant_nom_officiel",                  "student_last_name"),
        ("apprenant_prenom_officiel",               "student_first_name"),
        ("apprenant_login",                         "student_login"),
        ("cours_libelle",                           "course_name"),
        ("sequence_libelle",                        "sequence_name"),
        ("seance_debut",                            "session_start"),
        ("seance_fin",                              "session_end"),
        ("seance_datedebut",                        "session_date"),
        ("seance_heuredebut",                       "session_time_start"),
        ("seance_heurefin",                         "session_time_end"),
        ("seance_duree",                            "session_duration_min"),
        ("seance_enseignant",                       "teacher"),
        ("seance_enseignant_code",                  "teacher_code"),
        ("seance_salle",                            "room"),
        ("seance_groupe_nom",                       "group_name"),
        ("seance_groupe_code",                      "group_code"),
        ("seance_faceaface",                        "is_face_to_face"),
        ("seance_realiseon",                        "session_realized_on"),
        ("typeseance_code",                         "session_type_code"),
        ("typeseance_libelle",                      "session_type"),
        ("modalite_seance_libelle",                 "delivery_mode"),
        ("modalite_seance_presentiel",              "is_in_person"),
        ("modalite_seance_distanciel",              "is_remote"),
        ("presence_presencevraifaux",               "is_present"),
        ("presence_controle_realisevraifaux",       "attendance_checked"),
        ("presence_controle_provenance",            "check_source"),
        ("presence_valeurpresence",                 "presence_value"),
        ("presence_dureeabsence",                   "absence_duration_min"),
        ("presence_motifabsence_libelle",           "absence_reason"),
        ("presence_motifabsence_justifievraifaux",  "absence_justified"),
        ("programme_id",                            "programme_id"),
        ("programme_libelle",                       "programme_name"),
        ("programme_acronyme",                      "programme_acronym"),
        ("entitepedagogique_libelle",               "entity_name"),
        ("entite_libelle",                          "entity_name_alt"),
        ("campus_id",                               "campus_id"),
        ("campus_libelle",                          "campus_name"),
        ("campus_code",                             "campus_code"),
        ("inscription_etatinscription_libelle",     "enrollment_status"),
    ]

    all_dfs = []
    for table_name, year_tag, is_sans_cours in attendance_tables:
        try:
            df        = spark.table(f"{silver_lh}.dbo.{table_name}")
            row_count = df.count()
            if row_count == 0:
                continue
            df_cols = set(df.columns)
            select_exprs = [
                F.lit(table_name)               .alias("source_table"),
                F.lit(year_tag)                 .alias("academic_year"),
                F.lit(is_sans_cours).cast("string").alias("is_session_without_course"),
            ]
            for src_col, out_alias in column_definitions:
                if src_col in df_cols:
                    select_exprs.append(F.col(src_col).cast(StringType()).alias(out_alias))
                else:
                    select_exprs.append(F.lit(None).cast(StringType()).alias(out_alias))
            select_exprs.append(F.current_timestamp().alias("gold_updated_at"))
            select_exprs.append(F.lit(run_id).alias("pipeline_run_id"))
            all_dfs.append(df.select(*select_exprs))
        except Exception as e:
            print(f"   WARNING: {table_name} failed — {str(e)[:60]}")

    if not all_dfs:
        raise RuntimeError("No attendance tables loaded")

    fact_attendance = reduce(lambda a, b: a.unionByName(b), all_dfs)
    fact_attendance = fact_attendance.withColumn(
        "row_id",
        F.concat_ws("_",
            F.col("source_table"), F.col("student_code"),
            F.col("session_id"),
            F.coalesce(F.col("attendance_key"), F.lit("NULL"))
        )
    ).dropDuplicates(["row_id"])

    output_path = f"{gold_lh}.dbo.FACT_ATTENDANCE"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(fact_attendance.alias("source"), "target.row_id = source.row_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_attendance.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   FACT_ATTENDANCE: {count:,} records")


def run_dim_course():
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    gold_lh = "Gold"

    fa = spark.table(f"{gold_lh}.dbo.FACT_ATTENDANCE") \
        .filter(F.col("course_id").isNotNull()) \
        .select(
            "course_id", "course_code", "course_name",
            "sequence_id", "sequence_name",
            "programme_id", "programme_name", "programme_acronym",
            "entity_name", "entity_name_alt",
            "campus_id", "campus_name", "campus_code",
            "session_type_code", "session_type",
            "delivery_mode", "is_face_to_face", "is_in_person", "is_remote",
            "academic_year",
        )

    try:
        fg = spark.table(f"{gold_lh}.dbo.FACT_GRADES") \
            .filter(F.col("course_id").isNotNull()) \
            .select(
                "course_id", "course_code", "course_name",
                F.lit(None).cast("string").alias("sequence_id"),
                F.lit(None).cast("string").alias("sequence_name"),
                "programme_id", "programme_name", "programme_acronym",
                F.lit(None).cast("string").alias("entity_name"),
                F.lit(None).cast("string").alias("entity_name_alt"),
                F.lit(None).cast("string").alias("campus_id"),
                F.lit(None).cast("string").alias("campus_name"),
                F.lit(None).cast("string").alias("campus_code"),
                F.lit(None).cast("string").alias("session_type_code"),
                F.lit(None).cast("string").alias("session_type"),
                F.lit(None).cast("string").alias("delivery_mode"),
                F.lit(None).cast("string").alias("is_face_to_face"),
                F.lit(None).cast("string").alias("is_in_person"),
                F.lit(None).cast("string").alias("is_remote"),
                "academic_year",
            )
        combined = fa.unionByName(fg)
    except:
        combined = fa

    window = Window.partitionBy("course_id").orderBy(
        F.col("course_name").isNotNull().desc(),
        F.col("course_code").isNotNull().desc(),
        F.col("programme_name").isNotNull().desc()
    )

    dim_course = combined \
        .withColumn("_rank", F.row_number().over(window)) \
        .filter(F.col("_rank") == 1).drop("_rank") \
        .withColumn("gold_updated_at", F.current_timestamp()) \
        .withColumn("pipeline_run_id", F.lit(run_id))

    output_path = f"{gold_lh}.dbo.DIM_COURSE"
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forName(spark, output_path).alias("target") \
            .merge(dim_course.alias("source"), "target.course_id = source.course_id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        dim_course.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(output_path)

    count = spark.table(output_path).count()
    print(f"   DIM_COURSE: {count:,} records")


# ==============================================================
# RUN ALL SHELLS IN ORDER
# DIM_COURSE must be last — depends on FACT_ATTENDANCE
# ==============================================================
try:
    run_shell("DIM_STUDENT",       run_dim_student)
    run_shell("FACT_FINANCE",      run_fact_finance)
    run_shell("FACT_GRADES",       run_fact_grades)
    run_shell("FACT_CANDIDATURE",  run_fact_candidature)
    run_shell("DIM_PROGRAMME",     run_dim_programme)
    run_shell("FACT_ATTENDANCE",   run_fact_attendance)
    run_shell("DIM_COURSE",        run_dim_course)   # always last
except Exception:
    pass   # error already printed by run_shell

# ==============================================================
# FINAL SUMMARY
# ==============================================================
total_elapsed = round(time.time() - pipeline_start)
ok_count      = sum(1 for v in results.values() if v.startswith("OK"))
fail_count    = sum(1 for v in results.values() if v.startswith("FAILED"))

print("\n" + "=" * 60)
print(f"PIPELINE SUMMARY — Run ID: {run_id}")
print(f"Total time : {total_elapsed}s")
print(f"Completed  : {ok_count}/{len(results)} shells")
print("=" * 60)
for name, status in results.items():
    print(f"   {name:25s} {status}")

if fail_count == 0:
    print("\nAll Gold tables refreshed successfully.")
else:
    print(f"\n{fail_count} shell(s) failed — check output above.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# DATE TYPE FIX SHELL — Run once before connecting Power BI
# ==============================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

gold_lh = "Gold"

print("Fixing date column types for Power BI...")

date_fixes = [
    ("FACT_GRADES",       "session_start_date"),
    ("FACT_GRADES",       "session_end_date"),
    ("FACT_ATTENDANCE",   "session_date"),
    ("FACT_CANDIDATURE",  "application_start_date"),
    ("FACT_CANDIDATURE",  "application_end_date"),
    ("FACT_CANDIDATURE",  "decision_date"),
    ("FACT_CANDIDATURE",  "interview_date"),
    ("FACT_CANDIDATURE",  "enrollment_date"),
    ("DIM_STUDENT",       "birth_date"),
    ("DIM_STUDENT",       "enrollment_date"),
    ("DIM_STUDENT",       "admission_date"),
    ("DIM_STUDENT",       "exit_date"),
]

for table, col in date_fixes:
    try:
        full_path = f"{gold_lh}.dbo.{table}"
        df = spark.table(full_path)

        # Only fix if column exists and is currently a string
        col_type = dict(df.dtypes).get(col)
        if col_type is None:
            print(f"   SKIP   {table}.{col} — column not found")
            continue
        if col_type == "date":
            print(f"   SKIP   {table}.{col} — already DATE type")
            continue

        df_fixed = df.withColumn(col, F.to_date(F.col(col)))
        df_fixed.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_path)
        print(f"   OK     {table}.{col}  ({col_type} -> date)")

    except Exception as e:
        print(f"   FAILED {table}.{col} — {str(e)[:80]}")

print("\nDate fix complete — Power BI time intelligence is now enabled")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# FACT_CANDIDATURE — ENROLLMENT FLAG (Fixed)
# ==============================================================
from pyspark.sql import functions as F

gold_lh = "Gold"

print("Adding is_enrolled flag to FACT_CANDIDATURE...")

fc = spark.table(f"{gold_lh}.dbo.FACT_CANDIDATURE")
ds = spark.table(f"{gold_lh}.dbo.DIM_STUDENT").select("student_code")

# After left join: _enrolled_marker is True if matched, NULL if not matched
fc_flagged = fc.join(
    ds.withColumn("_enrolled_marker", F.lit(True)),
    "student_code",
    "left"
).withColumn(
    "is_enrolled",
    # FIX: use isNotNull() instead of isTrue() — isTrue() does not exist in PySpark
    F.when(F.col("_enrolled_marker").isNotNull(), F.lit("true"))
     .otherwise(F.lit("false"))
).drop("_enrolled_marker")

# Save back to Gold
output_path = f"{gold_lh}.dbo.FACT_CANDIDATURE"
fc_flagged.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_path)

# Summary
total      = fc_flagged.count()
enrolled   = fc_flagged.filter(F.col("is_enrolled") == "true").count()
unenrolled = fc_flagged.filter(F.col("is_enrolled") == "false").count()

print(f"\nSUCCESS: FACT_CANDIDATURE updated")
print(f"   Total candidatures : {total:,}")
print(f"   Enrolled students  : {enrolled:,}  ({enrolled/total:.0%})")
print(f"   Applicants only    : {unenrolled:,}  ({unenrolled/total:.0%})")

print(f"\nStatus breakdown of non-enrolled applicants:")
fc_flagged.filter(F.col("is_enrolled") == "false") \
    .groupBy("status_label") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(15, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================
# FINAL VALIDATION — Run this last before Power BI
# ==============================================================
from pyspark.sql import functions as F

gold_lh = "Gold"

print("FINAL GOLD VALIDATION")
print("=" * 70)

# Row counts
print("\n1. Row counts:")
tables = [
    "DIM_STUDENT", "DIM_COURSE", "DIM_PROGRAMME",
    "FACT_ATTENDANCE", "FACT_GRADES",
    "FACT_FINANCE", "FACT_CANDIDATURE"
]
for table in tables:
    try:
        count = spark.table(f"{gold_lh}.dbo.{table}").count()
        print(f"   {table:30s} : {count:,}")
    except Exception as e:
        print(f"   {table:30s} : ERROR — {str(e)[:50]}")

# Join key validation
print("\n2. Join key validation (FACT -> DIM_STUDENT):")
ds    = spark.table(f"{gold_lh}.dbo.DIM_STUDENT")
facts = [
    "FACT_ATTENDANCE",
    "FACT_GRADES",
    "FACT_FINANCE",
    "FACT_CANDIDATURE",
]
all_ok = True
for table in facts:
    df        = spark.table(f"{gold_lh}.dbo.{table}")
    total     = df.count()
    matched   = df.join(ds, "student_code", "inner").count()
    ratio     = matched / total if total > 0 else 0
    status    = "OK" if ratio >= 0.70 else "WARNING"
    if ratio < 0.70:
        all_ok = False
    print(f"   {table:30s} : {ratio:.0%} matched  ({matched:,}/{total:,})  {status}")

# Date type check
print("\n3. Date column type check:")
date_checks = [
    ("FACT_ATTENDANCE",  "session_date"),
    ("FACT_GRADES",      "session_start_date"),
    ("FACT_CANDIDATURE", "application_start_date"),
    ("DIM_STUDENT",      "birth_date"),
]
for table, col in date_checks:
    try:
        df       = spark.table(f"{gold_lh}.dbo.{table}")
        col_type = dict(df.dtypes).get(col, "NOT FOUND")
        status   = "OK" if col_type == "date" else "WARNING — still string"
        print(f"   {table:30s}.{col:30s} : {col_type}  {status}")
    except Exception as e:
        print(f"   {table:30s}.{col:30s} : ERROR")

# is_enrolled check
print("\n4. FACT_CANDIDATURE enrollment flag:")
try:
    fc         = spark.table(f"{gold_lh}.dbo.FACT_CANDIDATURE")
    enrolled   = fc.filter(F.col("is_enrolled") == "true").count()
    unenrolled = fc.filter(F.col("is_enrolled") == "false").count()
    total      = fc.count()
    has_flag   = "is_enrolled" in fc.columns
    print(f"   is_enrolled column present : {'YES' if has_flag else 'NO — run enrollment flag shell'}")
    print(f"   Enrolled                   : {enrolled:,}  ({enrolled/total:.0%})")
    print(f"   Not enrolled               : {unenrolled:,}  ({unenrolled/total:.0%})")
except Exception as e:
    print(f"   ERROR: {e}")

print("\n" + "=" * 70)
if all_ok:
    print("ALL CHECKS PASSED — You are ready to connect Power BI")
else:
    print("WARNING: Some checks failed — review above before connecting Power BI")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
