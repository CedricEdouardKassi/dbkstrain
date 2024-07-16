# Databricks notebook source
# MAGIC %md
# MAGIC # Atelier pratique :  projet fil rouge
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # I. A propos du jeu de données
# MAGIC
# MAGIC ## Description de la base de données
# MAGIC Il s'agit d'un ensemble de données sur les accidents de voiture à l'échelle nationale qui couvre 49 États des États-Unis. Les données sur les accidents ont été collectées entre février 2016 et mars 2023, à l'aide de plusieurs API qui fournissent des données en continu sur les incidents (ou événements) de la circulation. Ces API diffusent des données sur le trafic saisies par diverses entités, notamment les départements des transports des États-Unis et des États, les forces de l'ordre, les caméras de surveillance du trafic et les capteurs de trafic situés sur les réseaux routiers. L'ensemble de données contient actuellement environ 7,7 millions d'enregistrements d'accidents. Pour plus d'informations sur cet ensemble de données, veuillez consulter le site suivant.
# MAGIC ## Remerciements
# MAGIC Si vous utilisez cet ensemble de données, veuillez citer les articles suivants :
# MAGIC Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy et Rajiv Ramnath. « A Countrywide Traffic Accident Dataset », 2019.
# MAGIC Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy, Radu Teodorescu et Rajiv Ramnath. « Accident Risk Prediction based on Heterogeneous Sparse Data : New Dataset and Insights ». In proceedings of the 27th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems, ACM, 2019.
# MAGIC ## Contenu
# MAGIC Cet ensemble de données a été collecté en temps réel à l'aide de plusieurs API de trafic. Il contient des données d'accidents collectées de février 2016 à mars 2023 pour les États-Unis contigus. Pour plus de détails sur ce jeu de données, veuillez consulter [ici].
# MAGIC ## Inspiration
# MAGIC Le jeu de données US-Accidents peut être utilisé pour de nombreuses applications, telles que la prédiction en temps réel des accidents de la route, l'étude de la sécurité routière, l'évaluation de la sécurité routière, etc.
# MAGIC
# MAGIC
# MAGIC ## Autres détails
# MAGIC Veuillez noter qu'il peut manquer des données pour certains jours, ce qui peut être dû à des problèmes de connectivité du réseau lors de la collecte des données. Malheureusement, l'ensemble de données ne sera plus mis à jour et cette version doit être considérée comme la plus récente.
# MAGIC
# MAGIC ## Politique d'utilisation et clause de non-responsabilité
# MAGIC Cet ensemble de données est distribué uniquement à des fins de recherche sous la licence Creative Commons Attribution-Noncommercial-ShareAlike (CC BY-NC-SA 4.0). En téléchargeant l'ensemble de données, vous acceptez de l'utiliser uniquement à des fins non commerciales, de recherche ou d'enseignement. Si vous utilisez cet ensemble de données, il est nécessaire de citer les articles mentionnés ci-dessus.

# COMMAND ----------

# MAGIC %md
# MAGIC # II. Chargement du dataset

# COMMAND ----------

# MAGIC %md
# MAGIC **II.1 [Python]** Lire le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (sans prendre en compte l'entête) dans le dataframe raw_accidents_noheader_df et l'afficher
# MAGIC
# MAGIC Tips : https://spark.apache.org/docs/latest/sql-data-sources-csv.html 

# COMMAND ----------


raw_accidents_noheader = spark.read.csv("/Volumes/training/train/rawdatas/US_Accidents_March23.csv")
display(raw_accidents_noheader)

# COMMAND ----------

# MAGIC %md
# MAGIC II.2 [Sql] Afficher le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (sans prendre en compte l'entête)

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC II.3 [Python] Lire le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (en prenant en compte l'entête) dans le dataframe `raw_accidents_noschema_df` et l'afficher
# MAGIC Tips : https://spark.apache.org/docs/latest/sql-data-sources-csv.html

# COMMAND ----------

raw_accidents_noschema_df = spark.read.option("header", "true").csv("/Volumes/training/train/rawdatas/US_Accidents_March23.csv")
display(raw_accidents_noschema_df)

# COMMAND ----------

# MAGIC %md
# MAGIC II.4 [Sql] Afficher le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (en prenant  en compte l'entête)

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %md
# MAGIC II.5 [python] Afficher le schema du dataframe `raw_accidents_no_schema_df`

# COMMAND ----------

raw_accidents_noschema_df.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC II.6 [python] Créer un dataframe `raw_accidents_df` à partir du fichier `/Volumes/training/train/rawdatas/US_Accidents_March23.csv` tout en inférant le schema et l'afficher

# COMMAND ----------

raw_accidents_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/training/train/rawdatas/US_Accidents_March23.csv")

display(raw_accidents_df)

# COMMAND ----------

# MAGIC %md
# MAGIC II.7 [python] Sauvegarder le contenu du dataframe `raw_accidents_df` en tant table delta `training.accidents.us_accidents_clean_<votrenom>`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC II.8 [sql] Afficher le contenu de votre table delta `training.accidents.us_accidents_clean_<votrenom>`

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # III. [BONUS] Analyse

# COMMAND ----------

# MAGIC %md
# MAGIC III.1 Arrondir la latitude et la longitude avec 6 chiffres significatifs

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC III.2 Ne garder que les données postérieures au 8 février 2016

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC III.3 Compter le nombre d'accidents par niveau de gravité pour chaque état et le visualiser sous former de graphe

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC III.4 Calculer le nombre total d'accidents par état

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC III.5 Utiliser les jointures pour calculer le pourcentage que représente chaque niveau de sévérité par état

# COMMAND ----------

rawDf.withColumn("Start_Lat", round($"Start_Lat"))

# COMMAND ----------

from pyspark.sql.functions import round, col


# COMMAND ----------

roundedDf = rawDf.withColumn("Start_Lat", round(col("Start_Lat"), 6))\
    .withColumn("Start_Lat", round(col("Start_Lat"), 6))\
    .withColumn("Start_Lng", round(col("Start_Lng"), 6))
 # editer plus tard en ajoutant .cache
display(roundedDf)

# COMMAND ----------

display(roundedDf.select("Source"))

# COMMAND ----------

display(roundedDf.select("Source").dropDuplicates())
#display(roundedDf.select("Source").distinct())

# COMMAND ----------

accidentsByStateAndSev = roundedDf.select("Severity", "State").groupBy("State", "Severity").count()
display(accidentsByStateAndSev)

# COMMAND ----------

recentsOnly = roundedDf.where(col("Start_Time") > "2016-02-08T06:40:00.000+0000")
recentsAccidentsByStateAndSev = recentsOnly.select("Severity", "State").groupBy("State", "Severity").count()
display(recentsAccidentsByStateAndSev)

# COMMAND ----------

recentsByState = recentsOnly.select("State").groupBy("State").count().withColumnRenamed("count", "Total Accidents in State")

display(recentsByState)

# COMMAND ----------

joinedDf = recentsAccidentsByStateAndSev.join(recentsByState, "State")
display(joinedDf)

# COMMAND ----------

percentagedDf = joinedDf.withColumn("Representativeness in state accidents", round(col("count")*100/col("Total Accidents in State"), 2))
display(percentagedDf)

# COMMAND ----------

statsDf = percentagedDf.drop("count", "Total Accidents in State")
display(statsDf)

# COMMAND ----------



# COMMAND ----------

# FIN JOUR 1

# COMMAND ----------

def isLikely(percentageCol):
  from pyspark.sql.functions import when
  return when(percentageCol < 25, "Rare").otherwise(when(percentageCol < 75, "Common").otherwise("Frequent"))

df_query = statsDf.select("State", "Severity", "Representativeness in state accidents", isLikely(col("Representativeness in state accidents")))
display(df_query)

# COMMAND ----------

df_query = statsDf.select("State", "Severity", "Representativeness in state accidents", isLikely(col("Representativeness in state accidents")).alias("isLikely"))
display(df_query)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
window = Window.orderBy("State", "County").partitionBy("State")
accidentRankInTheCounty = rawDfNoSchema.withColumn("Accident No in County", dense_rank().over(window))

# COMMAND ----------

display(accidentRankInTheCounty)

# COMMAND ----------


window2 = Window.orderBy("State", "County").partitionBy("State", "County")
accidentRankInTheCounty2 = rawDfNoSchema.withColumn("Accident No in County", dense_rank().over(window2))
display(accidentRankInTheCounty2)

# COMMAND ----------

from pyspark.sql.functions import row_number

accidentRankInTheCounty3 = rawDfNoSchema.withColumn("Accident No in County", row_number().over(window2))
display(accidentRankInTheCounty3.select("ID", "State", "County", "City", "Accident No in County"))

# COMMAND ----------

+ toDF du resultat d'une API
x ecriture 
x params dans les notebooks
x repartition/coalesce
x debug / spark UI
+ workflows
x SQL
