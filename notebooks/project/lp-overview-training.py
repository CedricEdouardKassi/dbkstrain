# Databricks notebook source
# MAGIC %md
# MAGIC # Atelier pratique :  projet fil rouge
# MAGIC -----------------------------------------
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


#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC II.2 [Sql] Afficher le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` 

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)

# COMMAND ----------

# MAGIC %md
# MAGIC II.3 [Python] Lire le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (en prenant en compte l'entête) dans le dataframe `raw_accidents_noschema_df` et l'afficher
# MAGIC Tips : https://spark.apache.org/docs/latest/sql-data-sources-csv.html

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC II.4 [Sql] Afficher le contenu du fichier csv `/Volumes/training/raw/accidents/US_Accidents_March23.csv` (en prenant  en compte l'entête)

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)

# COMMAND ----------

# MAGIC %md
# MAGIC II.5 [python] Afficher le schema du dataframe `raw_accidents_no_schema_df`

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC II.6 [python] Créer un dataframe `raw_accidents_df` à partir du fichier `/Volumes/training/train/rawdatas/US_Accidents_March23.csv` tout en inférant le schema et l'afficher

# COMMAND ----------

#TODO

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC II.7 [python] Sauvegarder le contenu du dataframe `raw_accidents_df` en tant table delta `training.accidents.us_accidents_clean_<votrenom>`

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %md
# MAGIC II.8 [sql] Afficher le contenu de votre table delta `training.accidents.us_accidents_clean_<votrenom>`

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # III. [BONUS] Analyse

# COMMAND ----------

# MAGIC %md
# MAGIC III.1 Arrondir la latitude et la longitude avec 6 chiffres significatifs

# COMMAND ----------

from pyspark.sql.functions import round, col

#TODO

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC III.2 Ne garder que les données postérieures au 8 février 2016

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)

# COMMAND ----------

# MAGIC %md
# MAGIC III.3 Compter le nombre d'accidents par niveau de gravité pour chaque état et le visualiser sous former de graphe

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)

# COMMAND ----------

# MAGIC %md
# MAGIC III.4 Calculer le nombre total d'accidents par état

# COMMAND ----------

#TODO

# COMMAND ----------

# MAGIC %sql
# MAGIC --(optionnel)
