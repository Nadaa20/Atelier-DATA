Projet Atelier – Intégration des Données

1. Script ETL : etl/off_etl_pyspark.py
2. Source : OpenFoodFacts (fichier TSV)
3. Pipeline : Bronze (fichier brut) → Silver (Spark) → Gold (MySQL)
4. Base cible : MySQL – schéma off_dm
5. Métriques qualité : quality_metrics.json

Exécution :
python etl/off_etl_pyspark.py
