# Data Quality Report — OpenFoodFacts Datamart

## 1. Objectif

Ce document décrit les règles de qualité, les contrôles appliqués et les résultats obtenus lors de la construction du datamart OpenFoodFacts à partir du dataset brut.

Le pipeline est implémenté en PySpark et alimente un datamart MySQL en modèle en étoile / flocon.

L’objectif est de garantir :
- La cohérence des dimensions
- L’unicité des produits
- La traçabilité des anomalies
- La mesure de la complétude des données

---

## 2. Périmètre des données

### Source
- OpenFoodFacts  
- Fichier : `en.openfoodfacts.org.products.csv`  
- Format réel : TSV (séparateur `\t`)

### Tables cibles
- `dim_brand`
- `dim_country`
- `dim_category`
- `dim_time`
- `dim_product`

---

## 3. Règles de qualité implémentées

### 3.1 Nettoyage des champs texte
- Suppression des chaînes vides (`""` → `NULL`)
- Suppression des espaces inutiles (trim)
- Normalisation des clés de dimensions en format ASCII safe
- Limitation de la longueur des clés :
  - Brand : 255 caractères
  - Category : 255 caractères
  - Country : 64 caractères

Ces règles évitent les erreurs d’encodage JDBC et les collisions sur les contraintes d’unicité MySQL.

---

### 3.2 Déduplication
- Déduplication sur la clé métier `code`
- En cas de doublon, conservation de l’enregistrement le plus récent à l’aide du champ `last_modified_t`

Objectif : garantir qu’un produit ne soit représenté qu’une seule fois dans le datamart.

---

### 3.3 Filtrage des lignes vides
Un produit est conservé uniquement s’il possède au moins une des informations suivantes :
- Nom du produit
- Marque
- Catégorie
- Pays
- Valeur nutritionnelle (énergie, sucre ou sel)

Cela permet d’exclure les lignes purement techniques ou vides de valeur analytique.

---

### 3.4 Règles d’anomalies nutritionnelles

#### 3.4.1 Sucre
- `sugars_100g < 0`
- `sugars_100g > 100`

#### 3.4.2 Sel
- `salt_100g < 0`
- `salt_100g > 25`

#### 3.4.3 Cohérence sel / sodium
Dans la nutrition, la relation théorique est :

salt ≈ sodium × 2.5


Un ratio est calculé :

ratio = salt_100g / (sodium_100g × 2.5)


Une anomalie est levée si :
- `ratio < 0.8`
- `ratio > 1.2`

Cela permet d’identifier des incohérences de saisie ou d’unités entre les champs sodium et sel.

---

## 4. Score de complétude

Chaque produit reçoit un score de complétude compris entre `0` et `1`.

Cinq critères sont évalués :
- Nom du produit
- Marque
- Catégorie
- Pays
- Valeurs nutritionnelles

Formule :

completeness_score =
(has_name + has_brand + has_category + has_country + has_nutrients) / 5


Interprétation :
- `0.0` → produit très incomplet
- `1.0` → produit entièrement renseigné

Ce score permet d’évaluer la qualité globale du dataset et d’identifier les zones à faible couverture d’information.

---

## 5. Traçabilité des anomalies

Chaque produit possède un champ :
- `quality_issues_json`

Exemple :
```json
{"anomaly": true}