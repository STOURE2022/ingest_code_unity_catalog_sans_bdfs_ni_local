Parfait ! 🎯 Voici une **documentation complète** de tous vos modules, prête pour présentation.

---

## 📚 **DOCUMENTATION COMPLÈTE DES MODULES WAX PIPELINE**

---

## 🏗️ **MODULES D'ORCHESTRATION (3 modules principaux)**

---

### **📦 1. unzip_module.py - Module de Dézipage**

**Rôle :** Extraction automatique des fichiers ZIP reçus

**Responsabilités :**
- Extraire les fichiers ZIP depuis `input/zip/`
- Organiser les fichiers extraits par table dans `extracted/[table]/`
- Archiver les ZIP traités dans `input/zip/processed/`
- Gérer les erreurs d'extraction (ZIP corrompus, etc.)

**Entrées :**
```
/Volumes/.../input/zip/
├── site_20250902.zip
├── customer_data.zip
└── product_catalog.zip
```

**Sorties :**
```
/Volumes/.../extracted/
├── site/
│   ├── site_20250902_120001.csv
│   └── site_20250906_120001.csv
├── customer/
│   └── customer_20250902.csv
└── product/
    └── product_20250902.csv

/Volumes/.../input/zip/processed/
└── site_20250902_20251014_200000.zip (archivé)
```

**Classe principale :**
```python
class UnzipManager:
    def process_all_zips() -> dict
    def _extract_single_zip(zip_filename) -> dict
    def _archive_zip(zip_filename)
    def list_extracted_files(table_name) -> list
```

**Fonctionnement :**
1. Scanne le répertoire `input/zip/`
2. Pour chaque ZIP trouvé :
   - Extrait les fichiers dans `extracted/[table_name]/`
   - Filtre les fichiers système (`.DS_Store`, `__MACOSX`)
   - Archive le ZIP traité avec timestamp
3. Retourne statistiques : ZIP traités, fichiers extraits, erreurs

**Durée moyenne :** 30 secondes

**Dépendances :**
- `zipfile` (Python standard)
- `os`, `shutil` (Python standard)
- Unity Catalog Volumes

**Utilisé par :** Module 2 (Auto Loader)

**Exemple de sortie :**
```
==========================================
📦 MODULE 1 : DÉZIPAGE
==========================================
✅ 3 fichier(s) ZIP trouvé(s)

📦 ZIP 1/3: site_20250902.zip
✅ 2 fichier(s) extrait(s)
   → /Volumes/.../extracted/site

📊 RÉSUMÉ DÉZIPAGE
==========================================
✅ ZIP extraits     : 3
❌ ZIP en échec     : 0
📄 Fichiers extraits : 8
==========================================
```

---

### **🔄 2. autoloader_module.py - Module Auto Loader**

**Rôle :** Ingestion automatique et continue des fichiers CSV

**Responsabilités :**
- Surveiller les répertoires `extracted/[table]/`
- Détecter automatiquement les nouveaux fichiers
- Ingérer les données dans tables `[table]_staging`
- Ajouter métadonnées de traçabilité (nom fichier, dates, timestamp)
- Gérer le checkpoint pour éviter doublons
- Inférer et gérer l'évolution du schéma

**Entrées :**
```
/Volumes/.../extracted/site/
├── site_20250902_120001.csv (105,628 lignes)
└── site_20250906_120001.csv (105,628 lignes)

/Volumes/.../input/config/
└── wax_configuration.xlsx (configuration des tables)
```

**Sorties :**
```
Tables Delta créées :
abu_catalog.gdp_poc_dev.site_staging (211,256 lignes)

Colonnes ajoutées :
├── FILE_NAME_RECEIVED  (nom du fichier source)
├── yyyy                (année extraite du nom)
├── mm                  (mois extrait du nom)
├── dd                  (jour extrait du nom)
└── INGESTION_TIMESTAMP (timestamp d'ingestion)

Checkpoint créé :
/Volumes/.../checkpoints/site/
```

**Classe principale :**
```python
class AutoLoaderModule:
    def process_all_tables(excel_config_path) -> dict
    def _process_single_table(table_name, config) -> dict
    def _add_metadata(df_stream) -> DataFrame
    def list_staging_tables() -> list
    def get_staging_stats() -> dict
```

**Fonctionnement :**
1. Lit la configuration Excel
2. Pour chaque table configurée :
   - Crée un stream Spark Structured Streaming
   - Configure Auto Loader (Cloud Files format)
   - Ajoute métadonnées (FILE_NAME, dates, timestamp)
   - Écrit dans table `[table]_staging`
   - Utilise checkpoint pour éviter doublons
3. Mode `trigger(once=True)` : traite une fois puis s'arrête

**Durée moyenne :** 1-2 minutes

**Technologies :**
- Spark Structured Streaming
- Databricks Auto Loader (cloudFiles)
- Delta Lake

**Dépendances :**
- Module 1 (fichiers extraits)
- Unity Catalog
- Excel de configuration

**Utilisé par :** Module 3 (Ingestion)

**Avantages Auto Loader :**
- ✅ Détection automatique nouveaux fichiers
- ✅ Checkpoint automatique (évite doublons)
- ✅ Scalabilité (millions de fichiers)
- ✅ Gestion erreurs intégrée
- ✅ Évolution schéma automatique

**Exemple de sortie :**
```
==========================================
🔄 MODULE 2 : AUTO LOADER
==========================================
📖 Lecture configuration
✅ 3 table(s) configurée(s)

==========================================
📋 Table 1/3: site
==========================================
📁 2 fichier(s) trouvés
📂 Source      : /Volumes/.../extracted/site
🗄️  Target      : site_staging
🔄 Création stream Auto Loader...
💾 Écriture vers site_staging...
⏳ Traitement en cours...
✅ 211,256 ligne(s) ingérée(s)

📊 RÉSUMÉ AUTO LOADER
==========================================
✅ Tables traitées  : 3
❌ Tables en échec  : 0
📈 Total lignes     : 325,489
==========================================
```

---

### **✅ 3. main.py - Module d'Ingestion Finale (waxng-ingestion)**

**Rôle :** Validation, transformation et ingestion finale des données

**Responsabilités :**
- Lire les données depuis tables `[table]_staging`
- Valider présence et types des colonnes
- Vérifier lignes corrompues (tolérance configurable)
- Appliquer typage des colonnes selon configuration
- Effectuer contrôles qualité (nulls, doublons, règles métier)
- Appliquer transformations métier (trim, nettoyage)
- Ingérer dans tables finales `[table]_all` et `[table]_last`
- Logger toutes les opérations (succès et erreurs)
- Générer rapport final

**Entrées :**
```
Tables Delta :
abu_catalog.gdp_poc_dev.site_staging (211,256 lignes)

Configuration Excel :
/Volumes/.../input/config/wax_configuration.xlsx
```

**Sorties :**
```
Tables Delta créées/mises à jour :
├── site_all        (historique complet, 211,256 lignes)
├── site_last       (données courantes, 105,628 lignes)
├── wax_execution_logs      (logs d'exécution)
└── wax_data_quality_errors (logs de qualité)

Table staging vidée :
site_staging (DELETE après traitement)

Rapport d'exécution généré
```

**Modules utilisés :**
- `validator.py` - Validations
- `column_processor.py` - Typage colonnes
- `file_processor.py` - Vérification corruption
- `ingestion.py` - Modes d'ingestion
- `delta_manager.py` - Gestion tables Delta
- `logger_manager.py` - Logging
- `simple_report_manager.py` - Rapport
- `utils.py` - Fonctions utilitaires

**Fonctionnement :**
1. Lit configuration Excel
2. Pour chaque table configurée :
   - Lit table `[table]_staging`
   - Valide colonnes attendues
   - Vérifie lignes corrompues (tolérance)
   - Applique typage des colonnes
   - Effectue contrôles qualité
   - Rejette lignes invalides
   - Ingère dans `[table]_all` (historique)
   - Ingère dans `[table]_last` (courant)
   - Log succès/erreurs
   - Vide table staging
3. Génère rapport final consolidé

**Durée moyenne :** 2-5 minutes

**Dépendances :**
- Module 2 (tables staging)
- Tous les modules utilitaires (11 modules)
- Unity Catalog
- Excel de configuration

**Utilisé par :** Utilisateurs finaux (via tables _all/_last)

**Exemple de sortie :**
```
==========================================
🚀 WAX PIPELINE - MODULE 3 : INGESTION
==========================================
📖 Lecture depuis : site_staging
✅ 211,256 ligne(s) trouvée(s)
📁 2 fichier(s) source détecté(s)

📄 Fichier 1/2: site_20250902_120001.csv
✅ 105,628 ligne(s)
✅ Validation colonnes : OK
✅ Lignes corrompues : 0
✅ Typage colonnes : OK
✅ Qualité données : 0 erreurs
💾 Ingestion...
✅ Fichier traité en 32.5s

🎉 TRAITEMENT TERMINÉ
==========================================
✅ Fichiers traités : 2
❌ Fichiers en échec : 0
==========================================
```

---

## 🔧 **MODULES UTILITAIRES (11 modules)**

---

### **✅ 4. validator.py - Validations**

**Rôle :** Valider la conformité des données

**Fonctions principales :**
```python
validate_filename(filename, table, path) -> bool
    # Valide format et date du nom de fichier
    
validate_columns_presence(df, expected_cols) -> (bool, DataFrame)
    # Vérifie présence des colonnes attendues
    
check_data_quality(df, table, merge_keys) -> DataFrame
    # Contrôles qualité :
    # - Valeurs nulles sur colonnes obligatoires
    # - Doublons sur clés métier
    # - Formats spécifiques (emails, dates, etc.)
    
validate_and_rebuild_dataframe(df, column_defs) -> (DataFrame, list, list)
    # Validation finale et reconstruction avec types corrects
```

**Exemples de validations :**
- ✅ Nom fichier : `site_20250902_120001.csv` → Date valide
- ❌ Nom fichier : `site_20251302_120001.csv` → Mois 13 invalide
- ✅ Colonnes : SITE_ID, SITE_NAME présentes
- ❌ Colonnes : SITE_CODE manquante
- ✅ Qualité : Aucun doublon sur SITE_ID
- ❌ Qualité : 15 valeurs nulles sur SITE_NAME (obligatoire)

---

### **🔄 5. column_processor.py - Typage des Colonnes**

**Rôle :** Convertir et typer les colonnes selon configuration

**Fonctions principales :**
```python
process_columns(df, column_defs) -> (DataFrame, list, list)
    # Applique typage selon config Excel :
    # - string → StringType
    # - integer → IntegerType
    # - date → DateType (avec format)
    # - decimal → DecimalType
    # etc.
    
reject_invalid_lines(df, invalid_flags) -> (DataFrame, list)
    # Rejette lignes avec erreurs de typage
```

**Exemple :**
```python
Avant :
SITE_ID: "S001" (string)
CREATED_DATE: "2025-09-02" (string)

Après :
SITE_ID: "S001" (string)
CREATED_DATE: 2025-09-02 (date)
```

**Gestion erreurs :**
- Valeur "ABC" pour colonne integer → Ligne rejetée
- Date "32/13/2025" invalide → Ligne rejetée
- Erreurs loggées dans `wax_data_quality_errors`

---

### **📄 6. file_processor.py - Traitement Fichiers**

**Rôle :** Vérifier corruption des données (version simplifiée)

**Fonction principale :**
```python
check_corrupt_records(df, total_rows, tolerance) -> (DataFrame, int, bool)
    # Vérifie colonne _corrupt_record créée par Auto Loader
    # Compare nombre lignes corrompues vs tolérance
    # Décide : continuer ou abort
```

**Exemple :**
```python
Fichier : 100,000 lignes
Corrompues : 50 lignes
Tolérance : 10% (10,000 lignes)
Résultat : ✅ Continue (50 < 10,000)

Fichier : 100,000 lignes
Corrompues : 15,000 lignes
Tolérance : 10% (10,000 lignes)
Résultat : ❌ Abort (15,000 > 10,000)
```

---

### **💾 7. delta_manager.py - Gestion Tables Delta**

**Rôle :** Gérer les opérations sur tables Delta Lake

**Fonctions principales :**
```python
create_or_update_table(df, table_name, mode) -> None
    # Crée ou met à jour table Delta
    
merge_into_table(df, table_name, merge_keys) -> None
    # MERGE avec gestion upserts/deletes
    
optimize_table(table_name) -> None
    # OPTIMIZE pour performance
    
vacuum_table(table_name, retention_hours) -> None
    # VACUUM pour nettoyer anciennes versions
```

**Opérations Delta :**
- CREATE TABLE IF NOT EXISTS
- INSERT INTO (mode append)
- MERGE INTO (mode upsert)
- OPTIMIZE + Z-ORDER
- VACUUM (nettoyage)

---

### **📊 8. ingestion.py - Modes d'Ingestion**

**Rôle :** Appliquer les différents modes d'ingestion

**Modes supportés :**

**FULL_SNAPSHOT (Snapshot complet) :**
```python
# Remplace toutes les données
# Usage : Fichiers complets quotidiens
DELETE FROM table_all WHERE yyyy=2025 AND mm=9 AND dd=2
INSERT INTO table_all (nouvelles données)
```

**APPEND (Ajout incrémental) :**
```python
# Ajoute sans supprimer
# Usage : Logs, événements
INSERT INTO table_all (nouvelles données)
```

**UPSERT (Mise à jour ou insertion) :**
```python
# Merge sur clés métier
# Usage : Données de référence
MERGE INTO table_all
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

**Fonctions principales :**
```python
apply_ingestion_mode(df, table, mode, zone) -> None
    # Applique le mode d'ingestion configuré
    
update_last_table(df, table) -> None
    # Met à jour table_last (données courantes)
```

---

### **📝 9. logger_manager.py - Logging**

**Rôle :** Logger toutes les opérations du pipeline

**Tables de logs :**

**wax_execution_logs :**
```sql
table_name       : site
filename         : site_20250902_120001.csv
input_format     : csv
ingestion_mode   : FULL_SNAPSHOT
output_zone      : internal
row_count        : 105,628
column_count     : 31
status           : SUCCESS
duration         : 32.5
error_message    : NULL
log_ts           : 2025-10-14 20:50:37
```

**wax_data_quality_errors :**
```sql
table_name       : site
filename         : site_20250902_120001.csv
column_name      : SITE_CODE
error_message    : Null value on mandatory column
error_count      : 15
log_ts           : 2025-10-14 20:50:38
```

**Fonctions principales :**
```python
log_execution(table, filename, status, ...) -> None
    # Log exécution fichier
    
write_quality_errors(errors_df, table) -> None
    # Log erreurs qualité
    
print_summary(table, filename, metrics) -> None
    # Affiche résumé console
    
calculate_final_metrics(df, errors_df) -> dict
    # Calcule métriques finales
```

---

### **📊 10. simple_report_manager.py - Rapport Final**

**Rôle :** Générer rapport d'exécution consolidé

**Sections du rapport :**
1. **Résumé exécution** (statut, durée, taux succès)
2. **Fichiers réussis** (liste avec détails)
3. **Fichiers rejetés** (liste avec raisons)
4. **Tables créées** (statistiques)
5. **Alertes** (erreurs qualité, performance)

**Fonction principale :**
```python
generate_simple_report(total_success, total_failed, duration) -> None
    # Génère rapport complet lisible
```

**Exemple de rapport :**
```
==========================================
📊 RAPPORT D'EXÉCUTION WAX PIPELINE
==========================================
✅ STATUT : SUCCÈS COMPLET

📊 RÉSULTAT :
   ✅ Fichiers réussis  : 5
   ❌ Fichiers rejetés  : 1
   📈 Taux de succès    : 83%

✅ FICHIERS TRAITÉS AVEC SUCCÈS
   1. site_20250902.csv : 105,628 lignes
   2. site_20250906.csv : 105,628 lignes
   ...

❌ FICHIERS REJETÉS
   1. site_20251302.csv
      Raison : Invalid date: Month 13

🗄️  TABLES CRÉÉES
   1. SITE
      • site_all : 211,256 lignes
      • site_last : 105,628 lignes
```

---

### **📊 11. dashboards.py - Dashboards & Métriques**

**Rôle :** Créer et afficher dashboards d'observabilité

**Dashboards disponibles :**
- Historique d'exécution
- Taux de succès par table
- Volume de données traité
- Erreurs qualité par type
- Performance (durée, throughput)

**Fonctions principales :**
```python
create_execution_logs_table() -> None
create_quality_logs_table() -> None
display_all_dashboards() -> None
```

---

### **🔧 12. maintenance.py - Maintenance**

**Rôle :** Opérations de maintenance sur tables Delta

**Opérations :**
```python
optimize_all_tables() -> None
    # OPTIMIZE sur toutes les tables
    
vacuum_all_tables(retention_hours=168) -> None
    # VACUUM (7 jours de rétention)
    
analyze_table_stats(table_name) -> dict
    # Statistiques détaillées d'une table
```

---

### **⚙️ 13. config.py - Configuration**

**Rôle :** Centraliser toute la configuration du pipeline

**Paramètres :**
```python
catalog           : abu_catalog
schema_files      : databricksassetbundletest
volume            : externalvolumetes
schema_tables     : gdp_poc_dev
env               : dev
version           : v1
```

**Chemins calculés :**
```python
volume_base       : /Volumes/abu_catalog/.../externalvolumetes
zip_path          : /Volumes/.../input/zip
extract_dir       : /Volumes/.../extracted
excel_path        : /Volumes/.../input/config/wax_configuration.xlsx
log_execution_path: /Volumes/.../logs/execution
log_quality_path  : /Volumes/.../logs/quality
```

---

### **🛠️ 14. utils.py - Fonctions Utilitaires**

**Rôle :** Fonctions réutilisables

**Fonctions principales :**
```python
parse_bool(value, default) -> bool
    # Convertit string en bool
    
normalize_delimiter(delimiter) -> str
    # Normalise délimiteur (\\t → \t)
    
extract_parts_from_filename(filename) -> dict
    # Extrait yyyy, mm, dd du nom fichier
    
build_regex_pattern(pattern) -> tuple
    # Construit regex pour matching fichiers
    
parse_tolerance(tolerance_str, total) -> int
    # Convertit "10%" en nombre absolu
    
safe_count(df) -> int
    # Count sécurisé avec gestion erreurs
    
deduplicate_columns(df) -> DataFrame
    # Supprime colonnes dupliquées
```

---

## 📊 **RÉCAPITULATIF : Interactions Entre Modules**

```
┌──────────────────────────────────────────────────────────────┐
│                    MODULES PRINCIPAUX (3)                     │
├──────────────────────────────────────────────────────────────┤
│  unzip_module.py          → Extraction ZIP                   │
│  autoloader_module.py     → Ingestion Auto Loader            │
│  main.py                  → Validation + Ingestion finale    │
└──────────────────────────────────────────────────────────────┘
                              ↓ utilisent
┌──────────────────────────────────────────────────────────────┐
│                  MODULES UTILITAIRES (11)                     │
├──────────────────────────────────────────────────────────────┤
│  validator.py             → Validations                      │
│  column_processor.py      → Typage colonnes                  │
│  file_processor.py        → Vérif corruption                 │
│  delta_manager.py         → Gestion Delta                    │
│  ingestion.py             → Modes ingestion                  │
│  logger_manager.py        → Logging                          │
│  simple_report_manager.py → Rapport                          │
│  dashboards.py            → Dashboards                       │
│  maintenance.py           → Maintenance                      │
│  config.py                → Configuration                    │
│  utils.py                 → Utilitaires                      │
└──────────────────────────────────────────────────────────────┘
```

---

## 🎯 **Statistiques du Projet**

| Métrique | Valeur |
|----------|--------|
| **Modules principaux** | 3 |
| **Modules utilitaires** | 11 |
| **Total modules** | 14 |
| **Lignes de code total** | ~3,500 |
| **Fonctions principales** | ~80 |
| **Tables Delta créées** | 2 par table source + 2 logs |
| **Durée totale pipeline** | 4-8 minutes |

---

Voilà ! Documentation complète et professionnelle de tous vos modules ! 📚🎯
