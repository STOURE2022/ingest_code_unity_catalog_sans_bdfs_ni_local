"""
main.py
WAX Pipeline - Unity Catalog uniquement
Version simplifiée : traitement séparé de tous les fichiers (pas de fusion)
"""

import sys
import os
import time
import re
from functools import reduce

# Import modules WAX
from config import Config
from validator import DataValidator
from file_processor import FileProcessor
from column_processor import ColumnProcessor
from delta_manager import DeltaManager
from logger_manager import LoggerManager
from ingestion import IngestionManager
from dashboards import DashboardManager
from maintenance import MaintenanceManager
from utils import (
    parse_bool, normalize_delimiter, parse_header_mode,
    deduplicate_columns, extract_parts_from_filename,
    build_regex_pattern, build_schema_from_config, safe_count, parse_tolerance
)

import pandas as pd
from pyspark.sql import SparkSession, functions as F


def main():
    """Point d'entrée pipeline - Unity Catalog avec traitement séparé des fichiers"""

    print("=" * 80)
    print("🚀 WAX PIPELINE - UNITY CATALOG")
    print("=" * 80)

    # ========== CONFIGURATION ==========

    print("\n⚙️  Configuration Unity Catalog...")

    # Configuration (à adapter selon votre environnement)
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1",
        auto_detect_files=True  # Auto-détection activée
    )

    config.print_config()

    # Validation
    validation = config.validate_paths()
    if not validation["valid"]:
        print("\n⚠️ Avertissements:")
        for issue in validation["issues"]:
            print(f"  {issue}")
    else:
        print(f"\n✅ Configuration validée - {validation['mode']}")
        for issue in validation["issues"]:
            if issue.startswith("ℹ️"):
                print(f"  {issue}")

    # ========== INITIALISATION SPARK ==========

    print("\n⚡ Initialisation Spark...")
    spark = SparkSession.builder.getOrCreate()
    print(f"✅ Spark version : {spark.version}")

    # ========== MANAGERS ==========

    validator = DataValidator(spark)
    file_processor = FileProcessor(spark, config)
    column_processor = ColumnProcessor(spark, config)
    delta_manager = DeltaManager(spark, config)
    logger_manager = LoggerManager(spark, config)
    ingestion_manager = IngestionManager(spark, config, delta_manager)
    dashboard_manager = DashboardManager(spark, config)
    maintenance_manager = MaintenanceManager(spark, config)

    print("✅ Managers initialisés\n")

    # ========== EXTRACTION ZIP ==========

    print("=" * 80)
    print("📦 EXTRACTION ZIP")
    print("=" * 80)

    try:
        file_processor.extract_zip()
    except Exception as e:
        print(f"❌ Erreur extraction ZIP : {e}")
        import traceback
        traceback.print_exc()
        return

    # ========== LECTURE EXCEL ==========

    print("\n" + "=" * 80)
    print("📑 LECTURE CONFIGURATION EXCEL")
    print("=" * 80)

    print(f"📄 Fichier : {config.excel_path}")

    try:
        file_columns_df = pd.read_excel(config.excel_path, sheet_name="Field-Column")
        file_tables_df = pd.read_excel(config.excel_path, sheet_name="File-Table")
        print(f"✅ Config chargée : {len(file_tables_df)} tables, {len(file_columns_df)} colonnes")
    except Exception as e:
        print(f"❌ Erreur lecture Excel : {e}")
        import traceback
        traceback.print_exc()
        return

    # ========== TRAITEMENT TABLES ==========

    print("\n" + "=" * 80)
    print("📄 TRAITEMENT DES TABLES")
    print("=" * 80)

    total_success = 0
    total_failed = 0
    total_files_processed = 0

    for table_idx, trow in file_tables_df.iterrows():
        start_table_time = time.time()

        source_table = trow["Delta Table Name"]
        filename_pattern = str(trow.get("Filename Pattern", "")).strip()
        input_format = str(trow.get("Input Format", "csv")).strip().lower()
        output_zone = str(trow.get("Output Zone", "internal")).strip().lower()
        ingestion_mode = str(trow.get("Ingestion mode", "")).strip()

        print(f"\n{'=' * 80}")
        print(f"📋 Table {table_idx + 1}/{len(file_tables_df)}: {source_table}")
        print(f"   Zone: {output_zone}, Mode: {ingestion_mode}")
        print(f"{'=' * 80}")

        # Options
        trim_flag = parse_bool(trow.get("Trim", True), True)
        delimiter_raw = str(trow.get("Input delimiter", ","))
        del_cols_allowed = parse_bool(trow.get("Delete Columns Allowed", False), False)
        ignore_empty = parse_bool(trow.get("Ignore empty Files", True), True)
        
        charset = str(trow.get("Input charset", "UTF-8")).strip()
        if charset.lower() in ["nan", "", "none"]:
            charset = "UTF-8"

        invalid_gen = parse_bool(trow.get("Invalid Lines Generate", False), False)

        # Pattern matching
        try:
            rx_with_time, rx_without_time = build_regex_pattern(filename_pattern)
        except Exception as e:
            print(f"❌ Erreur pattern : {e}")
            logger_manager.log_execution(source_table, "N/A", input_format, ingestion_mode,
                                         output_zone, error_msg=f"Regex error: {e}",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        # Recherche fichiers
        try:
            all_files_paths = [os.path.join(config.extract_dir, f)
                               for f in os.listdir(config.extract_dir)]

            class FileInfo:
                def __init__(self, path):
                    self.path = path
                    self.name = os.path.basename(path)

            all_files = [FileInfo(p) for p in all_files_paths]
        except Exception as e:
            print(f"❌ Erreur listage : {e}")
            logger_manager.log_execution(source_table, "N/A", input_format, ingestion_mode,
                                         output_zone, error_msg=f"Directory error: {e}",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        matched = [fi for fi in all_files
                   if re.match(rx_with_time, fi.name) or re.match(rx_without_time, fi.name)]

        if len(matched) == 0:
            print(f"⚠️ Aucun fichier pour : {filename_pattern}")
            logger_manager.log_execution(source_table, "N/A", input_format, ingestion_mode,
                                         output_zone, error_msg="No file matching",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        print(f"✅ {len(matched)} fichier(s) trouvé(s)")

        # Validation noms fichiers
        files_to_read = []
        for fi in matched:
            parts = extract_parts_from_filename(fi.name)
            if validator.validate_filename(fi.name, source_table, fi.path, config.log_quality_path):
                files_to_read.append((fi.path, parts))

        if not files_to_read:
            print(f"⚠️ Tous fichiers rejetés")
            logger_manager.log_execution(source_table, "N/A", input_format, ingestion_mode,
                                         output_zone, error_msg="All files rejected",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        # Mode de traitement : TOUS les fichiers séparément
        print(f"\n🔄 Mode: Traitement séparé de {len(files_to_read)} fichier(s)")

        # Délimiteur
        try:
            sep_char = normalize_delimiter(delimiter_raw)
        except Exception as e:
            logger_manager.log_execution(source_table, "N/A", input_format, ingestion_mode,
                                         output_zone, error_msg=f"Delimiter error: {e}",
                                         status="FAILED", start_time=start_table_time)
            total_failed += 1
            continue

        # Bad records
        bad_records = None
        if invalid_gen:
            bad_records = config.get_bad_records_path(source_table)

        # Header
        header_mode = str(trow.get("Input header", ""))
        user_header, first_line_only = parse_header_mode(header_mode)

        # Colonnes attendues
        expected_cols = file_columns_df[file_columns_df["Delta Table Name"] == source_table]["Column Name"].tolist()

        # Schéma imposé
        imposed_schema = None
        try:
            subset = file_columns_df[file_columns_df["Delta Table Name"] == source_table].copy()
            if not subset.empty and "Field Order" in subset.columns:
                subset = subset.sort_values(by=["Field Order"])
                imposed_schema = build_schema_from_config(subset)
        except Exception as e:
            print(f"⚠️ Schéma imposé impossible : {e}")

        # Options lecture
        read_options = {
            "delimiter": sep_char,
            "user_header": user_header,
            "first_line_only": first_line_only,
            "charset": charset,
            "ignore_empty": ignore_empty,
            "bad_records": bad_records
        }

        # Colonnes pour fixed-width
        column_defs_for_table = file_columns_df[
            file_columns_df["Delta Table Name"] == source_table
        ].sort_values(by=["Field Order"]) if "Field Order" in file_columns_df.columns else None

        # Colonnes spéciales (merge keys)
        specials = file_columns_df[file_columns_df["Delta Table Name"] == source_table].copy()
        if "Is Special" in specials.columns:
            specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
            merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        else:
            merge_keys = []

        # ========== TRAITEMENT DE CHAQUE FICHIER SÉPARÉMENT ==========

        for file_idx, (matched_uri, parts) in enumerate(files_to_read):
            print(f"\n{'=' * 80}")
            print(f"📄 FICHIER {file_idx + 1}/{len(files_to_read)}")
            print(f"   Table    : {source_table}")
            print(f"   Fichier  : {os.path.basename(matched_uri)}")
            print(f"   Date     : {parts.get('yyyy', 'N/A')}-{parts.get('mm', 'N/A'):02d}-{parts.get('dd', 'N/A'):02d}")
            print(f"{'=' * 80}")

            start_file_time = time.time()
            filename_current = os.path.basename(matched_uri)

            # ========== LECTURE DU FICHIER ==========
            try:
                df_raw = file_processor.read_file(
                    matched_uri, input_format, read_options,
                    expected_cols, imposed_schema, column_defs_for_table
                )
            except Exception as e:
                print(f"❌ Erreur lecture : {e}")
                logger_manager.log_execution(
                    source_table, filename_current, input_format,
                    ingestion_mode, output_zone,
                    error_msg=f"Read error: {e}", status="FAILED",
                    start_time=start_file_time
                )
                continue

            # ========== VALIDATIONS ==========

            # Validation colonnes
            is_valid, err_df = validator.validate_columns_presence(
                df_raw, expected_cols, del_cols_allowed, source_table, filename_current
            )

            if not is_valid:
                logger_manager.write_quality_errors(err_df, source_table, zone=output_zone)
                logger_manager.log_execution(
                    source_table, filename_current, input_format,
                    ingestion_mode, output_zone,
                    error_msg="Missing columns", status="FAILED",
                    start_time=start_file_time
                )
                continue

            # Ajouter colonnes manquantes
            if expected_cols:
                from pyspark.sql.types import StringType
                for c in expected_cols:
                    if c not in df_raw.columns:
                        df_raw = df_raw.withColumn(c, F.lit(None).cast(StringType()))
                    else:
                        df_raw = df_raw.withColumn(c, df_raw[c].cast(StringType()))

            # Check corrupt records
            total_rows = safe_count(df_raw)
            rej_tol = parse_tolerance(trow.get("Rejected line per file tolerance", "10%"), total_rows)

            df_raw, corrupt_rows, should_abort = file_processor.check_corrupt_records(
                df_raw, total_rows, rej_tol, source_table, filename_current
            )

            if should_abort:
                logger_manager.log_execution(
                    source_table, filename_current, input_format,
                    ingestion_mode, output_zone,
                    row_count=total_rows, column_count=len(df_raw.columns),
                    error_msg="Too many corrupted lines", status="FAILED",
                    start_time=start_file_time
                )
                continue

            # Trim
            if trim_flag:
                for c in df_raw.columns:
                    df_raw = df_raw.withColumn(c, F.trim(F.col(c)))

            # Dédupliquer
            df_raw = deduplicate_columns(df_raw)
            total_rows_initial = safe_count(df_raw)

            print(f"✅ Lignes lues : {total_rows_initial:,}")

            # ========== TYPAGE COLONNES ==========

            df_raw, col_errors, invalid_flags = column_processor.process_columns(
                df_raw, column_defs_for_table, table_name=source_table,
                filename=filename_current, total_rows=total_rows_initial
            )

            # Rejeter lignes invalides
            df_raw, line_errors = column_processor.reject_invalid_lines(
                df_raw, invalid_flags, table_name=source_table, filename=filename_current
            )

            # Fusionner erreurs colonnes
            all_column_errors = []
            if col_errors:
                all_column_errors.extend(col_errors)
            if line_errors:
                all_column_errors.extend(line_errors)

            # ========== VALIDATION QUALITÉ ==========

            df_err_global = validator.check_data_quality(
                df_raw, source_table, merge_keys, filename=filename_current,
                column_defs=file_columns_df
            )

            # ========== VALIDATION FINALE TYPES ==========

            df_raw, errors_list, column_errors = validator.validate_and_rebuild_dataframe(
                df_raw, column_defs_for_table, source_table, filename_current
            )

            # Fusionner toutes erreurs
            if all_column_errors:
                df_col_err = reduce(lambda a, b: a.union(b), all_column_errors)
                df_err_global = df_err_global.union(df_col_err) if df_err_global else df_col_err

            if errors_list:
                df_final_err = reduce(lambda a, b: a.union(b), errors_list)
                df_err_global = df_err_global.union(df_final_err) if df_err_global else df_final_err

            # ========== INGESTION ==========

            print(f"\n💾 Ingestion fichier : {filename_current}")
            print(f"   Mode: {ingestion_mode}")
            print(f"   Destination : {config.catalog}.{config.schema_tables}.{source_table}_{{all,last}}")

            try:
                ingestion_manager.apply_ingestion_mode(
                    df_raw, column_defs=column_defs_for_table,
                    table_name=source_table,
                    ingestion_mode=ingestion_mode, zone=output_zone,
                    parts=parts,  # Date extraite du nom de fichier
                    file_name_received=filename_current  # Nom du fichier pour traçabilité
                )
            except Exception as e:
                print(f"❌ Erreur ingestion : {e}")
                import traceback
                traceback.print_exc()
                logger_manager.log_execution(
                    source_table, filename_current, input_format,
                    ingestion_mode, output_zone,
                    row_count=0, column_count=len(df_raw.columns),
                    error_msg=f"Ingestion error: {e}",
                    status="FAILED", start_time=start_file_time
                )
                continue

            # ========== MÉTRIQUES & LOGS ==========

            metrics = logger_manager.calculate_final_metrics(df_raw, df_err_global)

            logger_manager.print_summary(
                table_name=source_table, filename=filename_current,
                total_rows=(total_rows_initial, metrics["total_rows_after"]),
                corrupt_rows=metrics["corrupt_rows"],
                anomalies_total=metrics["anomalies_total"],
                cleaned_rows=metrics["cleaned_rows"], errors_df=df_err_global
            )

            logger_manager.write_quality_errors(df_err_global, source_table, zone=output_zone)

            logger_manager.log_execution(
                table_name=source_table, filename=filename_current,
                input_format=input_format,
                ingestion_mode=ingestion_mode, output_zone=output_zone,
                row_count=metrics["total_rows_after"],
                column_count=len(df_raw.columns),
                masking_applied=(output_zone == "secret"),
                error_msg=f"{metrics['anomalies_total']} errors" if metrics["anomalies_total"] > 0 else None,
                status="SUCCESS", error_count=metrics["anomalies_total"],
                start_time=start_file_time
            )

            duration = round(time.time() - start_file_time, 2)
            print(f"\n✅ Fichier {filename_current} traité en {duration}s")
            total_success += 1
            total_files_processed += 1

    # ========== RÉSUMÉ FINAL ==========

    print("\n" + "=" * 80)
    print("🎉 TRAITEMENT TERMINÉ")
    print("=" * 80)
    print(f"✅ Fichiers traités avec succès : {total_files_processed}")
    print(f"❌ Échecs : {total_failed}")

    if total_files_processed > 0:
        print(f"\n📊 Tables créées dans : {config.catalog}.{config.schema_tables}")
        print(f"   Format: <table_name>_all (historique) et <table_name>_last (courant)")

        # Lister les tables créées
        print(f"\n📋 Tables disponibles :")
        try:
            tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.schema_tables}").collect()
            wax_tables = [t for t in tables if "_all" in t.tableName or "_last" in t.tableName]
            
            for table in wax_tables:
                table_full = f"{config.catalog}.{config.schema_tables}.{table.tableName}"
                count = spark.table(table_full).count()
                print(f"   - {table.tableName}: {count:,} lignes")
        except Exception as e:
            print(f"   ⚠️ Impossible de lister les tables : {e}")

    print("=" * 80)

    # ========== DASHBOARDS ==========

    if total_files_processed > 0:
        try:
            print("\n" + "=" * 80)
            print("📊 GÉNÉRATION DES DASHBOARDS")
            print("=" * 80)
            dashboard_manager.display_all_dashboards()
        except Exception as e:
            print(f"⚠️ Erreur dashboards : {e}")
            import traceback
            traceback.print_exc()

    print("\n🎯 Pipeline terminé !")


if __name__ == "__main__":
    main()
```

## ✅ **Changements Apportés**

### **Ce qui a été ENLEVÉ :**
- ❌ Toute la logique `merge_files_flag`
- ❌ Le paramètre `process_all_separately`
- ❌ Le mode fusion
- ❌ Toute la branche conditionnelle `if merge_mode / else`
- ❌ Les références aux colonnes Excel pour la fusion

### **Ce qui RESTE :**
- ✅ **Traitement séparé de TOUS les fichiers**
- ✅ **Un log par fichier**
- ✅ **FILE_NAME_RECEIVED** ajouté automatiquement
- ✅ **Traçabilité complète** par fichier
- ✅ Code **plus simple et lisible**

## 📊 **Comportement**
```
Pour chaque table dans Excel:
  Pour chaque fichier matchant le pattern:
    ✅ Lire le fichier
    ✅ Valider
    ✅ Typer les colonnes
    ✅ Ingérer SÉPARÉMENT dans la table
    ✅ Logger individuellement
```

## 📝 **Configuration Excel (Simplifiée)**

**Onglet "File-Table"** - Plus besoin des colonnes de fusion :

| Delta Table Name | Filename Pattern | Input Format | Ingestion mode | ... |
|-----------------|------------------|--------------|----------------|-----|
| site | site_<yyyy><mm><dd>_*.csv | csv | FULL_SNAPSHOT | ... |

## 🎯 **Résultat Attendu**

Avec 3 fichiers `site_20250902_*.csv`, `site_20250906_*.csv`, `site_20251302_*.csv` :
```
📄 FICHIER 1/3
   Table: site
   Fichier: site_20250902_120001.csv
   ✅ 105,628 lignes
   ✅ Traité en 32.5s

📄 FICHIER 2/3
   Table: site
   Fichier: site_20250906_120001.csv
   ✅ 105,628 lignes
   ✅ Traité en 33.2s

📄 FICHIER 3/3
   Table: site
   Fichier: site_20251302_120001.csv
   ❌ Rejeté (mois 13 invalide)

🎉 TRAITEMENT TERMINÉ
✅ Fichiers traités: 2
❌ Échecs: 1

📊 Tables disponibles:
   - site_all: 211,256 lignes (historique)
   - site_last: 105,628 lignes (dernier fichier)
