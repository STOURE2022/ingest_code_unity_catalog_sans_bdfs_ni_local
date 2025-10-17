"""
autoloader_module.py
MODULE 2 : Auto Loader avec Auto-Discovery
Lit automatiquement tous les dossiers dans extracted/
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class AutoLoaderModule:
    """Module Auto Loader avec découverte automatique des dossiers"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog
        self.source_base = f"{config.volume_base}/extracted"
        self.checkpoint_base = f"{config.volume_base}/checkpoints"
        self.schema_base = f"{config.volume_base}/schemas"
    
    def process_all_tables(self, excel_config_path: str) -> dict:
        """
        Lance Auto Loader pour tous les dossiers trouvés dans extracted/
        
        Args:
            excel_config_path: Chemin du fichier Excel de config
            
        Returns:
            dict: Résultats du traitement
        """
        
        print("=" * 80)
        print("🔄 MODULE 2 : AUTO LOADER (AUTO-DISCOVERY)")
        print("=" * 80)
        
        # Lire configuration Excel
        import pandas as pd
        
        print(f"\n📖 Lecture configuration : {excel_config_path}")
        
        try:
            file_tables_df = pd.read_excel(excel_config_path, sheet_name="File-Table")
            file_columns_df = pd.read_excel(excel_config_path, sheet_name="Field-Column")
            print(f"✅ Configuration chargée\n")
        except Exception as e:
            print(f"❌ Erreur lecture Excel : {e}")
            import traceback
            traceback.print_exc()
            return {"status": "ERROR", "error": str(e)}
        
        # ========== ✨ NOUVEAU : DÉCOUVERTE AUTOMATIQUE DES DOSSIERS ==========
        
        print(f"🔍 Scan du répertoire : {self.source_base}")
        
        # Lister tous les dossiers dans extracted/
        try:
            if not os.path.exists(self.source_base):
                print(f"⚠️  Répertoire extracted/ n'existe pas : {self.source_base}")
                return {"status": "NO_DATA", "message": "No extracted directory"}
            
            all_items = os.listdir(self.source_base)
            
            # Filtrer seulement les dossiers (pas les fichiers)
            discovered_folders = [
                item for item in all_items 
                if os.path.isdir(os.path.join(self.source_base, item))
                and not item.startswith('.')  # Ignorer dossiers cachés
            ]
            
            if not discovered_folders:
                print(f"⚠️  Aucun dossier trouvé dans extracted/")
                return {"status": "NO_DATA", "message": "No folders in extracted/"}
            
            print(f"✅ {len(discovered_folders)} dossier(s) découvert(s) :")
            for folder in discovered_folders:
                print(f"   • {folder}")
        
        except Exception as e:
            print(f"❌ Erreur scan dossiers : {e}")
            return {"status": "ERROR", "error": str(e)}
        
        # ========== TRAITER CHAQUE DOSSIER DÉCOUVERT ==========
        
        results = []
        success_count = 0
        failed_count = 0
        total_rows = 0
        
        for idx, folder_name in enumerate(discovered_folders, 1):
            
            print(f"\n{'=' * 80}")
            print(f"📁 Dossier {idx}/{len(discovered_folders)}: {folder_name}")
            print(f"{'=' * 80}")
            
            # Chercher la configuration correspondante dans l'Excel
            table_config = self._find_table_config(folder_name, file_tables_df)
            
            if table_config is None:
                print(f"⚠️  Aucune configuration trouvée dans Excel pour '{folder_name}'")
                print(f"   → Utilisation de la configuration par défaut")
                
                # Créer config par défaut
                table_config = {
                    "Delta Table Name": folder_name,
                    "Input Format": "csv",
                    "Input delimiter": ",",
                    "Input charset": "UTF-8"
                }
            else:
                print(f"✅ Configuration trouvée : table '{table_config['Delta Table Name']}'")
            
            # Vérifier qu'il y a des fichiers dans le dossier
            source_dir = os.path.join(self.source_base, folder_name)
            
            try:
                files = [f for f in os.listdir(source_dir) 
                        if not f.startswith('.') and os.path.isfile(os.path.join(source_dir, f))]
            except Exception as e:
                print(f"❌ Erreur listage fichiers : {e}")
                failed_count += 1
                results.append({
                    "folder": folder_name,
                    "status": "ERROR",
                    "error": str(e)
                })
                continue
            
            if not files:
                print(f"⚠️  Aucun fichier dans {folder_name}/")
                results.append({
                    "folder": folder_name,
                    "status": "NO_DATA",
                    "rows": 0
                })
                continue
            
            print(f"📁 {len(files)} fichier(s) trouvé(s) : {', '.join(files[:3])}")
            if len(files) > 3:
                print(f"   ... et {len(files) - 3} autre(s)")
            
            # Colonnes de cette table (si config existe)
            if isinstance(table_config, dict):
                table_name = table_config["Delta Table Name"]
                table_columns = file_columns_df[
                    file_columns_df["Delta Table Name"] == table_name
                ]
            else:
                table_name = table_config["Delta Table Name"]
                table_columns = file_columns_df[
                    file_columns_df["Delta Table Name"] == table_name
                ]
            
            # Traiter avec Auto Loader
            try:
                result = self._process_single_folder(
                    folder_name, 
                    table_config, 
                    table_columns
                )
                results.append(result)
                
                if result["status"] == "SUCCESS":
                    print(f"✅ {result.get('rows_ingested', 0):,} ligne(s) ingérée(s)")
                    success_count += 1
                    total_rows += result.get('rows_ingested', 0)
                elif result["status"] == "NO_DATA":
                    print(f"⚠️  Aucune nouvelle donnée")
                else:
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ Erreur : {e}")
                import traceback
                traceback.print_exc()
                
                failed_count += 1
                results.append({
                    "folder": folder_name,
                    "status": "ERROR",
                    "error": str(e)
                })
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ AUTO LOADER")
        print("=" * 80)
        print(f"✅ Dossiers traités : {success_count}")
        print(f"❌ Dossiers en échec : {failed_count}")
        print(f"📈 Total lignes      : {total_rows:,}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "success_count": success_count,
            "failed_count": failed_count,
            "total_rows": total_rows,
            "results": results
        }
    
    def _find_table_config(self, folder_name: str, file_tables_df) -> dict:
        """
        Cherche la configuration correspondante dans l'Excel
        
        Args:
            folder_name: Nom du dossier découvert
            file_tables_df: DataFrame Excel File-Table
            
        Returns:
            Configuration trouvée ou None
        """
        
        # Stratégie de matching :
        # 1. Match exact sur "Delta Table Name"
        # 2. Match partiel (folder_name contient table_name ou inversement)
        # 3. Aucun match → None
        
        for idx, row in file_tables_df.iterrows():
            table_name = str(row["Delta Table Name"]).strip().lower()
            folder_lower = folder_name.lower()
            
            # Match exact
            if table_name == folder_lower:
                print(f"   🎯 Match exact : '{folder_name}' = '{row['Delta Table Name']}'")
                return row
            
            # Match partiel (folder commence par table)
            if folder_lower.startswith(table_name):
                print(f"   🎯 Match partiel : '{folder_name}' contient '{row['Delta Table Name']}'")
                return row
            
            # Match partiel inverse (table commence par folder)
            if table_name.startswith(folder_lower):
                print(f"   🎯 Match partiel : '{row['Delta Table Name']}' contient '{folder_name}'")
                return row
        
        # Aucun match
        return None
    
    def _process_single_folder(self, folder_name: str, table_config, columns_config) -> dict:
        """
        Traite un dossier avec Auto Loader
        
        Args:
            folder_name: Nom du dossier dans extracted/
            table_config: Configuration de la table
            columns_config: Définitions des colonnes
            
        Returns:
            dict: Résultat du traitement
        """
        
        # Déterminer le nom de la table
        if isinstance(table_config, dict):
            table_name = table_config["Delta Table Name"]
        else:
            table_name = table_config["Delta Table Name"]
        
        # Chemins (Unity Catalog)
        source_path = f"{self.source_base}/{folder_name}"  # ← Utilise nom du dossier
        checkpoint_path = f"{self.checkpoint_base}/{table_name}"
        schema_path = f"{self.schema_base}/{table_name}"
        target_table = f"{self.config.catalog}.{self.config.schema_tables}.{table_name}_staging"
        
        print(f"\n📂 Source      : {source_path}")
        print(f"📂 Checkpoint  : {checkpoint_path}")
        print(f"🗄️  Target      : {target_table}")
        
        # Configuration lecture
        if isinstance(table_config, dict):
            input_format = str(table_config.get("Input Format", "csv")).strip().lower()
            delimiter = str(table_config.get("Input delimiter", ","))
            charset = str(table_config.get("Input charset", "UTF-8")).strip()
        else:
            input_format = str(table_config.get("Input Format", "csv")).strip().lower()
            delimiter = str(table_config.get("Input delimiter", ","))
            charset = str(table_config.get("Input charset", "UTF-8")).strip()
        
        if charset.lower() in ["nan", "", "none"]:
            charset = "UTF-8"
        
        # Options Auto Loader
        options = {
            "cloudFiles.format": input_format,
            "cloudFiles.useNotifications": "false",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.schemaLocation": schema_path,
        }
        
        # Options CSV
        if input_format in ["csv", "csv_quote", "csv_quote_ml"]:
            options.update({
                "header": "true",
                "delimiter": delimiter,
                "encoding": charset,
                "inferSchema": "false",
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
                "quote": '"',
                "escape": "\\"
            })
            
            if input_format == "csv_quote_ml":
                options["multiline"] = "true"
        
        # Créer stream
        print(f"\n🔄 Création stream Auto Loader...")
        
        try:
            df_stream = (
                self.spark.readStream
                .format("cloudFiles")
                .options(**options)
                .load(source_path)
            )
        except Exception as e:
            return {
                "status": "ERROR",
                "error": f"Stream creation failed: {e}"
            }
        
        # Ajouter métadonnées
        df_stream = self._add_metadata(df_stream)
        
        # Écrire dans table staging
        print(f"💾 Écriture vers {target_table}...")
        
        try:
            query = (
                df_stream.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(once=True)
                .toTable(target_table)
            )
            
            print(f"⏳ Traitement en cours...")
            query.awaitTermination()
            
            # Statistiques
            progress = query.lastProgress
            
            if progress:
                rows_ingested = progress.get("numInputRows", 0)
                
                if rows_ingested > 0:
                    return {
                        "status": "SUCCESS",
                        "rows_ingested": rows_ingested,
                        "target_table": target_table,
                        "source_folder": folder_name
                    }
                else:
                    return {
                        "status": "NO_DATA",
                        "rows_ingested": 0,
                        "target_table": target_table,
                        "source_folder": folder_name
                    }
            else:
                return {
                    "status": "NO_DATA",
                    "rows_ingested": 0,
                    "target_table": target_table,
                    "source_folder": folder_name
                }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "status": "ERROR",
                "error": f"Write failed: {e}"
            }
    
    def _add_metadata(self, df_stream):
        """Ajoute métadonnées au stream"""
        
        # Nom fichier
        df_stream = df_stream.withColumn(
            "FILE_NAME_RECEIVED",
            F.element_at(F.split(F.input_file_name(), "/"), -1)
        )
        
        # Date depuis nom fichier
        df_stream = df_stream.withColumn(
            "yyyy",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_(\d{4})\d{4}", 1).cast("int")
        )
        
        df_stream = df_stream.withColumn(
            "mm",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_\d{4}(\d{2})\d{2}", 1).cast("int")
        )
        
        df_stream = df_stream.withColumn(
            "dd",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_\d{6}(\d{2})", 1).cast("int")
        )
        
        # Timestamp ingestion
        df_stream = df_stream.withColumn(
            "INGESTION_TIMESTAMP",
            F.current_timestamp()
        )
        
        return df_stream
    
    def list_staging_tables(self) -> list:
        """Liste les tables staging créées"""
        
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.config.catalog}.{self.config.schema_tables}"
            ).collect()
            
            staging_tables = [t.tableName for t in tables if "_staging" in t.tableName]
            
            return staging_tables
            
        except Exception as e:
            print(f"⚠️  Erreur listage tables staging : {e}")
            return []
    
    def get_staging_stats(self) -> dict:
        """Récupère les statistiques des tables staging"""
        
        staging_tables = self.list_staging_tables()
        
        stats = {}
        
        for table_name in staging_tables:
            table_full = f"{self.config.catalog}.{self.config.schema_tables}.{table_name}"
            
            try:
                df = self.spark.table(table_full)
                count = df.count()
                
                # Fichiers sources
                sources = []
                if "FILE_NAME_RECEIVED" in df.columns:
                    sources = [row.FILE_NAME_RECEIVED 
                             for row in df.select("FILE_NAME_RECEIVED").distinct().collect()]
                
                stats[table_name] = {
                    "rows": count,
                    "sources": sources
                }
                
            except Exception as e:
                stats[table_name] = {
                    "error": str(e)
                }
        
        return stats


def main():
    """Point d'entrée du module Auto Loader"""
    
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    print("🚀 Démarrage Module 2 : Auto Loader (Auto-Discovery)")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-Module2-AutoLoader").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1"
    )
    
    # Chemin Excel
    excel_path = f"{config.volume_base}/input/config/wax_config.xlsx"
    
    # Auto Loader
    autoloader = AutoLoaderModule(spark, config)
    result = autoloader.process_all_tables(excel_path)
    
    # Afficher tables staging créées
    if result["status"] in ["SUCCESS", "PARTIAL"]:
        print("\n📋 Tables staging créées :")
        stats = autoloader.get_staging_stats()
        
        for table_name, table_stats in stats.items():
            if "error" not in table_stats:
                print(f"   • {table_name}: {table_stats['rows']:,} lignes")
                if table_stats.get('sources'):
                    print(f"     Sources: {', '.join(table_stats['sources'][:3])}")
    
    # Retourner code de sortie
    if result["status"] == "SUCCESS":
        print("\n✅ Module 2 terminé avec succès")
        return 0
    elif result["status"] == "PARTIAL":
        print("\n⚠️  Module 2 terminé avec des erreurs partielles")
        return 1
    else:
        print(f"\n❌ Module 2 terminé avec erreurs")
        return 2


if __name__ == "__main__":
    import sys
    sys.exit(main())
