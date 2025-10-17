"""
autoloader_module.py
MODULE 2 : Auto Loader
Ingestion automatique depuis extracted/ vers tables _staging
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class AutoLoaderModule:
    """Module Auto Loader pour surveillance et ingestion automatique"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog (directs - fonctionne dans Databricks Jobs)
        self.source_base = f"{config.volume_base}/extracted"
        self.checkpoint_base = f"{config.volume_base}/checkpoints"
        self.schema_base = f"{config.volume_base}/schemas"
    
    def process_all_tables(self, excel_config_path: str) -> dict:
        """
        Lance Auto Loader pour toutes les tables configurées
        
        Args:
            excel_config_path: Chemin du fichier Excel de config (Unity Catalog)
            
        Returns:
            dict: Résultats du traitement
        """
        
        print("=" * 80)
        print("🔄 MODULE 2 : AUTO LOADER")
        print("=" * 80)
        
        # Lire configuration Excel
        import pandas as pd
        
        print(f"\n📖 Lecture configuration : {excel_config_path}")
        
        try:
            file_tables_df = pd.read_excel(excel_config_path, sheet_name="File-Table")
            file_columns_df = pd.read_excel(excel_config_path, sheet_name="Field-Column")
            print(f"✅ {len(file_tables_df)} table(s) configurée(s)\n")
        except Exception as e:
            print(f"❌ Erreur lecture Excel : {e}")
            import traceback
            traceback.print_exc()
            return {"status": "ERROR", "error": str(e)}
        
        # Traiter chaque table
        results = []
        success_count = 0
        failed_count = 0
        total_rows = 0
        
        for idx, trow in file_tables_df.iterrows():
            table_name = trow["Delta Table Name"]
            
            print(f"{'=' * 80}")
            print(f"📋 Table {idx + 1}/{len(file_tables_df)}: {table_name}")
            print(f"{'=' * 80}")
            
            # Vérifier si des fichiers existent pour cette table
            source_dir = os.path.join(self.source_base, table_name)
            
            if not os.path.exists(source_dir):
                print(f"⚠️  Répertoire source introuvable : {source_dir}")
                print(f"   Aucun fichier extrait pour cette table")
                results.append({
                    "table": table_name,
                    "status": "NO_DATA",
                    "rows": 0
                })
                continue
            
            try:
                files = [f for f in os.listdir(source_dir) if not f.startswith('.')]
            except Exception as e:
                print(f"❌ Erreur listage fichiers : {e}")
                results.append({
                    "table": table_name,
                    "status": "ERROR",
                    "error": str(e)
                })
                failed_count += 1
                continue
            
            if not files:
                print(f"⚠️  Aucun fichier dans {source_dir}")
                results.append({
                    "table": table_name,
                    "status": "NO_DATA",
                    "rows": 0
                })
                continue
            
            print(f"📁 {len(files)} fichier(s) trouvé(s) : {', '.join(files[:3])}")
            if len(files) > 3:
                print(f"   ... et {len(files) - 3} autre(s)")
            
            # Colonnes de cette table
            table_columns = file_columns_df[
                file_columns_df["Delta Table Name"] == table_name
            ]
            
            # Traiter avec Auto Loader
            try:
                result = self._process_single_table(table_name, trow, table_columns)
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
                    "table": table_name,
                    "status": "ERROR",
                    "error": str(e)
                })
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ AUTO LOADER")
        print("=" * 80)
        print(f"✅ Tables traitées  : {success_count}")
        print(f"❌ Tables en échec  : {failed_count}")
        print(f"📈 Total lignes     : {total_rows:,}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "success_count": success_count,
            "failed_count": failed_count,
            "total_rows": total_rows,
            "results": results
        }
    
    def _process_single_table(self, table_name: str, table_config, columns_config) -> dict:
        """
        Traite une table avec Auto Loader
        
        Args:
            table_name: Nom de la table
            table_config: Configuration de la table (pandas Series)
            columns_config: Définitions des colonnes (pandas DataFrame)
            
        Returns:
            dict: Résultat du traitement
        """
        
        # Chemins (Unity Catalog - pour Spark)
        source_path = f"{self.source_base}/{table_name}"
        checkpoint_path = f"{self.checkpoint_base}/{table_name}"
        schema_path = f"{self.schema_base}/{table_name}"
        target_table = f"{self.config.catalog}.{self.config.schema_tables}.{table_name}_staging"
        
        print(f"\n📂 Source      : {source_path}")
        print(f"📂 Checkpoint  : {checkpoint_path}")
        print(f"📂 Schema      : {schema_path}")
        print(f"🗄️  Target      : {target_table}")
        
        # Configuration lecture
        input_format = str(table_config.get("Input Format", "csv")).strip().lower()
        delimiter = str(table_config.get("Input delimiter", ","))
        charset = str(table_config.get("Input charset", "UTF-8")).strip()
        
        if charset.lower() in ["nan", "", "none"]:
            charset = "UTF-8"
        
        # Options Auto Loader
        options = {
            "cloudFiles.format": input_format,
            "cloudFiles.useNotifications": "false",  # Mode directory listing
            "cloudFiles.includeExistingFiles": "true",  # Traiter fichiers existants
            "cloudFiles.schemaLocation": schema_path,
        }
        
        # Options spécifiques CSV
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
            
            # Multi-line pour csv_quote_ml
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
                .trigger(once=True)  # Traiter une fois (batch-like)
                .toTable(target_table)
            )
            
            # Attendre fin
            print(f"⏳ Traitement en cours...")
            query.awaitTermination()
            
            # Récupérer statistiques
            progress = query.lastProgress
            
            if progress:
                rows_ingested = progress.get("numInputRows", 0)
                
                if rows_ingested > 0:
                    return {
                        "status": "SUCCESS",
                        "rows_ingested": rows_ingested,
                        "target_table": target_table
                    }
                else:
                    return {
                        "status": "NO_DATA",
                        "rows_ingested": 0,
                        "target_table": target_table
                    }
            else:
                return {
                    "status": "NO_DATA",
                    "rows_ingested": 0,
                    "target_table": target_table
                }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "status": "ERROR",
                "error": f"Write failed: {e}"
            }
    
    def _add_metadata(self, df_stream):
        """
        Ajoute métadonnées au stream
        
        Args:
            df_stream: DataFrame streaming
            
        Returns:
            DataFrame avec métadonnées
        """
        
        # Nom fichier
        df_stream = df_stream.withColumn(
            "FILE_NAME_RECEIVED",
            F.element_at(F.split(F.input_file_name(), "/"), -1)
        )
        
        # Date depuis nom fichier (pattern : *_YYYYMMDD* ou *_YYYYMMDD_*)
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
    
    print("🚀 Démarrage Module 2 : Auto Loader")
    
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
    
    # Chemin Excel (Unity Catalog)
    excel_path = f"{config.volume_base}/input/config/wax_configuration.xlsx"
    
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
                if table_stats['sources']:
                    print(f"     Sources: {', '.join(table_stats['sources'][:3])}")
                    if len(table_stats['sources']) > 3:
                        print(f"     ... et {len(table_stats['sources']) - 3} autre(s)")
    
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
