"""
report_manager.py
Génération de rapports complets d'exécution du pipeline
"""

from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from typing import Dict, List
import json


class ReportManager:
    """Gestionnaire de rapports d'exécution"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.execution_start = datetime.now()
        
    def generate_full_report(self, total_files_processed: int, total_failed: int,
                            total_tables: int, execution_time: float):
        """
        Génère un rapport complet de l'exécution
        
        Args:
            total_files_processed: Nombre de fichiers traités avec succès
            total_failed: Nombre d'échecs
            total_tables: Nombre de tables configurées
            execution_time: Durée totale d'exécution
        """
        
        print("\n" + "=" * 100)
        print("📊 RAPPORT COMPLET D'EXÉCUTION - WAX PIPELINE")
        print("=" * 100)
        
        # ========== SECTION 1 : RÉSUMÉ GÉNÉRAL ==========
        self._print_summary_section(total_files_processed, total_failed, 
                                    total_tables, execution_time)
        
        # ========== SECTION 2 : DÉTAILS PAR FICHIER ==========
        self._print_file_details_section()
        
        # ========== SECTION 3 : DÉTAILS PAR TABLE ==========
        self._print_table_details_section()
        
        # ========== SECTION 4 : QUALITÉ DES DONNÉES ==========
        self._print_quality_section()
        
        # ========== SECTION 5 : PERFORMANCE ==========
        self._print_performance_section()
        
        # ========== SECTION 6 : ERREURS ET ALERTES ==========
        self._print_errors_section()
        
        # ========== SECTION 7 : RECOMMANDATIONS ==========
        self._print_recommendations_section(total_files_processed, total_failed)
        
        print("\n" + "=" * 100)
        print("✅ Rapport terminé")
        print("=" * 100)
    
    def _print_summary_section(self, total_files_processed: int, total_failed: int,
                               total_tables: int, execution_time: float):
        """Résumé général de l'exécution"""
        
        print("\n" + "=" * 100)
        print("📋 1. RÉSUMÉ GÉNÉRAL")
        print("=" * 100)
        
        print(f"""
🕒 Date d'exécution    : {self.execution_start.strftime('%Y-%m-%d %H:%M:%S')}
⏱️  Durée totale        : {execution_time:.2f} secondes ({execution_time/60:.2f} minutes)
🌍 Environnement       : {self.config.env}
📚 Catalogue           : {self.config.catalog}
🗄️  Schéma tables       : {self.config.schema_tables}

📊 STATISTIQUES GLOBALES:
   ✅ Fichiers traités avec succès : {total_files_processed}
   ❌ Fichiers en échec            : {total_failed}
   📋 Tables configurées           : {total_tables}
   📈 Taux de succès               : {(total_files_processed/(total_files_processed+total_failed)*100 if (total_files_processed+total_failed)>0 else 0):.1f}%
        """)
    
    def _print_file_details_section(self):
        """Détails par fichier traité"""
        
        print("\n" + "=" * 100)
        print("📄 2. DÉTAILS PAR FICHIER")
        print("=" * 100)
        
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            # Vérifier si table existe
            if not self._table_exists(exec_table):
                print("   ℹ️  Aucune donnée d'exécution disponible")
                return
            
            # Récupérer logs d'exécution du jour
            logs_df = self.spark.table(exec_table).filter(
                F.to_date(F.col("log_ts")) == F.current_date()
            )
            
            if logs_df.count() == 0:
                print("   ℹ️  Aucune exécution aujourd'hui")
                return
            
            # Afficher détails
            logs_df.select(
                F.col("table_name").alias("Table"),
                F.col("filename").alias("Fichier"),
                F.col("row_count").alias("Lignes"),
                F.col("column_count").alias("Colonnes"),
                F.col("error_count").alias("Erreurs"),
                F.col("status").alias("Statut"),
                F.round(F.col("duration"), 2).alias("Durée (s)"),
                F.date_format(F.col("log_ts"), "HH:mm:ss").alias("Heure")
            ).orderBy("log_ts").show(100, truncate=False)
            
            # Statistiques agrégées
            stats = logs_df.agg(
                F.sum("row_count").alias("total_rows"),
                F.sum("error_count").alias("total_errors"),
                F.avg("duration").alias("avg_duration"),
                F.max("duration").alias("max_duration")
            ).collect()[0]
            
            print(f"""
📊 Statistiques des fichiers:
   📈 Total lignes traitées : {stats['total_rows']:,}
   ⚠️  Total erreurs         : {stats['total_errors']:,}
   ⏱️  Durée moyenne         : {stats['avg_duration']:.2f}s
   ⏱️  Durée max             : {stats['max_duration']:.2f}s
            """)
            
        except Exception as e:
            print(f"   ⚠️  Erreur lecture logs : {e}")
    
    def _print_table_details_section(self):
        """Détails par table créée"""
        
        print("\n" + "=" * 100)
        print("🗄️  3. DÉTAILS PAR TABLE")
        print("=" * 100)
        
        try:
            # Lister toutes les tables WAX
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.config.catalog}.{self.config.schema_tables}"
            ).collect()
            
            wax_tables = [t for t in tables if "_all" in t.tableName or "_last" in t.tableName]
            
            if not wax_tables:
                print("   ℹ️  Aucune table WAX créée")
                return
            
            print("\n📋 Tables créées:")
            print("-" * 100)
            
            table_stats = []
            
            for table in wax_tables:
                table_full = f"{self.config.catalog}.{self.config.schema_tables}.{table.tableName}"
                
                try:
                    df = self.spark.table(table_full)
                    
                    # Statistiques de base
                    count = df.count()
                    cols = len(df.columns)
                    
                    # Partitions
                    partitions = []
                    if "yyyy" in df.columns:
                        parts = df.select("yyyy", "mm", "dd").distinct().collect()
                        partitions = [f"{p.yyyy}-{p.mm:02d}-{p.dd:02d}" for p in parts]
                    
                    # Fichiers sources
                    sources = []
                    if "FILE_NAME_RECEIVED" in df.columns:
                        sources = [row.FILE_NAME_RECEIVED for row in 
                                  df.select("FILE_NAME_RECEIVED").distinct().collect()]
                    
                    table_stats.append({
                        "name": table.tableName,
                        "rows": count,
                        "columns": cols,
                        "partitions": len(partitions),
                        "sources": len(sources),
                        "sources_list": sources[:5]  # Max 5 premiers
                    })
                    
                    print(f"\n📊 {table.tableName}")
                    print(f"   Lignes          : {count:,}")
                    print(f"   Colonnes        : {cols}")
                    print(f"   Partitions      : {len(partitions)}")
                    if partitions:
                        print(f"   Dates           : {', '.join(partitions[:5])}")
                    if sources:
                        print(f"   Fichiers sources: {len(sources)}")
                        for src in sources[:5]:
                            print(f"      - {src}")
                    
                except Exception as e:
                    print(f"   ⚠️  Erreur lecture table {table.tableName}: {e}")
            
            # Résumé
            if table_stats:
                total_rows = sum(t["rows"] for t in table_stats)
                total_sources = sum(t["sources"] for t in table_stats)
                
                print(f"""
📊 Résumé tables:
   🗄️  Nombre de tables       : {len(table_stats)}
   📈 Total lignes           : {total_rows:,}
   📁 Total fichiers sources : {total_sources}
                """)
        
        except Exception as e:
            print(f"   ⚠️  Erreur lecture tables : {e}")
    
    def _print_quality_section(self):
        """Section qualité des données"""
        
        print("\n" + "=" * 100)
        print("✅ 4. QUALITÉ DES DONNÉES")
        print("=" * 100)
        
        try:
            quality_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_data_quality_errors"
            
            if not self._table_exists(quality_table):
                print("   ℹ️  Aucune donnée de qualité disponible")
                return
            
            errors_df = self.spark.table(quality_table).filter(
                F.to_date(F.col("log_ts")) == F.current_date()
            )
            
            if errors_df.count() == 0:
                print("   ✅ Aucune erreur de qualité détectée aujourd'hui")
                return
            
            # Top 10 erreurs
            print("\n⚠️  Top 10 erreurs par type:")
            errors_df.groupBy("error_message").agg(
                F.sum(F.col("error_count").cast("bigint")).alias("total_errors"),
                F.countDistinct("table_name").alias("tables_affected")
            ).orderBy(F.desc("total_errors")).limit(10).show(truncate=False)
            
            # Erreurs par table
            print("\n📊 Erreurs par table:")
            errors_df.groupBy("table_name").agg(
                F.sum(F.col("error_count").cast("bigint")).alias("total_errors"),
                F.countDistinct("column_name").alias("columns_affected")
            ).orderBy(F.desc("total_errors")).show(truncate=False)
            
            # Statistiques générales
            total_errors = errors_df.agg(
                F.sum(F.col("error_count").cast("bigint"))
            ).collect()[0][0] or 0
            
            tables_affected = errors_df.select("table_name").distinct().count()
            
            print(f"""
📊 Statistiques qualité:
   ⚠️  Total erreurs        : {total_errors:,}
   🗄️  Tables affectées     : {tables_affected}
            """)
            
        except Exception as e:
            print(f"   ⚠️  Erreur lecture qualité : {e}")
    
    def _print_performance_section(self):
        """Section performance"""
        
        print("\n" + "=" * 100)
        print("⚡ 5. PERFORMANCE")
        print("=" * 100)
        
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            if not self._table_exists(exec_table):
                print("   ℹ️  Pas de données de performance")
                return
            
            logs_df = self.spark.table(exec_table).filter(
                F.to_date(F.col("log_ts")) == F.current_date()
            )
            
            if logs_df.count() == 0:
                return
            
            # Performance par table
            print("\n📊 Performance par table:")
            logs_df.groupBy("table_name").agg(
                F.count("*").alias("executions"),
                F.sum("row_count").alias("total_rows"),
                F.avg("duration").alias("avg_duration"),
                F.min("duration").alias("min_duration"),
                F.max("duration").alias("max_duration"),
                (F.sum("row_count") / F.sum("duration")).alias("rows_per_sec")
            ).select(
                "table_name",
                "executions",
                "total_rows",
                F.round("avg_duration", 2).alias("avg_dur"),
                F.round("min_duration", 2).alias("min_dur"),
                F.round("max_duration", 2).alias("max_dur"),
                F.round("rows_per_sec", 0).alias("lignes/s")
            ).show(truncate=False)
            
            # Fichiers les plus lents
            print("\n🐢 Top 5 fichiers les plus lents:")
            logs_df.select(
                "table_name",
                "filename",
                "row_count",
                F.round("duration", 2).alias("duration"),
                F.round(F.col("row_count") / F.col("duration"), 0).alias("rows_per_sec")
            ).orderBy(F.desc("duration")).limit(5).show(truncate=False)
            
        except Exception as e:
            print(f"   ⚠️  Erreur analyse performance : {e}")
    
    def _print_errors_section(self):
        """Section erreurs et alertes"""
        
        print("\n" + "=" * 100)
        print("🚨 6. ERREURS ET ALERTES")
        print("=" * 100)
        
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            if not self._table_exists(exec_table):
                print("   ℹ️  Pas de données d'erreurs")
                return
            
            # Échecs du jour
            failed_df = self.spark.table(exec_table).filter(
                (F.to_date(F.col("log_ts")) == F.current_date()) &
                (F.col("status") == "FAILED")
            )
            
            failed_count = failed_df.count()
            
            if failed_count == 0:
                print("   ✅ Aucun échec aujourd'hui")
            else:
                print(f"\n❌ {failed_count} fichier(s) en échec:\n")
                failed_df.select(
                    "table_name",
                    "filename",
                    "error_message",
                    F.date_format("log_ts", "HH:mm:ss").alias("time")
                ).show(truncate=False)
            
            # Alertes qualité
            quality_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_data_quality_errors"
            
            if self._table_exists(quality_table):
                critical_errors = self.spark.table(quality_table).filter(
                    (F.to_date(F.col("log_ts")) == F.current_date()) &
                    (F.col("error_message").contains("ABORT") | 
                     F.col("error_message").contains("REJECT"))
                )
                
                crit_count = critical_errors.count()
                
                if crit_count > 0:
                    print(f"\n⚠️  {crit_count} erreur(s) critique(s):\n")
                    critical_errors.select(
                        "table_name",
                        "filename",
                        "error_message"
                    ).distinct().show(truncate=False)
            
        except Exception as e:
            print(f"   ⚠️  Erreur analyse erreurs : {e}")
    
    def _print_recommendations_section(self, total_files: int, total_failed: int):
        """Recommandations basées sur l'exécution"""
        
        print("\n" + "=" * 100)
        print("💡 7. RECOMMANDATIONS")
        print("=" * 100)
        
        recommendations = []
        
        # Vérifier taux d'échec
        if total_failed > 0 and total_files > 0:
            failure_rate = (total_failed / (total_files + total_failed)) * 100
            if failure_rate > 10:
                recommendations.append(
                    f"⚠️  Taux d'échec élevé ({failure_rate:.1f}%) - Vérifier les fichiers sources"
                )
        
        # Vérifier tables créées
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.config.catalog}.{self.config.schema_tables}"
            ).collect()
            
            wax_tables = [t for t in tables if "_all" in t.tableName or "_last" in t.tableName]
            
            if len(wax_tables) > 10:
                recommendations.append(
                    "💾 Nombre élevé de tables - Envisager OPTIMIZE et VACUUM réguliers"
                )
            
            # Vérifier taille des tables
            for table in wax_tables:
                table_full = f"{self.config.catalog}.{self.config.schema_tables}.{table.tableName}"
                try:
                    count = self.spark.table(table_full).count()
                    if count > 10_000_000:
                        recommendations.append(
                            f"📊 Table {table.tableName} volumineuse ({count:,} lignes) - "
                            f"Envisager Z-ORDER sur colonnes fréquemment filtrées"
                        )
                except:
                    pass
        except:
            pass
        
        # Vérifier erreurs qualité
        try:
            quality_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_data_quality_errors"
            if self._table_exists(quality_table):
                error_count = self.spark.table(quality_table).filter(
                    F.to_date(F.col("log_ts")) == F.current_date()
                ).count()
                
                if error_count > 100:
                    recommendations.append(
                        f"⚠️  Nombreuses erreurs qualité ({error_count}) - "
                        f"Vérifier les règles de validation"
                    )
        except:
            pass
        
        # Afficher recommandations
        if recommendations:
            print("\n📋 Actions recommandées:\n")
            for i, rec in enumerate(recommendations, 1):
                print(f"   {i}. {rec}")
        else:
            print("\n   ✅ Aucune action particulière recommandée")
        
        # Suggestions générales
        print("""
💡 Bonnes pratiques:
   1. Exécuter OPTIMIZE sur les tables volumineuses chaque semaine
   2. Exécuter VACUUM pour nettoyer les anciennes versions (retention 7 jours)
   3. Surveiller les logs de qualité quotidiennement
   4. Archiver les fichiers traités
   5. Vérifier les dashboards d'observabilité régulièrement
        """)
    
    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe"""
        try:
            self.spark.table(table_name)
            return True
        except:
            return False
    
    def export_to_json(self, output_path: str):
        """Exporte le rapport en JSON"""
        
        report_data = {
            "execution_date": self.execution_start.isoformat(),
            "environment": self.config.env,
            "catalog": self.config.catalog,
            "schema": self.config.schema_tables
        }
        
        try:
            # Logs d'exécution
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            if self._table_exists(exec_table):
                logs_df = self.spark.table(exec_table).filter(
                    F.to_date(F.col("log_ts")) == F.current_date()
                )
                report_data["executions"] = [row.asDict() for row in logs_df.collect()]
            
            # Sauvegarder JSON
            with open(output_path, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            print(f"✅ Rapport JSON exporté : {output_path}")
            
        except Exception as e:
            print(f"❌ Erreur export JSON : {e}")
    
    def export_to_html(self, output_path: str):
        """Exporte le rapport en HTML"""
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Rapport WAX Pipeline - {self.execution_start.strftime('%Y-%m-%d')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #3498db; color: white; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .success {{ color: #27ae60; font-weight: bold; }}
        .error {{ color: #e74c3c; font-weight: bold; }}
        .stat-box {{ display: inline-block; margin: 10px; padding: 15px; background: #ecf0f1; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Rapport WAX Pipeline</h1>
        <p><strong>Date:</strong> {self.execution_start.strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Environnement:</strong> {self.config.env}</p>
        <p><strong>Catalogue:</strong> {self.config.catalog}.{self.config.schema_tables}</p>
        
        <h2>Statistiques</h2>
        <!-- Ajouter les données du rapport ici -->
        
    </div>
</body>
</html>
        """
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✅ Rapport HTML exporté : {output_path}")
            
        except Exception as e:
            print(f"❌ Erreur export HTML : {e}")
