"""
simple_report_manager.py
Rapport simplifié avec informations essentielles uniquement
"""

from datetime import datetime
from pyspark.sql import functions as F


class SimpleReportManager:
    """Gestionnaire de rapport simplifié et lisible"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.execution_start = datetime.now()
    
    def generate_simple_report(self, total_files_processed: int, total_failed: int,
                               execution_time: float):
        """
        Génère un rapport simple et clair
        
        Args:
            total_files_processed: Nombre de fichiers réussis
            total_failed: Nombre de fichiers échoués
            execution_time: Durée totale en secondes
        """
        
        print("\n" + "=" * 100)
        print("📊 RAPPORT D'EXÉCUTION WAX PIPELINE")
        print("=" * 100)
        
        # ========== 1. RÉSUMÉ EXÉCUTION ==========
        self._print_execution_summary(total_files_processed, total_failed, execution_time)
        
        # ========== 2. FICHIERS RÉUSSIS ==========
        self._print_successful_files()
        
        # ========== 3. FICHIERS REJETÉS ==========
        self._print_rejected_files()
        
        # ========== 4. TABLES CRÉÉES ==========
        self._print_created_tables()
        
        # ========== 5. ALERTES ==========
        self._print_alerts()
        
        print("\n" + "=" * 100)
        print("✅ Rapport terminé")
        print("=" * 100 + "\n")
    
    def _print_execution_summary(self, total_success: int, total_failed: int, 
                                 execution_time: float):
        """Résumé de l'exécution"""
        
        total_files = total_success + total_failed
        success_rate = (total_success / total_files * 100) if total_files > 0 else 0
        
        # Icône selon taux de succès
        if success_rate == 100:
            status_icon = "✅"
            status_text = "SUCCÈS COMPLET"
        elif success_rate >= 75:
            status_icon = "⚠️"
            status_text = "SUCCÈS PARTIEL"
        else:
            status_icon = "❌"
            status_text = "ÉCHEC PARTIEL"
        
        print(f"""
{status_icon} STATUT : {status_text}

📅 Date      : {self.execution_start.strftime('%Y-%m-%d %H:%M:%S')}
⏱️  Durée     : {execution_time:.1f}s ({execution_time/60:.1f} min)
🌍 Environnement : {self.config.env.upper()}

📊 RÉSULTAT :
   ✅ Fichiers réussis  : {total_success}
   ❌ Fichiers rejetés  : {total_failed}
   📈 Taux de succès    : {success_rate:.0f}%
        """)
    
    def _print_successful_files(self):
        """Liste des fichiers traités avec succès"""
        
        print("\n" + "-" * 100)
        print("✅ FICHIERS TRAITÉS AVEC SUCCÈS")
        print("-" * 100)
        
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            # Vérifier si la table existe
            if not self._table_exists(exec_table):
                print("   ℹ️  Table execution_logs non disponible (première exécution ?)")
                return
            
            # Fichiers réussis du jour
            successful_df = self.spark.table(exec_table).filter(
                (F.to_date(F.col("log_ts")) == F.current_date()) &
                (F.col("status") == "SUCCESS")
            )
            
            count = successful_df.count()
            
            if count == 0:
                print("   ℹ️  Aucun fichier traité avec succès aujourd'hui")
                return
            
            # Afficher les fichiers
            files = successful_df.select(
                F.col("table_name").alias("Table"),
                F.col("filename").alias("Fichier"),
                F.col("row_count").alias("Lignes"),
                F.round(F.col("duration"), 1).alias("Durée (s)"),
                F.date_format(F.col("log_ts"), "HH:mm:ss").alias("Heure")
            ).orderBy("log_ts").collect()
            
            print(f"\n   📁 {count} fichier(s) traité(s) avec succès:\n")
            
            for idx, file in enumerate(files, 1):
                print(f"   {idx}. {file.Fichier}")
                print(f"      • Table      : {file.Table}")
                print(f"      • Lignes     : {file.Lignes:,}")
                print(f"      • Durée      : {file['Durée (s)']}s")
                print(f"      • Heure      : {file.Heure}")
                print()
            
            # Total lignes traitées
            total_rows = sum(f.Lignes for f in files)
            print(f"   📊 Total : {total_rows:,} lignes traitées avec succès")
            
        except Exception as e:
            print(f"   ⚠️  Erreur lecture fichiers réussis : {e}")
    
    def _print_rejected_files(self):
        """Liste des fichiers rejetés avec raisons (tous types de rejets)"""
        
        print("\n" + "-" * 100)
        print("❌ FICHIERS REJETÉS")
        print("-" * 100)
        
        rejected_files = []
        
        # ========== 1. REJETS DANS EXECUTION_LOGS (échecs de traitement) ==========
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            # Vérifier si la table existe
            if self._table_exists(exec_table):
                failed_df = self.spark.table(exec_table).filter(
                    (F.to_date(F.col("log_ts")) == F.current_date()) &
                    (F.col("status") == "FAILED")
                )
                
                if failed_df.count() > 0:
                    for row in failed_df.collect():
                        rejected_files.append({
                            "filename": row.filename,
                            "table": row.table_name,
                            "reason": row.error_message if row.error_message else "Unknown error",
                            "time": row.log_ts.strftime("%H:%M:%S") if row.log_ts else "N/A"
                        })
            else:
                print("   ℹ️  Table execution_logs non disponible (première exécution ?)")
        except Exception as e:
            pass  # Ignorer silencieusement les erreurs
        
        # ========== 2. REJETS DANS QUALITY_ERRORS (rejets de validation) ==========
        try:
            quality_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_data_quality_errors"
            
            # Vérifier si la table existe
            if self._table_exists(quality_table):
                # Chercher les rejets de fichiers (colonne "filename")
                filename_errors = self.spark.table(quality_table).filter(
                    (F.to_date(F.col("log_ts")) == F.current_date()) &
                    (F.col("column_name") == "filename")
                )
                
                if filename_errors.count() > 0:
                    for row in filename_errors.collect():
                        # Vérifier si pas déjà dans la liste
                        if not any(f["filename"] == row.filename for f in rejected_files):
                            # Construire raison depuis error_message
                            reason = row.error_message if row.error_message else "Validation failed"
                            
                            rejected_files.append({
                                "filename": row.filename,
                                "table": row.table_name,
                                "reason": reason,
                                "time": row.log_ts.strftime("%H:%M:%S") if row.log_ts else "N/A"
                            })
            else:
                print("   ℹ️  Table quality_errors non disponible (première exécution ?)")
        except Exception as e:
            pass  # Ignorer silencieusement les erreurs
        
        # ========== AFFICHAGE ==========
        
        if not rejected_files:
            print("   ✅ Aucun fichier rejeté")
            return
        
        print(f"\n   🚫 {len(rejected_files)} fichier(s) rejeté(s):\n")
        
        for idx, file_info in enumerate(rejected_files, 1):
            print(f"   {idx}. {file_info['filename']}")
            print(f"      • Table      : {file_info['table']}")
            print(f"      • Raison     : {file_info['reason']}")
            print(f"      • Heure      : {file_info['time']}")
            print()
    
    def _print_created_tables(self):
        """Tables créées avec leurs statistiques"""
        
        print("\n" + "-" * 100)
        print("🗄️  TABLES CRÉÉES")
        print("-" * 100)
        
        try:
            # Lister tables WAX
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.config.catalog}.{self.config.schema_tables}"
            ).collect()
            
            wax_tables = [t for t in tables if "_all" in t.tableName or "_last" in t.tableName]
            
            if not wax_tables:
                print("   ℹ️  Aucune table créée")
                return
            
            # Grouper par base (table_all et table_last ensemble)
            table_groups = {}
            for table in wax_tables:
                base_name = table.tableName.replace("_all", "").replace("_last", "")
                if base_name not in table_groups:
                    table_groups[base_name] = []
                table_groups[base_name].append(table.tableName)
            
            print(f"\n   📊 {len(table_groups)} table(s) créée(s):\n")
            
            for idx, (base_name, table_list) in enumerate(table_groups.items(), 1):
                print(f"   {idx}. {base_name.upper()}")
                
                for table_name in sorted(table_list):
                    table_full = f"{self.config.catalog}.{self.config.schema_tables}.{table_name}"
                    
                    try:
                        df = self.spark.table(table_full)
                        count = df.count()
                        
                        # Récupérer nombre de fichiers sources
                        sources_count = 0
                        if "FILE_NAME_RECEIVED" in df.columns:
                            sources_count = df.select("FILE_NAME_RECEIVED").distinct().count()
                        
                        # Type de table
                        if "_all" in table_name:
                            table_type = "Historique"
                        else:
                            table_type = "Courante"
                        
                        print(f"      • {table_name}")
                        print(f"        - Type        : {table_type}")
                        print(f"        - Lignes      : {count:,}")
                        if sources_count > 0:
                            print(f"        - Fichiers    : {sources_count}")
                    
                    except Exception as e:
                        print(f"      • {table_name} : Erreur lecture")
                
                print()
            
        except Exception as e:
            print(f"   ⚠️  Erreur lecture tables : {e}")
    
    def _print_alerts(self):
        """Alertes et points d'attention"""
        
        print("\n" + "-" * 100)
        print("⚠️  ALERTES ET POINTS D'ATTENTION")
        print("-" * 100)
        
        alerts = []
        
        # Vérifier erreurs qualité
        try:
            quality_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_data_quality_errors"
            
            if self._table_exists(quality_table):
                errors_df = self.spark.table(quality_table).filter(
                    F.to_date(F.col("log_ts")) == F.current_date()
                )
                
                error_count = errors_df.count()
                
                if error_count > 0:
                    # Top 3 erreurs
                    top_errors = errors_df.groupBy("error_message").agg(
                        F.sum(F.col("error_count").cast("bigint")).alias("total")
                    ).orderBy(F.desc("total")).limit(3).collect()
                    
                    alerts.append({
                        "type": "QUALITÉ",
                        "severity": "warning" if error_count < 100 else "critical",
                        "message": f"{error_count} erreur(s) de qualité détectée(s)",
                        "details": [f"{row.error_message}: {row.total}" for row in top_errors]
                    })
        except:
            pass
        
        # Vérifier performance
        try:
            exec_table = f"{self.config.catalog}.{self.config.schema_tables}.wax_execution_logs"
            
            if self._table_exists(exec_table):
                slow_files = self.spark.table(exec_table).filter(
                    (F.to_date(F.col("log_ts")) == F.current_date()) &
                    (F.col("status") == "SUCCESS") &
                    (F.col("duration") > 60)  # Plus de 60 secondes
                )
                
                slow_count = slow_files.count()
                
                if slow_count > 0:
                    alerts.append({
                        "type": "PERFORMANCE",
                        "severity": "info",
                        "message": f"{slow_count} fichier(s) lent(s) (>60s)",
                        "details": []
                    })
        except:
            pass
        
        # Afficher alertes
        if not alerts:
            print("\n   ✅ Aucune alerte - Exécution parfaite !")
            return
        
        print(f"\n   📋 {len(alerts)} alerte(s) détectée(s):\n")
        
        for idx, alert in enumerate(alerts, 1):
            # Icône selon sévérité
            if alert["severity"] == "critical":
                icon = "🔴"
            elif alert["severity"] == "warning":
                icon = "🟠"
            else:
                icon = "🔵"
            
            print(f"   {idx}. {icon} [{alert['type']}] {alert['message']}")
            
            for detail in alert["details"]:
                print(f"      • {detail}")
            print()
    
    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe"""
        try:
            self.spark.table(table_name)
            return True
        except:
            return False
