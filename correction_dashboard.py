"""
dashboards.py
Dashboards d'observabilité pour Unity Catalog - Version Corrigée
"""


class DashboardManager:
    """Génère dashboards SQL pour monitoring"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _get_table_name(self, table_suffix: str) -> str:
        """
        Retourne nom complet de table Unity Catalog
        
        Args:
            table_suffix: Suffixe de la table (ex: 'wax_execution_logs')
        
        Returns:
            Nom complet: catalog.schema.table_suffix
        """
        return f"{self.config.catalog}.{self.config.schema_tables}.{table_suffix}"

    def create_execution_logs_table(self):
        """Crée table logs exécution dans Unity Catalog"""
        table_name = self._get_table_name("wax_execution_logs")
        log_path = self.config.log_exec_path

        try:
            # ✅ Vérifier si des données existent
            try:
                df_logs = self.spark.read.format("delta").load(log_path)
                
                # Créer table managée depuis les données
                df_logs.write.format("delta") \
                    .mode("ignore") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)
                
                print(f"✅ Table logs créée : {table_name}")
                
            except Exception as read_error:
                # Pas de données encore, créer table vide avec schéma
                print(f"ℹ️  Aucune donnée de log pour l'instant : {table_name}")
                
                # Créer schéma vide
                from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
                
                schema = StructType([
                    StructField("table_name", StringType(), True),
                    StructField("filename", StringType(), True),
                    StructField("input_format", StringType(), True),
                    StructField("ingestion_mode", StringType(), True),
                    StructField("output_zone", StringType(), True),
                    StructField("row_count", IntegerType(), True),
                    StructField("column_count", IntegerType(), True),
                    StructField("masking_applied", BooleanType(), True),
                    StructField("error_count", IntegerType(), True),
                    StructField("error_message", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("duration", DoubleType(), True),
                    StructField("env", StringType(), True),
                    StructField("log_ts", TimestampType(), True),
                    StructField("yyyy", IntegerType(), True),
                    StructField("mm", IntegerType(), True),
                    StructField("dd", IntegerType(), True)
                ])
                
                df_empty = self.spark.createDataFrame([], schema)
                df_empty.write.format("delta") \
                    .mode("ignore") \
                    .partitionBy("yyyy", "mm", "dd") \
                    .saveAsTable(table_name)
                
                print(f"✅ Table logs vide créée : {table_name}")
                
        except Exception as e:
            print(f"⚠️ Erreur création table logs : {e}")

    def create_quality_logs_table(self):
        """Crée table logs qualité dans Unity Catalog"""
        table_name = self._get_table_name("wax_data_quality_errors")
        log_path = self.config.log_quality_path

        try:
            # ✅ Vérifier si des données existent
            try:
                df_quality = self.spark.read.format("delta").load(log_path)
                
                # Créer table managée depuis les données
                df_quality.write.format("delta") \
                    .mode("ignore") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)
                
                print(f"✅ Table qualité créée : {table_name}")
                
            except Exception as read_error:
                # Pas de données encore, créer table vide avec schéma
                print(f"ℹ️  Aucune donnée qualité pour l'instant : {table_name}")
                
                # Créer schéma vide
                from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
                
                schema = StructType([
                    StructField("table_name", StringType(), True),
                    StructField("filename", StringType(), True),
                    StructField("column_name", StringType(), True),
                    StructField("error_message", StringType(), True),
                    StructField("raw_value", StringType(), True),
                    StructField("error_count", IntegerType(), True),
                    StructField("Zone", StringType(), True),
                    StructField("Env", StringType(), True),
                    StructField("log_ts", TimestampType(), True),
                    StructField("yyyy", IntegerType(), True),
                    StructField("mm", IntegerType(), True),
                    StructField("dd", IntegerType(), True)
                ])
                
                df_empty = self.spark.createDataFrame([], schema)
                df_empty.write.format("delta") \
                    .mode("ignore") \
                    .partitionBy("yyyy", "mm", "dd") \
                    .saveAsTable(table_name)
                
                print(f"✅ Table qualité vide créée : {table_name}")
                
        except Exception as e:
            print(f"⚠️ Erreur création table qualité : {e}")

    def show_recent_executions(self, limit: int = 20):
        """Affiche dernières exécutions"""
        full_table = self._get_table_name("wax_execution_logs")

        query = f"""
            SELECT 
                table_name, 
                input_format, 
                filename, 
                row_count, 
                column_count,
                status, 
                error_count, 
                env, 
                ROUND(duration, 2) as duration_sec,
                log_ts
            FROM {full_table}
            ORDER BY log_ts DESC
            LIMIT {limit}
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête executions : {e}")
            return None

    def show_statistics_by_table(self):
        """Statistiques par table (mois courant)"""
        full_table = self._get_table_name("wax_execution_logs")

        query = f"""
            SELECT 
                table_name,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
                ROUND(AVG(duration), 2) as avg_duration_sec,
                SUM(row_count) as total_rows_processed,
                SUM(error_count) as total_errors
            FROM {full_table}
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
            GROUP BY table_name
            ORDER BY total_runs DESC
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête stats : {e}")
            return None

    def show_top_errors(self, limit: int = 10):
        """Top erreurs qualité (mois courant)"""
        full_table = self._get_table_name("wax_data_quality_errors")

        query = f"""
            SELECT 
                error_message,
                COUNT(*) as occurrence_count,
                SUM(CAST(error_count AS BIGINT)) as total_error_count
            FROM {full_table}
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
              AND error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY total_error_count DESC
            LIMIT {limit}
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête top errors : {e}")
            return None

    def show_errors_by_table(self):
        """Erreurs par table (mois courant)"""
        full_table = self._get_table_name("wax_data_quality_errors")

        query = f"""
            SELECT 
                table_name,
                COUNT(DISTINCT filename) as file_count,
                COUNT(*) as error_occurrence_count,
                SUM(CAST(error_count AS BIGINT)) as total_errors
            FROM {full_table}
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
            GROUP BY table_name
            ORDER BY total_errors DESC
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête errors by table : {e}")
            return None

    def show_errors_by_column(self, table_name: str = None, limit: int = 20):
        """Erreurs par colonne pour une table"""
        full_table = self._get_table_name("wax_data_quality_errors")

        where_clause = ""
        if table_name:
            where_clause = f"AND table_name = '{table_name}'"

        query = f"""
            SELECT 
                table_name,
                column_name,
                error_message,
                SUM(CAST(error_count AS BIGINT)) as total_errors
            FROM {full_table}
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
              {where_clause}
            GROUP BY table_name, column_name, error_message
            ORDER BY total_errors DESC
            LIMIT {limit}
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête errors by column : {e}")
            return None

    def show_daily_trend(self, days: int = 7):
        """Tendance journalière des exécutions"""
        full_table = self._get_table_name("wax_execution_logs")

        query = f"""
            SELECT 
                yyyy,
                mm,
                dd,
                COUNT(*) as total_executions,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successes,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failures,
                SUM(row_count) as total_rows
            FROM {full_table}
            WHERE log_ts >= DATE_SUB(CURRENT_DATE(), {days})
            GROUP BY yyyy, mm, dd
            ORDER BY yyyy DESC, mm DESC, dd DESC
        """

        try:
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur requête trend : {e}")
            return None

    def table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe"""
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def display_all_dashboards(self):
        """Affiche tous les dashboards"""
        print("\n" + "=" * 80)
        print("📊 OBSERVABILITÉ - DASHBOARDS WAX")
        print("=" * 80)

        # Créer tables
        print("\n🔧 Création tables Unity Catalog...")
        self.create_execution_logs_table()
        self.create_quality_logs_table()

        # Vérifier si tables ont des données
        exec_table = self._get_table_name("wax_execution_logs")
        quality_table = self._get_table_name("wax_data_quality_errors")

        has_exec_data = self.table_exists(exec_table)
        has_quality_data = self.table_exists(quality_table)

        if not has_exec_data:
            print("\n⚠️ Aucune donnée d'exécution disponible")
            print("   Les dashboards seront disponibles après la première exécution du pipeline")
            return

        # Dernières exécutions
        print("\n✅ Dernières exécutions :")
        df = self.show_recent_executions()
        if df and df.count() > 0:
            df.show(20, truncate=False)
        else:
            print("   Aucune exécution enregistrée")

        # Stats par table
        print("\n📈 Statistiques par table (mois courant) :")
        df = self.show_statistics_by_table()
        if df and df.count() > 0:
            df.show(truncate=False)
        else:
            print("   Aucune statistique disponible")

        if has_quality_data:
            # Top erreurs
            print("\n📊 Top 10 erreurs qualité :")
            df = self.show_top_errors()
            if df and df.count() > 0:
                df.show(truncate=False)
            else:
                print("   Aucune erreur enregistrée")

            # Erreurs par table
            print("\n📉 Erreurs par table :")
            df = self.show_errors_by_table()
            if df and df.count() > 0:
                df.show(truncate=False)
            else:
                print("   Aucune erreur enregistrée")

        # Tendance 7 jours
        print("\n📅 Tendance 7 derniers jours :")
        df = self.show_daily_trend()
        if df and df.count() > 0:
            df.show(truncate=False)
        else:
            print("   Aucune donnée de tendance")

        print("\n" + "=" * 80)
