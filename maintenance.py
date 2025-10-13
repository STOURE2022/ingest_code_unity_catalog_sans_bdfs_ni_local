"""
maintenance.py
Scripts de maintenance Delta Lake (OPTIMIZE, VACUUM, ANALYZE)
"""


class MaintenanceManager:
    """Gestion maintenance tables Delta"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _get_full_table_name(self, table_name: str) -> str:
        """Retourne nom complet table selon config"""
        if self.config.use_unity_catalog:
            return f"{self.config.unity_catalog}.{self.config.unity_schema}.{table_name}"
        else:
            return f"wax_obs.{table_name}"

    def optimize_table(self, table_name: str, z_order_cols: list = None):
        """
        OPTIMIZE + Z-ORDER sur une table

        Args:
            table_name: Nom de la table
            z_order_cols: Colonnes pour Z-ORDER (ex: ['merge_key', 'date'])
        """
        full_name = self._get_full_table_name(table_name)

        try:
            if z_order_cols:
                z_cols = ", ".join(z_order_cols)
                self.spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({z_cols})")
                print(f"✅ Table {full_name} optimisée avec Z-ORDER sur {z_cols}")
            else:
                self.spark.sql(f"OPTIMIZE {full_name}")
                print(f"✅ Table {full_name} optimisée")
        except Exception as e:
            print(f"❌ Erreur OPTIMIZE {full_name} : {e}")

    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        """
        VACUUM table (supprime anciennes versions)

        Args:
            table_name: Nom de la table
            retention_hours: Rétention en heures (défaut: 168h = 7 jours)
        """
        full_name = self._get_full_table_name(table_name)

        try:
            self.spark.sql(f"VACUUM {full_name} RETAIN {retention_hours} HOURS")
            print(f"✅ Table {full_name} nettoyée (retention {retention_hours}h)")
        except Exception as e:
            print(f"❌ Erreur VACUUM {full_name} : {e}")

    def analyze_table(self, table_name: str, compute_all_columns: bool = True):
        """
        ANALYZE TABLE (calcule statistiques)

        Args:
            table_name: Nom de la table
            compute_all_columns: Calculer stats pour toutes colonnes
        """
        full_name = self._get_full_table_name(table_name)

        try:
            if compute_all_columns:
                self.spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS FOR ALL COLUMNS")
                print(f"✅ Statistiques calculées pour toutes colonnes de {full_name}")
            else:
                self.spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS")
                print(f"✅ Statistiques calculées pour {full_name}")
        except Exception as e:
            print(f"❌ Erreur ANALYZE {full_name} : {e}")

    def show_table_history(self, table_name: str, limit: int = 20):
        """
        Affiche l'historique Delta d'une table

        Returns:
            DataFrame avec historique
        """
        full_name = self._get_full_table_name(table_name)

        try:
            return self.spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT {limit}")
        except Exception as e:
            print(f"❌ Erreur HISTORY {full_name} : {e}")
            return None

    def show_table_details(self, table_name: str):
        """
        Affiche détails d'une table

        Returns:
            DataFrame avec détails
        """
        full_name = self._get_full_table_name(table_name)

        try:
            return self.spark.sql(f"DESCRIBE EXTENDED {full_name}")
        except Exception as e:
            print(f"❌ Erreur DESCRIBE {full_name} : {e}")
            return None

    def get_table_stats(self, table_name: str):
        """
        Retourne statistiques de la table

        Returns:
            Dict avec statistiques
        """
        full_name = self._get_full_table_name(table_name)

        try:
            df = self.spark.table(full_name)

            stats = {
                "table_name": table_name,
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns,
                "size_bytes": None  # Calculé via DESCRIBE DETAIL
            }

            # Taille via DESCRIBE DETAIL
            try:
                detail = self.spark.sql(f"DESCRIBE DETAIL {full_name}")
                size_bytes = detail.select("sizeInBytes").first()[0]
                stats["size_bytes"] = size_bytes
                stats["size_mb"] = round(size_bytes / (1024 * 1024), 2) if size_bytes else None
            except:
                pass

            return stats
        except Exception as e:
            print(f"❌ Erreur stats {full_name} : {e}")
            return None

    def maintain_all_tables(self, table_names: list, z_order_keys: dict = None):
        """
        Maintenance complète de plusieurs tables

        Args:
            table_names: Liste noms de tables
            z_order_keys: Dict {table_name: [colonnes]}
        """
        print("\n" + "=" * 80)
        print("🔧 MAINTENANCE TABLES DELTA")
        print("=" * 80)

        z_order_keys = z_order_keys or {}

        for table_name in table_names:
            print(f"\n📋 Table : {table_name}")

            # Stats avant
            stats_before = self.get_table_stats(table_name)
            if stats_before:
                print(f"   Lignes : {stats_before['row_count']:,}")
                if stats_before.get('size_mb'):
                    print(f"   Taille : {stats_before['size_mb']} MB")

            # OPTIMIZE
            z_cols = z_order_keys.get(table_name)
            self.optimize_table(table_name, z_order_cols=z_cols)

            # ANALYZE
            self.analyze_table(table_name)

            # VACUUM (optionnel - décommenter si souhaité)
            # self.vacuum_table(table_name, retention_hours=168)

        print("\n" + "=" * 80)
        print("✅ MAINTENANCE TERMINÉE")
        print("=" * 80)

    def show_lineage(self, table_name: str):
        """
        Affiche lineage Unity Catalog (si disponible)

        Returns:
            DataFrame avec lineage
        """
        if not self.config.use_unity_catalog:
            print("⚠️ Lineage disponible uniquement avec Unity Catalog")
            return None

        try:
            query = f"""
                SELECT * FROM system.access.table_lineage
                WHERE table_catalog = '{self.config.unity_catalog}'
                  AND table_schema = '{self.config.unity_schema}'
                  AND table_name = '{table_name}'
            """
            return self.spark.sql(query)
        except Exception as e:
            print(f"⚠️ Erreur lineage : {e}")
            return None