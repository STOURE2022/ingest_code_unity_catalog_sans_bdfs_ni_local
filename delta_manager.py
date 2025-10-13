"""
Gestion tables Delta Lake - Unity Catalog uniquement
Tables managées uniquement
"""

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class DeltaManager:
    """Gestionnaire Delta Lake - Unity Catalog tables managées"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self._setup_unity_catalog()

    def _setup_unity_catalog(self):
        """Configure Unity Catalog"""
        try:
            # Créer schéma fichiers si nécessaire
            self.spark.sql(f"""
                CREATE SCHEMA IF NOT EXISTS {self.config.catalog}.{self.config.schema_files}
            """)

            # Créer volume si nécessaire
            self.spark.sql(f"""
                CREATE VOLUME IF NOT EXISTS {self.config.catalog}.{self.config.schema_files}.{self.config.volume}
            """)

            print(f"✅ Volume : {self.config.catalog}.{self.config.schema_files}.{self.config.volume}")

            # Créer schéma tables si différent
            if self.config.schema_tables != self.config.schema_files:
                self.spark.sql(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.config.catalog}.{self.config.schema_tables}
                """)
                print(f"✅ Schéma tables : {self.config.catalog}.{self.config.schema_tables}")

        except Exception as e:
            print(f"⚠️ Unity Catalog setup : {e}")

    def save_delta(self, df: DataFrame, table_name: str, mode: str = "append",
                   add_ts: bool = False, parts: dict = None,
                   file_name_received: str = None):
        """
        Sauvegarde DataFrame en table managée Delta

        Args:
            df: DataFrame à sauvegarder
            table_name: Nom complet table (catalog.schema.table)
            mode: Mode écriture (append, overwrite)
            add_ts: Ajouter FILE_PROCESS_DATE
            parts: Dict avec yyyy, mm, dd
            file_name_received: Nom fichier source
        """
        from .utils import deduplicate_columns
        import os

        # Extraire dates
        today = datetime.today()
        y = int((parts or {}).get("yyyy", today.year))
        m = int((parts or {}).get("mm", today.month))
        d = int((parts or {}).get("dd", today.day))

        # Ajouter métadonnées
        if add_ts:
            df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())

        if file_name_received:
            base_name = os.path.splitext(os.path.basename(file_name_received))[0]
            df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))

        # Réorganiser colonnes (métadonnées en premier)
        ordered_cols = []
        for meta_col in ["FILE_NAME_RECEIVED", "FILE_PROCESS_DATE"]:
            if meta_col in df.columns:
                ordered_cols.append(meta_col)
        other_cols = [c for c in df.columns if c not in ordered_cols]
        df = df.select(ordered_cols + other_cols)

        # Dédupliquer colonnes
        df = deduplicate_columns(df)

        # Ajouter colonnes de partitionnement
        df = (df.withColumn("yyyy", F.lit(y).cast("int"))
              .withColumn("mm", F.lit(m).cast("int"))
              .withColumn("dd", F.lit(d).cast("int")))

        # Optimisation partitionnement
        row_count = df.count()
        if row_count > 1_000_000:
            num_partitions = max(1, row_count // 1_000_000)
            df = df.repartition(num_partitions, "yyyy", "mm", "dd")

        # Sauvegarder table managée
        print(f"💾 Sauvegarde : {table_name}")

        try:
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode(mode) \
                .partitionBy("yyyy", "mm", "dd") \
                .saveAsTable(table_name)

            print(f"✅ Table sauvegardée : {table_name}")
            print(f"   Mode: {mode}, Date: {y}-{m:02d}-{d:02d}, Lignes: {row_count}")
        except Exception as e:
            print(f"❌ Erreur sauvegarde : {e}")
            raise

    def get_delta_table(self, table_name: str) -> DeltaTable:
        """
        Récupère une DeltaTable managée

        Args:
            table_name: Nom complet table (catalog.schema.table)

        Returns:
            DeltaTable instance
        """
        return DeltaTable.forName(self.spark, table_name)

    def table_exists(self, table_name: str) -> bool:
        """
        Vérifie si table existe

        Args:
            table_name: Nom complet table

        Returns:
            True si existe
        """
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False