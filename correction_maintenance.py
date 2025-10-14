"""
maintenance.py
Scripts de maintenance Delta Lake (OPTIMIZE, VACUUM, ANALYZE)
Version Unity Catalog corrigée
"""


class MaintenanceManager:
    """Gestion maintenance tables Delta"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def _get_full_table_name(self, table_name: str) -> str:
        """
        Retourne nom complet table Unity Catalog
        
        Args:
            table_name: Nom de la table (peut être avec ou sans catalog.schema)
        
        Returns:
            Nom complet: catalog.schema.table
        """
        # Si déjà un nom complet, retourner tel quel
        if "." in table_name and table_name.count(".") >= 2:
            return table_name
        
        # Sinon, construire avec config
        return f"{self.config.catalog}.{self.config.schema_tables}.{table_name}"

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

    # ... reste des méthodes identiques
