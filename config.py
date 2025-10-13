"""
Configuration Unity Catalog - Simplifié
Plus de support DBFS ni Local, Unity Catalog uniquement
"""

from typing import Dict, Any


class Config:
    """
    Configuration Unity Catalog uniquement
    Fichiers dans Volume, Tables dans Schéma séparé
    """

    def __init__(
            self,
            # Unity Catalog - Fichiers
            catalog: str = "abu_catalog",
            schema_files: str = "databricksassetbundletest",
            volume: str = "externalvolumetes",

            # Unity Catalog - Tables
            schema_tables: str = "gdp_poc_dev",

            # Paramètres généraux
            env: str = "dev",
            version: str = "v1"
    ):
        """
        Configuration Unity Catalog

        Args:
            catalog: Catalogue Unity (ex: "abu_catalog")
            schema_files: Schéma pour fichiers/volume (ex: "databricksassetbundletest")
            volume: Nom du volume (ex: "externalvolumetes")
            schema_tables: Schéma pour tables Delta (ex: "gdp_poc_dev")
            env: Environnement (dev/int/prd)
            version: Version pipeline (v1, v2, etc.)
        """
        # Paramètres Unity Catalog
        self.catalog = catalog
        self.schema_files = schema_files
        self.volume = volume
        self.schema_tables = schema_tables

        # Paramètres généraux
        self.env = env
        self.version = version

        # Chemins construits
        self.volume_base = f"/Volumes/{catalog}/{schema_files}/{volume}"

        # Fichiers sources
        self.zip_path = f"{self.volume_base}/input/wax_data.zip"
        self.excel_path = f"{self.volume_base}/input/wax_config.xlsx"
        self.extract_dir = f"{self.volume_base}/temp/extracted"

        # Logs
        self.log_exec_path = f"{self.volume_base}/logs/execution"
        self.log_quality_path = f"{self.volume_base}/logs/quality"

        # Patterns de dates
        self.date_patterns = [
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyyMMddHHmmss",
            "yyyyMMdd"
        ]

        # Mapping types
        self.type_mapping = {
            "STRING": "string",
            "INTEGER": "int",
            "INT": "int",
            "LONG": "long",
            "FLOAT": "float",
            "DOUBLE": "double",
            "DECIMAL": "decimal(18,2)",
            "BOOLEAN": "boolean",
            "DATE": "date",
            "TIMESTAMP": "timestamp"
        }

    def get_table_full_name(self, table_name: str) -> str:
        """
        Retourne nom complet table Unity Catalog

        Args:
            table_name: Nom simple de la table

        Returns:
            Nom complet: catalog.schema_tables.table_name
        """
        return f"{self.catalog}.{self.schema_tables}.{table_name}"

    def get_bad_records_path(self, table_name: str) -> str:
        """Chemin bad records dans le volume"""
        return f"{self.volume_base}/logs/badrecords/{self.env}/{table_name}"

    def validate_paths(self) -> Dict[str, Any]:
        """Valide configuration Unity Catalog"""
        issues = []

        # Vérifier format chemins
        if not self.volume_base.startswith("/Volumes/"):
            issues.append(f"❌ volume_base doit commencer par /Volumes/")

        # Info configuration
        issues.append(f"ℹ️ Configuration Unity Catalog:")
        issues.append(f"   📁 Fichiers: {self.catalog}.{self.schema_files}.{self.volume}")
        issues.append(f"   🗄️  Tables: {self.catalog}.{self.schema_tables}")
        issues.append(f"   📂 Volume: {self.volume_base}")

        return {
            "valid": not any(msg.startswith("❌") for msg in issues),
            "issues": issues,
            "mode": "Unity Catalog - Tables Managées"
        }

    def print_config(self):
        """Affiche configuration"""
        print("=" * 80)
        print("⚙️  CONFIGURATION UNITY CATALOG")
        print("=" * 80)
        print(f"📚 Catalogue    : {self.catalog}")
        print(f"📂 Schéma Files : {self.schema_files}")
        print(f"💾 Volume       : {self.volume}")
        print(f"🗄️  Schéma Tables: {self.schema_tables}")
        print(f"🌍 Environnement: {self.env}")
        print(f"📌 Version      : {self.version}")
        print()
        print(f"📍 Chemins:")
        print(f"   ZIP   : {self.zip_path}")
        print(f"   Excel : {self.excel_path}")
        print(f"   Extract: {self.extract_dir}")
        print(f"   Logs  : {self.log_exec_path}")
        print("=" * 80)

    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire"""
        return {
            "catalog": self.catalog,
            "schema_files": self.schema_files,
            "volume": self.volume,
            "schema_tables": self.schema_tables,
            "env": self.env,
            "version": self.version,
            "volume_base": self.volume_base,
            "zip_path": self.zip_path,
            "excel_path": self.excel_path,
            "extract_dir": self.extract_dir,
            "log_exec_path": self.log_exec_path,
            "log_quality_path": self.log_quality_path
        }

    def __repr__(self) -> str:
        return (
            f"Config(Unity Catalog)\n"
            f"  Fichiers: {self.catalog}.{self.schema_files}.{self.volume}\n"
            f"  Tables: {self.catalog}.{self.schema_tables}\n"
            f"  Env: {self.env}, Version: {self.version}"
        )