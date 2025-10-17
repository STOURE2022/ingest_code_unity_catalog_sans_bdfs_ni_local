"""
Configuration Unity Catalog - Pour Pipeline WAX avec Auto Loader
Plus de support DBFS ni Local, Unity Catalog uniquement
"""

from typing import Dict, Any
import os


class Config:
    """
    Configuration Unity Catalog pour Pipeline WAX
    Fichiers dans Volume, Tables dans Schéma séparé
    Support Auto Loader avec checkpoints et schemas
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

        # ========== CHEMINS DE BASE ==========
        self.volume_base = f"/Volumes/{catalog}/{schema_files}/{volume}"

        # ========== RÉPERTOIRES D'ENTRÉE ==========
        self.input_base = f"{self.volume_base}/input"
        
        # ✅ CHANGEMENT 1 : zip_dir (répertoire) au lieu de zip_path (fichier)
        self.zip_dir = f"{self.input_base}/zip"
        
        # Config Excel
        self.config_dir = f"{self.input_base}/config"
        
        # ✅ CHANGEMENT 2 : Nom cohérent avec autoloader_module.py
        self.excel_path = f"{self.input_base}/wax_config.xlsx"
        
        # ========== RÉPERTOIRES DE TRAVAIL ==========
        # ✅ CHANGEMENT 3 : extracted/ à la racine (pas dans temp/)
        self.extract_dir = f"{self.volume_base}/extracted"
        
        # ✅ NOUVEAU : Checkpoints pour Auto Loader
        self.checkpoint_dir = f"{self.volume_base}/checkpoints"
        
        # ✅ NOUVEAU : Schemas pour Auto Loader
        self.schema_dir = f"{self.volume_base}/schemas"
        
        # ✅ NOUVEAU : Archive pour ZIP traités
        self.archive_dir = f"{self.zip_dir}/processed"

        # ========== LOGS ==========
        self.log_base = f"{self.volume_base}/logs"
        self.log_exec_path = f"{self.log_base}/execution"
        self.log_quality_path = f"{self.log_base}/quality"
        
        # Bad records
        self.bad_records_base = f"{self.log_base}/badrecords"

        # ========== PATTERNS & MAPPINGS (conservés) ==========
        self.date_patterns = [
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyyMMddHHmmss",
            "yyyyMMdd"
        ]

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
        return f"{self.bad_records_base}/{self.env}/{table_name}"

    def validate_paths(self) -> Dict[str, Any]:
        """
        Valide configuration Unity Catalog
        ✅ NOUVEAU : Crée automatiquement les répertoires manquants
        """
        issues = []

        # Vérifier format chemins
        if not self.volume_base.startswith("/Volumes/"):
            issues.append(f"❌ volume_base doit commencer par /Volumes/")

        # Vérifier existence volume
        if not os.path.exists(self.volume_base):
            issues.append(f"❌ Volume introuvable : {self.volume_base}")
            return {
                "valid": False,
                "issues": issues,
                "mode": "Unity Catalog - Error"
            }

        # Info configuration
        issues.append(f"ℹ️ Configuration Unity Catalog:")
        issues.append(f"   📁 Fichiers: {self.catalog}.{self.schema_files}.{self.volume}")
        issues.append(f"   🗄️  Tables: {self.catalog}.{self.schema_tables}")
        issues.append(f"   📂 Volume: {self.volume_base}")

        # ✅ NOUVEAU : Créer répertoires automatiquement
        dirs_to_create = [
            self.input_base,
            self.zip_dir,
            self.config_dir,
            self.extract_dir,
            self.checkpoint_dir,
            self.schema_dir,
            self.archive_dir,
            self.log_base,
            self.log_exec_path,
            self.log_quality_path,
            self.bad_records_base
        ]

        for dir_path in dirs_to_create:
            if not os.path.exists(dir_path):
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    issues.append(f"ℹ️ Créé : {dir_path}")
                except Exception as e:
                    issues.append(f"⚠️ Impossible de créer {dir_path}: {e}")

        # Vérifier fichier Excel
        if os.path.exists(self.excel_path):
            issues.append(f"✅ Excel trouvé : {self.excel_path}")
        else:
            issues.append(f"⚠️ Excel manquant : {self.excel_path}")

        return {
            "valid": not any(msg.startswith("❌") for msg in issues),
            "issues": issues,
            "mode": "Unity Catalog - Tables Managées"
        }

    def print_config(self):
        """Affiche configuration"""
        print("=" * 80)
        print("⚙️  CONFIGURATION UNITY CATALOG - WAX PIPELINE")
        print("=" * 80)
        print(f"📚 Catalogue    : {self.catalog}")
        print(f"📂 Schéma Files : {self.schema_files}")
        print(f"💾 Volume       : {self.volume}")
        print(f"🗄️  Schéma Tables: {self.schema_tables}")
        print(f"🌍 Environnement: {self.env}")
        print(f"📌 Version      : {self.version}")
        print()
        print(f"📍 Chemins principaux:")
        print(f"   Base Volume  : {self.volume_base}")
        print(f"   ZIP Dir      : {self.zip_dir}")
        print(f"   Excel        : {self.excel_path}")
        print(f"   Extracted    : {self.extract_dir}")
        print(f"   Checkpoints  : {self.checkpoint_dir}")
        print(f"   Schemas      : {self.schema_dir}")
        print(f"   Logs         : {self.log_base}")
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
            "zip_dir": self.zip_dir,
            "excel_path": self.excel_path,
            "extract_dir": self.extract_dir,
            "checkpoint_dir": self.checkpoint_dir,
            "schema_dir": self.schema_dir,
            "archive_dir": self.archive_dir,
            "log_exec_path": self.log_exec_path,
            "log_quality_path": self.log_quality_path
        }

    def __repr__(self) -> str:
        return (
            f"Config(Unity Catalog - WAX Pipeline)\n"
            f"  Fichiers: {self.catalog}.{self.schema_files}.{self.volume}\n"
            f"  Tables: {self.catalog}.{self.schema_tables}\n"
            f"  Env: {self.env}, Version: {self.version}"
        )
```

---

## 📊 **Tableau des Changements**

| Propriété | AVANT (votre config) | APRÈS (corrigé) | Raison |
|-----------|---------------------|-----------------|--------|
| **zip_path** | `input/wax_data.zip` (fichier) | **zip_dir** `input/zip/` (répertoire) | unzip_module liste TOUS les ZIP |
| **excel_path** | `input/wax_config.xlsx` | `input/wax_config.xlsx` | ✅ Conservé (nom actuel) |
| **extract_dir** | `temp/extracted/` | `extracted/` | Cohérence avec modules |
| **checkpoint_dir** | ❌ Manquant | `checkpoints/` | ✅ Requis pour Auto Loader |
| **schema_dir** | ❌ Manquant | `schemas/` | ✅ Requis pour Auto Loader |
| **archive_dir** | ❌ Manquant | `input/zip/processed/` | ✅ Pour archiver ZIP traités |
| **config_dir** | ❌ Manquant | `input/config/` | ✅ Organisation |
| **validate_paths()** | Basique | Crée répertoires auto | ✅ Évite erreurs |

---

## 🎯 **Actions à Faire**

### **1. Remplacer config.py**
Copiez le nouveau `config.py` ci-dessus

### **2. Vérifier structure de vos fichiers**

Votre volume doit avoir cette structure :
```
/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/
│
├── input/
│   ├── zip/                          ← Déposer TOUS vos ZIP ici
│   │   ├── site_20250902.zip
│   │   ├── customer_data.zip
│   │   └── processed/                ← Créé auto (archives)
│   │
│   └── wax_config.xlsx               ← Votre fichier Excel actuel
│
├── extracted/                        ← Créé auto (pas temp/extracted)
├── checkpoints/                      ← Créé auto (Auto Loader)
├── schemas/                          ← Créé auto (Auto Loader)
└── logs/                             ← Créé auto
