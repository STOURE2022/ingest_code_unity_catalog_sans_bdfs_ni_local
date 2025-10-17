"""
config.py
Configuration centralisée pour le pipeline WAX
Unity Catalog uniquement
"""

import os


class Config:
    """Configuration du pipeline WAX"""
    
    def __init__(self, catalog: str, schema_files: str, volume: str,
                 schema_tables: str, env: str = "dev", version: str = "v1"):
        """
        Initialise la configuration
        
        Args:
            catalog: Nom du catalogue Unity Catalog
            schema_files: Schéma contenant le volume (pour les fichiers)
            volume: Nom du volume Unity Catalog
            schema_tables: Schéma contenant les tables Delta
            env: Environnement (dev, prod, etc.)
            version: Version du pipeline
        """
        
        self.catalog = catalog
        self.schema_files = schema_files
        self.volume = volume
        self.schema_tables = schema_tables
        self.env = env
        self.version = version
        
        # ========== CHEMINS UNITY CATALOG (directs) ==========
        # Dans Databricks Jobs, /Volumes/ fonctionne pour tout
        
        # Base du volume
        self.volume_base = f"/Volumes/{catalog}/{schema_files}/{volume}"
        
        # Répertoires d'entrée
        self.input_base = f"{self.volume_base}/input"
        self.zip_dir = f"{self.input_base}/zip"
        self.config_dir = f"{self.input_base}/config"
        
        # Fichier Excel de configuration
        self.excel_path = f"{self.config_dir}/wax_configuration.xlsx"
        
        # Répertoires de travail
        self.extract_dir = f"{self.volume_base}/extracted"
        self.checkpoint_dir = f"{self.volume_base}/checkpoints"
        self.schema_dir = f"{self.volume_base}/schemas"
        
        # Répertoires de logs
        self.log_base = f"{self.volume_base}/logs"
        self.log_execution_path = f"{self.log_base}/execution"
        self.log_quality_path = f"{self.log_base}/quality"
        
        # Répertoires de sortie
        self.output_base = f"{self.volume_base}/output"
        self.bad_records_base = f"{self.output_base}/bad_records"
        
        # Archive
        self.archive_dir = f"{self.zip_dir}/processed"
        
        # Tables Unity Catalog
        self.tables_schema = f"{catalog}.{schema_tables}"
    
    def get_table_path(self, table_name: str, suffix: str = "") -> str:
        """
        Retourne le chemin complet d'une table
        
        Args:
            table_name: Nom de la table
            suffix: Suffixe (_all, _last, _staging, etc.)
            
        Returns:
            Chemin complet de la table
        """
        full_name = f"{table_name}{suffix}" if suffix else table_name
        return f"{self.catalog}.{self.schema_tables}.{full_name}"
    
    def get_bad_records_path(self, table_name: str) -> str:
        """
        Retourne le chemin pour les bad records d'une table
        
        Args:
            table_name: Nom de la table
            
        Returns:
            Chemin pour les bad records
        """
        return f"{self.bad_records_base}/{table_name}"
    
    def validate_paths(self) -> dict:
        """
        Valide l'existence des chemins principaux
        
        Returns:
            dict: Résultat de la validation
        """
        issues = []
        
        # Vérifier existence du volume
        if not os.path.exists(self.volume_base):
            issues.append(f"❌ Volume introuvable : {self.volume_base}")
            return {
                "valid": False,
                "mode": "error",
                "issues": issues
            }
        
        # Vérifier répertoires critiques
        critical_dirs = [
            (self.input_base, "Répertoire input"),
            (self.config_dir, "Répertoire config"),
        ]
        
        for dir_path, dir_name in critical_dirs:
            if not os.path.exists(dir_path):
                issues.append(f"⚠️  {dir_name} manquant : {dir_path}")
        
        # Vérifier fichier Excel
        if not os.path.exists(self.excel_path):
            issues.append(f"⚠️  Fichier Excel manquant : {self.excel_path}")
        else:
            issues.append(f"ℹ️  Fichier Excel trouvé : {self.excel_path}")
        
        # Créer répertoires de travail si nécessaires
        work_dirs = [
            self.extract_dir,
            self.checkpoint_dir,
            self.schema_dir,
            self.log_base,
            self.log_execution_path,
            self.log_quality_path,
            self.output_base,
            self.bad_records_base,
            self.archive_dir
        ]
        
        for work_dir in work_dirs:
            if not os.path.exists(work_dir):
                try:
                    os.makedirs(work_dir, exist_ok=True)
                    issues.append(f"ℹ️  Créé : {work_dir}")
                except Exception as e:
                    issues.append(f"⚠️  Impossible de créer {work_dir}: {e}")
        
        # Déterminer statut global
        has_errors = any(issue.startswith("❌") for issue in issues)
        
        return {
            "valid": not has_errors,
            "mode": "error" if has_errors else "ok",
            "issues": issues
        }
    
    def print_config(self):
        """Affiche la configuration"""
        
        print("\n📋 Configuration WAX Pipeline")
        print("─" * 60)
        print(f"Environnement  : {self.env}")
        print(f"Version        : {self.version}")
        print(f"Catalogue      : {self.catalog}")
        print(f"Schéma fichiers: {self.schema_files}")
        print(f"Volume         : {self.volume}")
        print(f"Schéma tables  : {self.schema_tables}")
        print("─" * 60)
        print(f"Base volume    : {self.volume_base}")
        print(f"Excel config   : {self.excel_path}")
        print("─" * 60)
