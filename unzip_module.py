"""
unzip_module.py
MODULE 1 : Dézipage des fichiers ZIP
Extrait les fichiers ZIP depuis input/zip/ vers extracted/
"""

import os
import zipfile
import shutil
from datetime import datetime
from pyspark.sql import SparkSession


class UnzipManager:
    """Gestionnaire de dézipage de fichiers ZIP"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog
        self.zip_dir = f"{config.volume_base}/input/zip"
        self.extract_base_dir = f"{config.volume_base}/extracted"
        self.archive_dir = f"{config.volume_base}/input/zip/processed"
        
        # Conversion pour Python standard (pandas, os, zipfile)
        self.zip_dir_fs = self._to_fs_path(self.zip_dir)
        self.extract_base_dir_fs = self._to_fs_path(self.extract_base_dir)
        self.archive_dir_fs = self._to_fs_path(self.archive_dir)
    
    def _to_fs_path(self, unity_catalog_path: str) -> str:
        """Convertit chemin Unity Catalog en chemin système de fichiers"""
        if unity_catalog_path.startswith("/Volumes"):
            return unity_catalog_path.replace("/Volumes", "/dbfs/Volumes")
        return unity_catalog_path
    
    def process_all_zips(self) -> dict:
        """
        Traite tous les fichiers ZIP présents dans input/zip/
        
        Returns:
            dict: Statistiques du traitement
        """
        
        print("=" * 80)
        print("📦 MODULE 1 : DÉZIPAGE")
        print("=" * 80)
        
        print(f"\n📂 Répertoire source : {self.zip_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        # Créer répertoire extraction si nécessaire
        os.makedirs(self.extract_base_dir_fs, exist_ok=True)
        os.makedirs(self.archive_dir_fs, exist_ok=True)
        
        # Lister tous les ZIP
        try:
            if not os.path.exists(self.zip_dir_fs):
                print(f"\n⚠️  Répertoire ZIP introuvable : {self.zip_dir}")
                print(f"   Créer le répertoire et y placer les fichiers ZIP")
                os.makedirs(self.zip_dir_fs, exist_ok=True)
                return {"status": "NO_DATA", "zip_count": 0, "error": "Directory created"}
            
            all_files = os.listdir(self.zip_dir_fs)
            zip_files = [f for f in all_files if f.endswith('.zip')]
            
        except Exception as e:
            print(f"❌ Erreur listage ZIP : {e}")
            return {"status": "ERROR", "error": str(e), "zip_count": 0}
        
        if not zip_files:
            print("\n⚠️  Aucun fichier ZIP trouvé dans input/zip/")
            return {"status": "NO_DATA", "zip_count": 0}
        
        print(f"\n✅ {len(zip_files)} fichier(s) ZIP trouvé(s)")
        
        # Extraire chaque ZIP
        extracted_count = 0
        failed_count = 0
        total_files = 0
        results = []
        
        for idx, zip_file in enumerate(zip_files, 1):
            print(f"\n{'─' * 80}")
            print(f"📦 ZIP {idx}/{len(zip_files)}: {zip_file}")
            print(f"{'─' * 80}")
            
            try:
                result = self._extract_single_zip(zip_file)
                
                if result["status"] == "SUCCESS":
                    extracted_count += 1
                    total_files += result["file_count"]
                    print(f"✅ {result['file_count']} fichier(s) extrait(s)")
                    print(f"   → {result['extract_dir']}")
                    
                    # Archiver ZIP traité
                    self._archive_zip(zip_file)
                    
                    results.append({
                        "zip_file": zip_file,
                        "status": "SUCCESS",
                        "file_count": result["file_count"],
                        "target_table": result.get("target_table", "unknown")
                    })
                else:
                    failed_count += 1
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    
                    results.append({
                        "zip_file": zip_file,
                        "status": "FAILED",
                        "error": result.get("error", "Unknown")
                    })
                    
            except Exception as e:
                failed_count += 1
                print(f"❌ Erreur : {e}")
                import traceback
                traceback.print_exc()
                
                results.append({
                    "zip_file": zip_file,
                    "status": "FAILED",
                    "error": str(e)
                })
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ DÉZIPAGE")
        print("=" * 80)
        print(f"✅ ZIP extraits     : {extracted_count}")
        print(f"❌ ZIP en échec     : {failed_count}")
        print(f"📄 Fichiers extraits : {total_files}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "zip_count": extracted_count,
            "failed_count": failed_count,
            "total_files": total_files,
            "results": results
        }
    
    def _extract_single_zip(self, zip_filename: str) -> dict:
        """
        Extrait un seul fichier ZIP
        
        Args:
            zip_filename: Nom du fichier ZIP
            
        Returns:
            dict: Résultat de l'extraction
        """
        
        zip_path = os.path.join(self.zip_dir_fs, zip_filename)
        
        # Déterminer le répertoire d'extraction selon le nom du fichier
        # Ex: site_20250902.zip → extracted/site/
        # Ex: customer_data.zip → extracted/customer/
        base_name = zip_filename.replace('.zip', '')
        
        # Extraire le nom de la table (première partie avant _)
        if '_' in base_name:
            table_name = base_name.split('_')[0]
        else:
            table_name = base_name
        
        extract_dir = os.path.join(self.extract_base_dir_fs, table_name)
        
        # Créer répertoire si nécessaire
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Extraire
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lister les fichiers dans le ZIP
                file_list = zip_ref.namelist()
                
                # Filtrer les fichiers système (comme __MACOSX)
                valid_files = [f for f in file_list 
                              if not f.startswith('__MACOSX') 
                              and not f.startswith('.')
                              and not f.endswith('/')]
                
                if not valid_files:
                    return {
                        "status": "ERROR",
                        "error": "No valid files in ZIP"
                    }
                
                # Extraire seulement les fichiers valides
                for file in valid_files:
                    zip_ref.extract(file, extract_dir)
                
                file_count = len(valid_files)
            
            return {
                "status": "SUCCESS",
                "file_count": file_count,
                "extract_dir": extract_dir.replace("/dbfs", ""),  # Afficher chemin Unity Catalog
                "target_table": table_name
            }
            
        except zipfile.BadZipFile as e:
            return {
                "status": "ERROR",
                "error": f"Invalid ZIP file: {e}"
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    def _archive_zip(self, zip_filename: str):
        """
        Archive le ZIP traité dans input/zip/processed/
        
        Args:
            zip_filename: Nom du fichier ZIP
        """
        
        source = os.path.join(self.zip_dir_fs, zip_filename)
        
        # Ajouter timestamp pour éviter les écrasements
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = zip_filename.replace('.zip', '')
        archive_name = f"{base_name}_{timestamp}.zip"
        
        dest = os.path.join(self.archive_dir_fs, archive_name)
        
        try:
            shutil.move(source, dest)
            print(f"   📦 Archivé : {archive_name}")
        except Exception as e:
            print(f"   ⚠️  Erreur archivage : {e}")
            # Ne pas bloquer le processus si l'archivage échoue
    
    def list_extracted_files(self, table_name: str = None) -> list:
        """
        Liste les fichiers extraits
        
        Args:
            table_name: Nom de la table (optionnel, sinon tous)
            
        Returns:
            Liste des fichiers extraits
        """
        
        try:
            if table_name:
                target_dir = os.path.join(self.extract_base_dir_fs, table_name)
                if os.path.exists(target_dir):
                    return os.listdir(target_dir)
                return []
            else:
                # Lister toutes les tables
                all_files = {}
                if os.path.exists(self.extract_base_dir_fs):
                    for table_dir in os.listdir(self.extract_base_dir_fs):
                        table_path = os.path.join(self.extract_base_dir_fs, table_dir)
                        if os.path.isdir(table_path):
                            all_files[table_dir] = os.listdir(table_path)
                return all_files
                
        except Exception as e:
            print(f"⚠️  Erreur listage fichiers extraits : {e}")
            return [] if table_name else {}


def main():
    """Point d'entrée du module dézipage"""
    
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    print("🚀 Démarrage Module 1 : Dézipage")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-Module1-Unzip").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1"
    )
    
    # Dézipper
    unzip_manager = UnzipManager(spark, config)
    result = unzip_manager.process_all_zips()
    
    # Afficher fichiers extraits
    if result["status"] in ["SUCCESS", "PARTIAL"]:
        print("\n📋 Fichiers extraits par table :")
        extracted = unzip_manager.list_extracted_files()
        for table, files in extracted.items():
            print(f"   • {table}: {len(files)} fichier(s)")
            for f in files[:3]:
                print(f"      - {f}")
            if len(files) > 3:
                print(f"      ... et {len(files) - 3} autre(s)")
    
    # Retourner code de sortie
    if result["status"] == "SUCCESS":
        print("\n✅ Module 1 terminé avec succès")
        return 0
    elif result["status"] == "PARTIAL":
        print("\n⚠️  Module 1 terminé avec des erreurs partielles")
        return 1
    else:
        print(f"\n❌ Module 1 terminé avec erreurs : {result.get('error', 'Unknown')}")
        return 2


if __name__ == "__main__":
    exit(main())
