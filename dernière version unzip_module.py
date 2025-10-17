class UnzipManager:
    """Gestionnaire de dézipage de fichiers ZIP"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog (directs - fonctionne dans Databricks Jobs)
        self.zip_dir = f"{config.volume_base}/input/zip"
        self.extract_base_dir = f"{config.volume_base}/extracted"
        self.archive_dir = f"{config.volume_base}/input/zip/processed"
    
    def process_all_zips(self) -> dict:
        """Traite tous les fichiers ZIP"""
        
        print("=" * 80)
        print("📦 MODULE 1 : DÉZIPAGE")
        print("=" * 80)
        
        print(f"\n📂 Répertoire source : {self.zip_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        # Créer répertoires
        os.makedirs(self.extract_base_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        
        # Lister ZIP
        try:
            if not os.path.exists(self.zip_dir):
                print(f"\n⚠️  Répertoire ZIP introuvable : {self.zip_dir}")
                os.makedirs(self.zip_dir, exist_ok=True)
                return {"status": "NO_DATA", "zip_count": 0}
            
            all_files = os.listdir(self.zip_dir)
            zip_files = [f for f in all_files if f.endswith('.zip')]
            
        except Exception as e:
            print(f"❌ Erreur listage : {e}")
            return {"status": "ERROR", "error": str(e), "zip_count": 0}
        
        if not zip_files:
            print("\n⚠️  Aucun ZIP trouvé")
            return {"status": "NO_DATA", "zip_count": 0}
        
        print(f"\n✅ {len(zip_files)} ZIP trouvé(s)")
        
        # Extraire chaque ZIP
        extracted_count = 0
        failed_count = 0
        total_files = 0
        
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
                    self._archive_zip(zip_file)
                else:
                    failed_count += 1
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    
            except Exception as e:
                failed_count += 1
                print(f"❌ Erreur : {e}")
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ DÉZIPAGE")
        print("=" * 80)
        print(f"✅ ZIP extraits : {extracted_count}")
        print(f"❌ ZIP en échec : {failed_count}")
        print(f"📄 Fichiers     : {total_files}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "zip_count": extracted_count,
            "failed_count": failed_count,
            "total_files": total_files
        }
    
    def _extract_single_zip(self, zip_filename: str) -> dict:
        """Extrait un ZIP"""
        
        zip_path = os.path.join(self.zip_dir, zip_filename)
        
        # Déterminer table
        base_name = zip_filename.replace('.zip', '')
        table_name = base_name.split('_')[0] if '_' in base_name else base_name
        
        extract_dir = os.path.join(self.extract_base_dir, table_name)
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                valid_files = [f for f in file_list 
                              if not f.startswith('__MACOSX') 
                              and not f.startswith('.')
                              and not f.endswith('/')]
                
                if not valid_files:
                    return {"status": "ERROR", "error": "No valid files"}
                
                for file in valid_files:
                    zip_ref.extract(file, extract_dir)
                
                return {
                    "status": "SUCCESS",
                    "file_count": len(valid_files),
                    "extract_dir": extract_dir,
                    "target_table": table_name
                }
                
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
    
    def _archive_zip(self, zip_filename: str):
        """Archive ZIP traité"""
        
        source = os.path.join(self.zip_dir, zip_filename)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{zip_filename.replace('.zip', '')}_{timestamp}.zip"
        dest = os.path.join(self.archive_dir, archive_name)
        
        try:
            shutil.move(source, dest)
            print(f"   📦 Archivé : {archive_name}")
        except Exception as e:
            print(f"   ⚠️  Erreur archivage : {e}")
