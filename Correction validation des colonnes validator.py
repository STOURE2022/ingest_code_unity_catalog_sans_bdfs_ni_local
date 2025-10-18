Code Complet de la Section Modifiée
Voici la partie exacte à modifier dans autoloader_module.py :
# Ligne ~280-320 dans _process_single_table()

# Lire Input header depuis config
if isinstance(table_config, dict):
    input_header = str(table_config.get("Input header", "")).strip().upper()
else:
    input_header = str(table_config.get("Input header", "")).strip().upper()

print(f"   Input header mode: {input_header}")

# Créer stream Auto Loader
try:
    df_stream = (
        self.spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
    )
    
    # ✅ NORMALISATION COLONNES SI HEADER_USE
    if input_header == "HEADER_USE":
        print(f"   → Mode HEADER_USE : Normalisation en lowercase")
        
        normalized_count = 0
        for col in df_stream.columns:
            if not col.startswith("_"):  # Garder _metadata, _corrupt_record
                col_lower = col.lower()
                if col != col_lower:
                    df_stream = df_stream.withColumnRenamed(col, col_lower)
                    normalized_count += 1
        
        if normalized_count > 0:
            print(f"   → {normalized_count} colonne(s) normalisée(s)")
    
    # Filtrer par table
    df_stream = df_stream.filter(...)
    
except Exception as e:
    return {"status": "ERROR", "error": f"Stream failed: {e}"}
