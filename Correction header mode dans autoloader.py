Code Complet de la Section
# Dans _process_single_table(), ligne ~250-350

# Lire Input header depuis config
if isinstance(table_config, dict):
    input_format = str(table_config.get("Input Format", "csv")).strip().lower()
    delimiter = str(table_config.get("Input delimiter", ","))
    charset = str(table_config.get("Input charset", "UTF-8")).strip()
    input_header = str(table_config.get("Input header", "")).strip().upper()
else:
    input_format = str(table_config.get("Input Format", "csv")).strip().lower()
    delimiter = str(table_config.get("Input delimiter", ","))
    charset = str(table_config.get("Input charset", "UTF-8")).strip()
    input_header = str(table_config.get("Input header", "")).strip().upper()

if charset.lower() in ["nan", "", "none"]:
    charset = "UTF-8"

# Options Auto Loader
options = {
    "cloudFiles.format": input_format,
    "cloudFiles.useNotifications": "false",
    "cloudFiles.includeExistingFiles": "true",
    "cloudFiles.schemaLocation": schema_path,
}

# Options CSV selon le mode header
if input_format in ["csv", "csv_quote", "csv_quote_ml"]:
    
    if input_header == "FIRST_LINE":
        # Mode FIRST_LINE : Ne pas utiliser le header du CSV
        options.update({
            "header": "false",
            "delimiter": delimiter,
            "encoding": charset,
            "inferSchema": "false",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "quote": '"',
            "escape": "\\"
        })
    else:
        # HEADER_USE ou défaut : Utiliser le header du CSV
        options.update({
            "header": "true",
            "delimiter": delimiter,
            "encoding": charset,
            "inferSchema": "false",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "quote": '"',
            "escape": "\\"
        })
    
    if input_format == "csv_quote_ml":
        options["multiline"] = "true"

print(f"\n🔄 Création stream Auto Loader...")
print(f"   Pattern: {table_name}_*.csv")
print(f"   Input header mode: {input_header}")

try:
    # Créer stream
    df_stream = (
        self.spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(source_path)
    )
    
    # Normalisation selon le mode
    if input_header == "HEADER_USE":
        print(f"   → Mode HEADER_USE : Normalisation en lowercase")
        
        normalized_count = 0
        for col in df_stream.columns:
            if not col.startswith("_"):
                col_lower = col.lower()
                if col != col_lower:
                    df_stream = df_stream.withColumnRenamed(col, col_lower)
                    normalized_count += 1
        
        if normalized_count > 0:
            print(f"   → {normalized_count} colonne(s) normalisée(s)")
    
    elif input_header == "FIRST_LINE":
        print(f"   → Mode FIRST_LINE : Renommage selon Excel")
        
        if not columns_config.empty:
            expected_column_names = columns_config["Column Name"].tolist()
            
            auto_cols = [c for c in df_stream.columns if c.startswith("_c")]
            auto_cols_sorted = sorted(auto_cols, key=lambda x: int(x[2:]) if x[2:].isdigit() else 999)
            
            for i, auto_col in enumerate(auto_cols_sorted):
                if i < len(expected_column_names):
                    new_name = expected_column_names[i].lower()
                    df_stream = df_stream.withColumnRenamed(auto_col, new_name)
                    print(f"      {auto_col} → {new_name}")
            
            print(f"   → {min(len(auto_cols_sorted), len(expected_column_names))} colonne(s) renommée(s)")
    
    else:
        print(f"   → Mode par défaut")
    
    # Filtrer par table
    df_stream = df_stream.filter(...)
    
except Exception as e:
    return {"status": "ERROR", "error": f"Stream failed: {e}"}
