# ========== VALIDATION COLONNES ==========

print(f"\n📋 Validation présence colonnes...")

validation_result = validator.validate_columns_presence(
    df_raw, 
    expected_cols,  # Déjà en lowercase depuis l'Excel
    source_table, 
    filename_current
)

missing_cols = validation_result["missing_columns"]

if missing_cols:
    print(f"   → Ajout automatique de {len(missing_cols)} colonne(s) avec NULL")
    
    # Ajouter les colonnes manquantes
    for col in missing_cols:
        df_raw = df_raw.withColumn(col, F.lit(None).cast("string"))
    
    print(f"✅ Colonnes ajoutées")
    
    # Logger comme info (traçabilité)
    if validation_result["df_errors"] is not None:
        logger_manager.write_quality_errors(
            validation_result["df_errors"], 
            source_table, 
            zone=output_zone
        )
else:
    print(f"✅ Toutes les colonnes présentes")

# Continuer le traitement
