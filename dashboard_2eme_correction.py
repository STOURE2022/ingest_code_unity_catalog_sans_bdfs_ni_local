def show_top_errors(self, limit: int = 10):
    """Top erreurs qualité (mois courant)"""
    full_table = self._get_table_name("wax_data_quality_errors")

    # ✅ Vérifier si table existe et a des données
    try:
        df_check = self.spark.table(full_table)
        if df_check.count() == 0:
            print("   ℹ️  Aucune erreur enregistrée")
            return None
    except Exception:
        print("   ℹ️  Table qualité pas encore créée")
        return None

    # ✅ Vérifier si colonnes de partitionnement existent
    columns = df_check.columns
    has_partitions = all(col in columns for col in ['yyyy', 'mm'])

    if has_partitions:
        # Requête avec filtrage par date
        query = f"""
            SELECT 
                error_message,
                COUNT(*) as occurrence_count,
                SUM(CAST(error_count AS BIGINT)) as total_error_count
            FROM {full_table}
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
              AND error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY total_error_count DESC
            LIMIT {limit}
        """
    else:
        # Requête sans filtrage par date (première exécution)
        query = f"""
            SELECT 
                error_message,
                COUNT(*) as occurrence_count,
                SUM(CAST(error_count AS BIGINT)) as total_error_count
            FROM {full_table}
            WHERE error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY total_error_count DESC
            LIMIT {limit}
        """

    try:
        return self.spark.sql(query)
    except Exception as e:
        print(f"⚠️ Erreur requête top errors : {e}")
        return None


def show_errors_by_table(self):
    """Erreurs par table (mois courant)"""
    full_table = self._get_table_name("wax_data_quality_errors")

    # ✅ Vérifier existence et données
    try:
        df_check = self.spark.table(full_table)
        if df_check.count() == 0:
            print("   ℹ️  Aucune erreur enregistrée")
            return None
        
        has_partitions = all(col in df_check.columns for col in ['yyyy', 'mm'])
    except Exception:
        print("   ℹ️  Table qualité pas encore créée")
        return None

    if has_partitions:
        where_clause = """
            WHERE yyyy = YEAR(CURRENT_DATE())
              AND mm = MONTH(CURRENT_DATE())
        """
    else:
        where_clause = ""

    query = f"""
        SELECT 
            table_name,
            COUNT(DISTINCT filename) as file_count,
            COUNT(*) as error_occurrence_count,
            SUM(CAST(error_count AS BIGINT)) as total_errors
        FROM {full_table}
        {where_clause}
        GROUP BY table_name
        ORDER BY total_errors DESC
    """

    try:
        return self.spark.sql(query)
    except Exception as e:
        print(f"⚠️ Erreur requête errors by table : {e}")
        return None

def show_errors_by_column(self, table_name: str = None, limit: int = 20):
    """Erreurs par colonne pour une table"""
    full_table = self._get_table_name("wax_data_quality_errors")

    # ✅ Vérifier existence
    try:
        df_check = self.spark.table(full_table)
        if df_check.count() == 0:
            print("   ℹ️  Aucune erreur enregistrée")
            return None
        
        has_partitions = all(col in df_check.columns for col in ['yyyy', 'mm'])
    except Exception:
        print("   ℹ️  Table qualité pas encore créée")
        return None

    where_clauses = []
    
    if has_partitions:
        where_clauses.append("yyyy = YEAR(CURRENT_DATE())")
        where_clauses.append("mm = MONTH(CURRENT_DATE())")
    
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
        SELECT 
            table_name,
            column_name,
            error_message,
            SUM(CAST(error_count AS BIGINT)) as total_errors
        FROM {full_table}
        {where_clause}
        GROUP BY table_name, column_name, error_message
        ORDER BY total_errors DESC
        LIMIT {limit}
    """

    try:
        return self.spark.sql(query)
    except Exception as e:
        print(f"⚠️ Erreur requête errors by column : {e}")
        return None
