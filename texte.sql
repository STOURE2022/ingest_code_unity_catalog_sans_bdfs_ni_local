-- ============================================================================
-- REQUÊTES DE VÉRIFICATION - Pipeline WAX Unity Catalog
-- À exécuter dans SQL Editor de Databricks après le pipeline
-- ============================================================================

-- ============================================================================
-- 1. VÉRIFICATION CONFIGURATION UNITY CATALOG
-- ============================================================================

-- Vérifier que le catalogue existe
SHOW CATALOGS;

-- Vérifier les schémas dans le catalogue
SHOW SCHEMAS IN abu_catalog;

-- Vérifier les volumes
SHOW VOLUMES IN abu_catalog.databricksassetbundletest;

-- Vérifier les tables dans gdp_poc_dev
SHOW TABLES IN abu_catalog.gdp_poc_dev;

-- Détail d'un volume spécifique
DESCRIBE VOLUME abu_catalog.databricksassetbundletest.externalvolumetes;


-- ============================================================================
-- 2. VÉRIFICATION PERMISSIONS
-- ============================================================================

-- Permissions sur le catalogue
SHOW GRANTS ON CATALOG abu_catalog;

-- Permissions sur le schéma fichiers
SHOW GRANTS ON SCHEMA abu_catalog.databricksassetbundletest;

-- Permissions sur le volume
SHOW GRANTS ON VOLUME abu_catalog.databricksassetbundletest.externalvolumetes;

-- Permissions sur le schéma tables
SHOW GRANTS ON SCHEMA abu_catalog.gdp_poc_dev;


-- ============================================================================
-- 3. ACCORDER LES PERMISSIONS (si nécessaire)
-- ============================================================================

-- Remplacer <votre_user> par votre email ou nom d'utilisateur

-- Permissions catalogue
GRANT USE CATALOG ON CATALOG abu_catalog TO `<votre_user>`;

-- Permissions schéma fichiers
GRANT USE SCHEMA ON SCHEMA abu_catalog.databricksassetbundletest TO `<votre_user>`;

-- Permissions volume
GRANT READ VOLUME, WRITE VOLUME
ON VOLUME abu_catalog.databricksassetbundletest.externalvolumetes
TO `<votre_user>`;

-- Permissions schéma tables
GRANT USE SCHEMA ON SCHEMA abu_catalog.gdp_poc_dev TO `<votre_user>`;
GRANT CREATE TABLE ON SCHEMA abu_catalog.gdp_poc_dev TO `<votre_user>`;
GRANT SELECT ON SCHEMA abu_catalog.gdp_poc_dev TO `<votre_user>`;
GRANT MODIFY ON SCHEMA abu_catalog.gdp_poc_dev TO `<votre_user>`;


-- ============================================================================
-- 4. VÉRIFICATION DES TABLES CRÉÉES
-- ============================================================================

-- Lister toutes les tables dans gdp_poc_dev
SHOW TABLES IN abu_catalog.gdp_poc_dev;

-- Compter le nombre de tables
SELECT COUNT(*) as nombre_tables
FROM abu_catalog.information_schema.tables
WHERE table_catalog = 'abu_catalog'
  AND table_schema = 'gdp_poc_dev';

-- Détail d'une table spécifique (remplacer TABLE_NAME)
DESCRIBE EXTENDED abu_catalog.gdp_poc_dev.TABLE_NAME_all;

-- Structure d'une table
DESCRIBE TABLE abu_catalog.gdp_poc_dev.TABLE_NAME_all;

-- Historique Delta d'une table
DESCRIBE HISTORY abu_catalog.gdp_poc_dev.TABLE_NAME_all LIMIT 20;


-- ============================================================================
-- 5. STATISTIQUES DES TABLES
-- ============================================================================

-- Statistiques pour toutes les tables
SELECT
    table_name,
    COUNT(*) as row_count
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all  -- Répéter pour chaque table
GROUP BY table_name;

-- Exemple: compter les lignes d'une table
SELECT COUNT(*) as total_rows
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all;

-- Aperçu des données
SELECT *
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all
LIMIT 10;

-- Vérifier les partitions
SELECT DISTINCT yyyy, mm, dd, COUNT(*) as row_count
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all
GROUP BY yyyy, mm, dd
ORDER BY yyyy DESC, mm DESC, dd DESC;

-- Dates min/max dans une table
SELECT
    MIN(FILE_PROCESS_DATE) as date_min,
    MAX(FILE_PROCESS_DATE) as date_max,
    COUNT(DISTINCT FILE_NAME_RECEIVED) as nb_fichiers
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all;


-- ============================================================================
-- 6. COMPARAISON _all vs _last
-- ============================================================================

-- Comparer le nombre de lignes entre _all et _last
SELECT
    'all' as table_type,
    COUNT(*) as row_count
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_all
UNION ALL
SELECT
    'last' as table_type,
    COUNT(*) as row_count
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_last;

-- Vérifier les doublons dans _last (ne devrait pas en avoir si mode DELTA_FROM_NON_HISTORIZED)
SELECT
    merge_key_column,  -- Remplacer par votre clé de merge
    COUNT(*) as count
FROM abu_catalog.gdp_poc_dev.TABLE_NAME_last
GROUP BY merge_key_column
HAVING COUNT(*) > 1;


-- ============================================================================
-- 7. LOGS D'EXÉCUTION
-- ============================================================================

-- Créer table managée pour les logs d'exécution
CREATE TABLE IF NOT EXISTS abu_catalog.gdp_poc_dev.wax_execution_logs
USING DELTA
LOCATION '/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/logs/execution';

-- Créer table managée pour les logs qualité
CREATE TABLE IF NOT EXISTS abu_catalog.gdp_poc_dev.wax_data_quality_errors
USING DELTA
LOCATION '/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/logs/quality';

-- Dernières exécutions
SELECT
    table_name,
    filename,
    status,
    row_count,
    error_count,
    duration,
    log_ts
FROM abu_catalog.gdp_poc_dev.wax_execution_logs
ORDER BY log_ts DESC
LIMIT 20;

-- Statistiques par table
SELECT
    table_name,
    COUNT(*) as nb_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as nb_success,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as nb_failed,
    SUM(row_count) as total_rows_processed,
    ROUND(AVG(duration), 2) as avg_duration_sec
FROM abu_catalog.gdp_poc_dev.wax_execution_logs
WHERE yyyy = YEAR(CURRENT_DATE())
  AND mm = MONTH(CURRENT_DATE())
GROUP BY table_name
ORDER BY nb_executions DESC;

-- Erreurs récentes
SELECT
    table_name,
    filename,
    error_message,
    log_ts
FROM abu_catalog.gdp_poc_dev.wax_execution_logs
WHERE status = 'FAILED'
  AND log_ts >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY log_ts DESC;


-- ============================================================================
-- 8. LOGS QUALITÉ
-- ============================================================================

-- Top erreurs qualité
SELECT
    error_message,
    COUNT(*) as nb_occurrences,
    SUM(CAST(error_count AS BIGINT)) as total_errors
FROM abu_catalog.gdp_poc_dev.wax_data_quality_errors
WHERE yyyy = YEAR(CURRENT_DATE())
  AND mm = MONTH(CURRENT_DATE())
GROUP BY error_message
ORDER BY total_errors DESC
LIMIT 20;

-- Erreurs par table
SELECT
    table_name,
    COUNT(DISTINCT filename) as nb_files_with_errors,
    COUNT(*) as nb_error_types,
    SUM(CAST(error_count AS BIGINT)) as total_errors
FROM abu_catalog.gdp_poc_dev.wax_data_quality_errors
WHERE yyyy = YEAR(CURRENT_DATE())
  AND mm = MONTH(CURRENT_DATE())
GROUP BY table_name
ORDER BY total_errors DESC;

-- Erreurs par colonne pour une table spécifique
SELECT
    column_name,
    error_message,
    SUM(CAST(error_count AS BIGINT)) as total_errors
FROM abu_catalog.gdp_poc_dev.wax_data_quality_errors
WHERE table_name = 'TABLE_NAME'  -- Remplacer
  AND yyyy = YEAR(CURRENT_DATE())
  AND mm = MONTH(CURRENT_DATE())
GROUP BY column_name, error_message
ORDER BY total_errors DESC;


-- ============================================================================
-- 9. MAINTENANCE DES TABLES
-- ============================================================================

-- OPTIMIZE une table (compacter les petits fichiers)
OPTIMIZE abu_catalog.gdp_poc_dev.TABLE_NAME_all;

-- OPTIMIZE avec Z-ORDER (améliore les performances de filtrage)
OPTIMIZE abu_catalog.gdp_poc_dev.TABLE_NAME_all
ZORDER BY (yyyy, mm, merge_key_column);

-- VACUUM (supprimer anciennes versions, attention: irréversible!)
-- Par défaut conserve 7 jours, ajustez si besoin
VACUUM abu_catalog.gdp_poc_dev.TABLE_NAME_all RETAIN 168 HOURS;

-- ANALYZE (calculer statistiques pour l'optimiseur)
ANALYZE TABLE abu_catalog.gdp_poc_dev.TABLE_NAME_all COMPUTE STATISTICS;

-- ANALYZE pour toutes les colonnes
ANALYZE TABLE abu_catalog.gdp_poc_dev.TABLE_NAME_all
COMPUTE STATISTICS FOR ALL COLUMNS;

-- Voir les statistiques de table
DESCRIBE DETAIL abu_catalog.gdp_poc_dev.TABLE_NAME_all;


-- ============================================================================
-- 10. NETTOYAGE (si besoin)
-- ============================================================================

-- Supprimer une table
DROP TABLE IF EXISTS abu_catalog.gdp_poc_dev.TABLE_NAME_all;

-- Supprimer toutes les tables d'un préfixe
-- ATTENTION: Vérifier avant d'exécuter!
SHOW TABLES IN abu_catalog.gdp_poc_dev;
-- Puis supprimer manuellement ou avec script

-- Vider le dossier de logs (via Python/notebook)
-- dbutils.fs.rm("/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/logs/execution", recurse=True)
-- dbutils.fs.rm("/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/logs/quality", recurse=True)


-- ============================================================================
-- 11. LINEAGE UNITY CATALOG (si disponible)
-- ============================================================================

-- Voir le lineage d'une table
SELECT *
FROM system.access.table_lineage
WHERE table_catalog = 'abu_catalog'
  AND table_schema = 'gdp_poc_dev'
  AND table_name = 'TABLE_NAME_all'
ORDER BY created_at DESC;

-- Audit des accès
SELECT *
FROM system.access.audit
WHERE request_params.table_full_name = 'abu_catalog.gdp_poc_dev.TABLE_NAME_all'
ORDER BY event_time DESC
LIMIT 100;


-- ============================================================================
-- 12. MONITORING CONTINU
-- ============================================================================

-- Tendance journalière des exécutions
SELECT
    CONCAT(yyyy, '-', LPAD(mm, 2, '0'), '-', LPAD(dd, 2, '0')) as date,
    COUNT(*) as nb_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as nb_success,
    SUM(row_count) as total_rows
FROM abu_catalog.gdp_poc_dev.wax_execution_logs
WHERE log_ts >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY yyyy, mm, dd
ORDER BY yyyy DESC, mm DESC, dd DESC;

-- Tables non traitées aujourd'hui
SELECT DISTINCT table_name
FROM abu_catalog.information_schema.tables
WHERE table_catalog = 'abu_catalog'
  AND table_schema = 'gdp_poc_dev'
  AND table_name LIKE '%_all'
  AND table_name NOT IN (
    SELECT DISTINCT table_name
    FROM abu_catalog.gdp_poc_dev.wax_execution_logs
    WHERE DATE(log_ts) = CURRENT_DATE()
  );

-- Durée moyenne par table (pour détecter ralentissements)
SELECT
    table_name,
    ROUND(AVG(duration), 2) as avg_duration_sec,
    ROUND(MAX(duration), 2) as max_duration_sec,
    COUNT(*) as nb_executions
FROM abu_catalog.gdp_poc_dev.wax_execution_logs
WHERE log_ts >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY table_name
ORDER BY avg_duration_sec DESC;