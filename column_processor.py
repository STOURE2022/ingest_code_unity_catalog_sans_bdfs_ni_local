"""
column_processor.py
Traitement et typage des colonnes avec gestion des erreurs
ICT_DRIVEN, REJECT, LOG_ONLY
"""

from functools import reduce
import operator
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, TimestampType, StructType, StructField,
    StringType, IntegerType
)
from .utils import parse_bool, parse_tolerance, TYPE_MAPPING


class ColumnProcessor:
    """Processeur de colonnes avec typage et validation"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.ERROR_SCHEMA = self._build_error_schema()

    def _build_error_schema(self):
        """Schéma unifié pour erreurs"""
        return StructType([
            StructField("table_name", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("raw_value", StringType(), True),
            StructField("error_count", IntegerType(), True)
        ])

    def parse_date_with_logs(self, df: DataFrame, cname: str, patterns: list,
                             table_name: str, filename: str, default_date=None) -> tuple:
        """Parse dates avec logging"""
        raw_col = F.col(cname)
        col_expr = F.when(F.length(F.trim(raw_col)) == 0, F.lit(None)).otherwise(raw_col)

        ts_col = None
        for p in patterns:
            cand = F.expr(f"try_to_timestamp({cname}, '{p}')")
            ts_col = cand if ts_col is None else F.coalesce(ts_col, cand)

        parsed = F.to_date(ts_col)
        parsed_with_default = F.when(parsed.isNull(), F.lit(default_date)).otherwise(parsed)
        df_parsed = df.withColumn(cname, parsed_with_default)

        errs = (
            df.withColumn("line_id", F.monotonically_increasing_id() + 1)
            .select(
                F.lit(table_name).alias("table_name"),
                F.lit(filename).alias("filename"),
                F.lit(cname).alias("column_name"),
                F.when(parsed.isNull() & (F.trim(raw_col) == ""), F.lit("EMPTY_DATE"))
                .when(parsed.isNull() & (F.trim(raw_col) != ""), F.lit("INVALID_DATE"))
                .otherwise(F.lit(None)).alias("error_message"),
                raw_col.cast("string").alias("raw_value"),
                F.lit(1).alias("error_count")
            )
            .where(F.col("error_message").isNotNull())
            .limit(1000)
        )

        return df_parsed, errs

    def process_columns(self, df: DataFrame, column_defs, table_name: str,
                        filename: str, total_rows: int) -> tuple:
        """
        Traite toutes les colonnes avec typage et gestion d'erreurs

        Returns:
            (df_processed, all_errors, invalid_flags)
        """
        print(f"🔧 Typage colonnes pour {table_name}...")

        invalid_flags = []
        all_column_errors = []

        for _, crow in column_defs.iterrows():
            cname = crow["Column Name"]
            if cname not in df.columns:
                continue

            # Configuration colonne
            stype_str = str(crow.get("Field type", "STRING")).strip().upper()
            tr_type = str(crow.get("Transformation Type", "")).strip().lower()
            tr_patt = str(crow.get("Transformation pattern", "")).strip()
            regex_repl = str(crow.get("Regex replacement", "")).strip()
            is_nullable = parse_bool(crow.get("Is Nullable", "True"), True)
            err_action = str(crow.get("Error action", "ICT_DRIVEN")).strip().upper()
            if err_action in ["", "NAN", "NONE", "NULL"]:
                err_action = "ICT_DRIVEN"

            default_inv = str(crow.get("Default when invalid", "")).strip()

            # Transformations texte
            if tr_type == "uppercase":
                df = df.withColumn(cname, F.upper(F.col(cname)))
            elif tr_type == "lowercase":
                df = df.withColumn(cname, F.lower(F.col(cname)))
            elif tr_type == "regex" and tr_patt:
                df = df.withColumn(
                    cname,
                    F.regexp_replace(F.col(cname), tr_patt, regex_repl if regex_repl else "")
                )

            # Typage selon type
            stype = self._get_spark_type(stype_str)

            if isinstance(stype, (DateType, TimestampType)):
                # Dates
                patterns = []
                if tr_patt:
                    patterns.append(tr_patt)
                for p in self.config.date_patterns:
                    if p not in patterns:
                        patterns.append(p)

                df, errs = self.parse_date_with_logs(
                    df, cname, patterns, table_name, filename,
                    default_date=default_inv if default_inv else None
                )

                if not errs.rdd.isEmpty():
                    all_column_errors.append(errs)

            else:
                # Types numériques et autres
                df, col_errors, flag_col = self._process_numeric_column(
                    df, cname, stype_str, is_nullable, err_action,
                    table_name, filename, total_rows, crow
                )

                if col_errors:
                    all_column_errors.extend(col_errors)
                if flag_col:
                    invalid_flags.append(flag_col)

        return df, all_column_errors, invalid_flags

    def _process_numeric_column(self, df: DataFrame, cname: str, stype_str: str,
                                is_nullable: bool, err_action: str, table_name: str,
                                filename: str, total_rows: int, col_config) -> tuple:
        """
        Traite une colonne numérique avec gestion REJECT/ICT_DRIVEN/LOG_ONLY

        Returns:
            (df_updated, list_of_errors, flag_column_name)
        """
        # Remplacer virgules par points
        df = df.withColumn(cname, F.regexp_replace(F.col(cname), ",", "."))

        # Sauvegarder valeur originale
        df = df.withColumn(f"{cname}_original", F.col(cname))

        # Nettoyer (vides → NULL)
        df = df.withColumn(
            cname,
            F.when(
                (F.col(cname).isNull()) |
                (F.trim(F.col(cname)) == ""),
                F.lit(None)
            ).otherwise(F.col(cname))
        )

        # Cast sécurisé
        df = df.withColumn(
            f"{cname}_cast",
            F.expr(f"try_cast({cname} as {stype_str})")
        )

        # Identifier invalides
        invalid_cond = (F.col(f"{cname}_cast").isNull()) & \
                       (F.col(f"{cname}_original").isNotNull()) & \
                       (F.trim(F.col(f"{cname}_original")) != "")

        invalid_count = df.filter(invalid_cond).count()

        # Appliquer cast
        df = df.withColumn(
            cname,
            F.when(F.col(f"{cname}_cast").isNotNull(), F.col(f"{cname}_cast"))
            .otherwise(F.lit(None))
        ).drop(f"{cname}_cast")

        errors = []
        flag_col = None

        # Gestion erreurs
        if not is_nullable and invalid_count > 0:
            tolerance = parse_tolerance(
                col_config.get("Rejected line per file tolerance", "10%"),
                total_rows
            )

            if err_action == "REJECT":
                print(f"   🗑️ REJECT: {invalid_count} lignes pour {cname}")

                errs = df.filter(invalid_cond).limit(1000).select(
                    F.lit(table_name).alias("table_name"),
                    F.lit(filename).alias("filename"),
                    F.lit(cname).alias("column_name"),
                    F.lit("REJECT").alias("error_message"),
                    F.col(f"{cname}_original").cast("string").alias("raw_value"),
                    F.lit(invalid_count).alias("error_count")
                )
                errors.append(errs)

                # Supprimer lignes invalides
                df = df.filter(~invalid_cond)

            elif err_action == "ICT_DRIVEN":
                flag_col = f"{cname}_invalid"
                df = df.withColumn(
                    flag_col,
                    F.when(invalid_cond, F.lit(1)).otherwise(F.lit(0))
                )

                if total_rows > 0 and (invalid_count / float(total_rows)) > tolerance:
                    print(f"   ❌ ICT_DRIVEN ABORT: {cname}")
                    errs_summary = self.spark.createDataFrame(
                        [(table_name, filename, cname,
                          "ICT_DRIVEN_ABORT", None, invalid_count)],
                        self.ERROR_SCHEMA
                    )
                    errors.append(errs_summary)
                    # Vider le DataFrame
                    df = self.spark.createDataFrame([], df.schema)

                elif invalid_count > 0:
                    errs_detailed = df.filter(invalid_cond).limit(1000).withColumn(
                        "line_id", F.monotonically_increasing_id()
                    ).select(
                        F.lit(table_name).alias("table_name"),
                        F.lit(filename).alias("filename"),
                        F.lit(cname).alias("column_name"),
                        F.lit("ICT_DRIVEN").alias("error_message"),
                        F.col(f"{cname}_original").cast("string").alias("raw_value"),
                        F.lit(1).alias("error_count")
                    )
                    errors.append(errs_detailed)

            elif err_action == "LOG_ONLY":
                print(f"   ⚠️ LOG_ONLY: {invalid_count} erreurs sur {cname}")
                errs = self.spark.createDataFrame(
                    [(table_name, filename, cname,
                      "LOG_ONLY", None, invalid_count)],
                    self.ERROR_SCHEMA
                )
                errors.append(errs)

        # Nettoyer colonne originale
        df = df.drop(f"{cname}_original")

        return df, errors, flag_col

    def reject_invalid_lines(self, df: DataFrame, invalid_flags: list,
                             table_name: str, filename: str) -> tuple:
        """
        Rejette les lignes avec trop d'erreurs ICT_DRIVEN

        Returns:
            (df_valid, errors_for_rejected_lines)
        """
        if not invalid_flags:
            return df, []

        # CORRECTION DU BUG: utiliser reduce au lieu de sum()
        # Le problème était que sum([Column, Column]) essayait de faire 0 + Column
        # ce qui cause une erreur de type
        df = df.withColumn(
            "invalid_column_count",
            reduce(operator.add, [F.col(c) for c in invalid_flags])
        )

        # Seuil : max 10% des colonnes invalides
        max_invalid_per_line = max(1, int(len(invalid_flags) * 0.1))

        df_valid = df.filter(F.col("invalid_column_count") <= max_invalid_per_line)
        df_invalid = df.filter(F.col("invalid_column_count") > max_invalid_per_line)

        errors = []
        if not df_invalid.rdd.isEmpty():
            invalid_count = df_invalid.count()
            print(f"   🗑️ {invalid_count} lignes rejetées (trop d'erreurs)")

            err_lines = df_invalid.limit(1000).select(
                F.lit(table_name).alias("table_name"),
                F.lit(filename).alias("filename"),
                F.lit("MULTIPLE_COLUMNS").alias("column_name"),
                F.lit("ICT_DRIVEN_LINE_REJECT").alias("error_message"),
                F.lit(None).cast("string").alias("raw_value"),
                F.lit(1).alias("error_count")
            )
            errors.append(err_lines)

        # Nettoyer flags
        df_valid = df_valid.drop("invalid_column_count", *invalid_flags)

        return df_valid, errors

    def _get_spark_type(self, type_str: str):
        """Convertit type string → Spark type"""
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, FloatType, DoubleType,
            BooleanType, DateType, TimestampType
        )

        mapping = {
            "STRING": StringType(),
            "INTEGER": IntegerType(),
            "INT": IntegerType(),
            "LONG": LongType(),
            "FLOAT": FloatType(),
            "DOUBLE": DoubleType(),
            "BOOLEAN": BooleanType(),
            "DATE": DateType(),
            "TIMESTAMP": TimestampType()
        }

        return mapping.get(type_str, StringType())