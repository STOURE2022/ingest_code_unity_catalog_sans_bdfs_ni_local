"""
utils.py
Fonctions utilitaires pour le pipeline WAX
"""

import re
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
)

# ==================== TYPES SPARK ====================

TYPE_MAPPING = {
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


def spark_type_from_config(row):
    """Convertit type Excel → Spark Type"""
    t = str(row.get("Field type", "STRING")).strip().upper()

    if t in TYPE_MAPPING:
        return TYPE_MAPPING[t]

    if t == "DECIMAL":
        prec = int(row.get("Decimal precision", 38) or 38)
        scale = int(row.get("Decimal scale", 18) or 18)
        return DecimalType(prec, scale)

    return StringType()


def build_schema_from_config(column_defs) -> StructType:
    """Construit StructType depuis configuration Excel"""
    import pandas as pd

    fields = []
    for _, row in column_defs.iterrows():
        field_name = row["Column Name"]
        field_type = spark_type_from_config(row)
        is_nullable = parse_bool(row.get("Is Nullable", True), True)
        fields.append(StructField(field_name, field_type, is_nullable))

    return StructType(fields)


# ==================== PARSING ====================

def parse_bool(x, default=False):
    """Parse booléen depuis Excel"""
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


def normalize_delimiter(raw) -> str:
    """Normalise délimiteur (1 char)"""
    if raw is None or str(raw).strip() == "":
        return ","
    s = str(raw).strip()
    if len(s) == 1:
        return s
    raise ValueError(f"Délimiteur '{raw}' invalide")


def parse_header_mode(x) -> tuple:
    """
    Parse mode header
    Returns: (user_header: bool, first_line_only: bool)
    """
    if x is None:
        return False, False
    s = str(x).strip().upper()
    if s == "HEADER USE":
        return True, True
    if s == "FIRST LINE":
        return True, False
    return False, False


def parse_tolerance(raw, total_rows: int, default=0.0) -> float:
    """Parse tolérance (% ou absolu)"""
    if raw is None or str(raw).strip().lower() in ["", "nan", "n/a", "none"]:
        return default
    s = str(raw).strip().lower().replace(",", ".").replace("%", "").replace(" ", "")
    m = re.search(r"^(\d+(?:\.\d+)?)%?$", s)
    if not m:
        return default
    val = float(m.group(1))
    if "%" in str(raw):
        return val / 100.0
    if total_rows <= 0:
        return 0.0
    return val / total_rows


# ==================== DATAFRAME UTILS ====================

def deduplicate_columns(df: DataFrame) -> DataFrame:
    """Supprime colonnes dupliquées (case-insensitive)"""
    seen, cols = set(), []
    for c in df.columns:
        c_lower = c.lower()
        if c_lower not in seen:
            cols.append(c)
            seen.add(c_lower)
    return df.select(*cols)


def safe_count(df: DataFrame) -> int:
    """Count sécurisé"""
    try:
        return df.count()
    except Exception:
        return 0


# ==================== FILENAME UTILS ====================

def extract_parts_from_filename(fname: str) -> dict:
    """Extrait yyyy/mm/dd depuis nom fichier"""
    base = os.path.basename(fname)
    m = re.search(r"(?P<yyyy>\d{4})[-_]?(?P<mm>\d{2})[-_]?(?P<dd>\d{2})", base)
    if m:
        parts = {}
        if m.group("yyyy"):
            parts["yyyy"] = int(m.group("yyyy"))
        if m.group("mm"):
            parts["mm"] = int(m.group("mm"))
        if m.group("dd"):
            parts["dd"] = int(m.group("dd"))
        return parts
    return {}


def build_regex_pattern(filename_pattern: str) -> tuple:
    """
    Construit patterns regex pour matching fichiers
    Returns: (rx_with_time, rx_without_time)
    """
    replacements = [
        ("<yyyy>", r"\d{4}"),
        ("<mm>", r"\d{2}"),
        ("<dd>", r"\d{2}"),
        ("<hhmmss>", r"\d{6}")
    ]

    rx_with_time = filename_pattern
    for placeholder, regex in replacements:
        rx_with_time = rx_with_time.replace(placeholder, regex)
    rx_with_time = rx_with_time.replace(".", r"\.")
    rx_with_time = f"^{rx_with_time}$"

    rx_without_time = filename_pattern
    for placeholder, regex in replacements[:-1]:
        rx_without_time = rx_without_time.replace(placeholder, regex)
    rx_without_time = rx_without_time.replace("_<hhmmss>", "").replace("<hhmmss>", "")
    rx_without_time = rx_without_time.replace(".", r"\.")
    rx_without_time = f"^{rx_without_time}$"

    return rx_with_time, rx_without_time


# ==================== EXPORTS ====================

__all__ = [
    'TYPE_MAPPING',
    'spark_type_from_config',
    'build_schema_from_config',
    'parse_bool',
    'normalize_delimiter',
    'parse_header_mode',
    'parse_tolerance',
    'deduplicate_columns',
    'safe_count',
    'extract_parts_from_filename',
    'build_regex_pattern'
]