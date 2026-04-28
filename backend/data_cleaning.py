import re
from typing import Dict, List, Optional

import pandas as pd


DATE_HINTS = ("date", "time", "created", "updated", "close", "due", "timeline")
NUMERIC_HINTS = ("amount", "value", "revenue", "budget", "price", "total")
COLUMN_ALIASES = {
    "amount": [
        "amount",
        "deal value",
        "masked deal value",
        "revenue",
        "value",
        "billed value",
        "collected amount",
    ],
    "sector": ["sector", "industry", "vertical", "segment", "sector/service"],
    "status": ["status", "stage", "deal status", "deal stage", "execution status", "wo status"],
    "close_date": ["close date", "tentative close date", "probable end date", "actual billing month"],
    "created_date": ["created date", "created at"],
    "record_name": ["deal name", "deal name masked", "record name"],
}


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    cleaned = df.copy()
    cleaned.columns = [_normalize_column_name(column) for column in cleaned.columns]
    raw_missing_ratio = cleaned.replace(r"^\s*$", pd.NA, regex=True).isna().mean().mean()

    for column in cleaned.columns:
        if cleaned[column].dtype == object:
            cleaned[column] = cleaned[column].apply(
                lambda value: value.strip().lower() if isinstance(value, str) else value
            )
            cleaned[column] = cleaned[column].replace({"": pd.NA, "none": pd.NA, "null": pd.NA})

    for column in _match_columns(cleaned.columns.tolist(), DATE_HINTS):
        candidates = cleaned[column]
        if pd.api.types.is_object_dtype(candidates):
            candidates = candidates.where(candidates.astype(str).str.contains(r"\d", regex=True), pd.NA)
        cleaned[column] = pd.to_datetime(candidates, errors="coerce", utc=False)

    for column in _match_columns(cleaned.columns.tolist(), NUMERIC_HINTS):
        cleaned[column] = (
            cleaned[column]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("₹", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.strip()
        )
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    column_mapping = detect_column_mapping(cleaned)
    for canonical_name, source_column in column_mapping.items():
        if source_column and canonical_name not in cleaned.columns:
            cleaned[canonical_name] = cleaned[source_column]

    for column in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].fillna("unknown")

    cleaned.attrs["missing_ratio"] = float(raw_missing_ratio)
    cleaned.attrs["column_mapping"] = column_mapping
    return cleaned


def data_quality_warnings(df: pd.DataFrame, label: str) -> List[str]:
    if df is None or df.empty:
        return [f"{label} dataset is empty."]

    missing_ratio = float(df.attrs.get("missing_ratio", df.replace("unknown", pd.NA).isna().mean().mean()))
    warnings = []
    if missing_ratio > 0:
        warnings.append(f"{label} data has {missing_ratio:.1%} missing values overall.")
    if missing_ratio > 0.1:
        warnings.append(f"{label} data quality is weak and could affect insight accuracy.")
    return warnings


def detect_column_mapping(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for canonical_name, aliases in COLUMN_ALIASES.items():
        mapping[canonical_name] = _find_matching_column(df.columns.tolist(), aliases)
    return mapping


def get_canonical_column(df: pd.DataFrame, canonical_name: str) -> Optional[str]:
    mapping = df.attrs.get("column_mapping") or detect_column_mapping(df)
    return mapping.get(canonical_name)


def _find_matching_column(columns: List[str], aliases: List[str]) -> Optional[str]:
    normalized_aliases = [_normalize_column_name(alias) for alias in aliases]
    for column in columns:
        normalized_column = _normalize_column_name(column)
        if normalized_column in normalized_aliases:
            return column
    for column in columns:
        normalized_column = _normalize_column_name(column)
        if any(alias in normalized_column for alias in normalized_aliases):
            return column
    return None


def _match_columns(columns: List[str], hints) -> List[str]:
    matches = []
    for column in columns:
        column_l = column.lower()
        if column_l.endswith("_raw") or column_l.endswith("_type"):
            continue
        if any(hint in column_l for hint in hints):
            matches.append(column)
    return matches


def _normalize_column_name(value: str) -> str:
    normalized = str(value).strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")
