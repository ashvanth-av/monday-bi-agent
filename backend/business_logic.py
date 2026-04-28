from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from backend.data_cleaning import get_canonical_column


def get_pipeline_analysis(df: pd.DataFrame, sector: Optional[str] = None, date_range: Optional[str] = None) -> Dict:
    if df is None or df.empty:
        return {"metric": "pipeline", "summary": "No deals data is available for pipeline analysis."}

    working_df = _apply_common_filters(df, sector=sector, date_range=date_range)
    value_column = get_canonical_column(working_df, "amount") or _find_first_column(
        working_df, ["amount", "value", "revenue", "budget", "deal"]
    )
    status_column = get_canonical_column(working_df, "status") or _find_first_column(
        working_df, ["status", "stage"]
    )

    total_value = float(working_df[value_column].fillna(0).sum()) if value_column else 0.0
    deal_count = int(len(working_df))
    open_count = (
        int(_series_contains(working_df[status_column], "open|working").sum())
        if status_column
        else 0
    )
    return {
        "metric": "pipeline",
        "deal_count": deal_count,
        "pipeline_value": total_value,
        "open_deals": open_count,
        "filtered_sector": sector,
        "date_range": date_range,
    }


def get_revenue_analysis(df: pd.DataFrame) -> Dict:
    if df is None or df.empty:
        return {"metric": "revenue", "summary": "No deals data is available for revenue analysis."}

    value_column = get_canonical_column(df, "amount") or _find_first_column(
        df, ["amount", "value", "revenue", "budget", "deal"]
    )
    status_column = get_canonical_column(df, "status") or _find_first_column(df, ["status", "stage"])
    if not value_column:
        return {"metric": "revenue", "summary": "Revenue-like columns were not found in the deals board."}

    closed_won = df
    if status_column:
        closed_won = df[_series_contains(df[status_column], "won|closed")]

    total_revenue = float(closed_won[value_column].fillna(0).sum())
    avg_revenue = float(closed_won[value_column].fillna(0).mean()) if not closed_won.empty else 0.0
    return {
        "metric": "revenue",
        "recognized_revenue_column": value_column,
        "closed_won_count": int(len(closed_won)),
        "total_revenue": total_revenue,
        "average_revenue": avg_revenue,
    }


def get_sector_performance(df: pd.DataFrame) -> Dict:
    if df is None or df.empty:
        return {"metric": "sector_performance", "summary": "No deals data is available for sector analysis."}

    sector_column = get_canonical_column(df, "sector") or _find_first_column(
        df, ["sector", "industry", "vertical", "segment"]
    )
    value_column = get_canonical_column(df, "amount") or _find_first_column(
        df, ["amount", "value", "revenue", "budget", "deal"]
    )
    if not sector_column:
        return {
            "metric": "sector_performance",
            "summary": "No sector or industry column was detected in the deals board.",
        }

    grouped = (
        df.assign(_sector=df[sector_column].fillna("unknown"))
        .groupby("_sector")
        .agg(
            deals=("item_id", "count") if "item_id" in df.columns else (sector_column, "count"),
            value=(value_column, "sum") if value_column else (sector_column, "count"),
        )
        .sort_values("value", ascending=False)
        .reset_index()
    )

    return {
        "metric": "sector_performance",
        "top_sectors": grouped.head(5).to_dict(orient="records"),
        "sector_column": sector_column,
        "value_column": value_column,
    }


def get_work_order_metrics(df: pd.DataFrame) -> Dict:
    if df is None or df.empty:
        return {"metric": "operations", "summary": "No work orders data is available for operations analysis."}

    status_column = get_canonical_column(df, "status") or _find_first_column(df, ["status", "stage"])
    due_column = get_canonical_column(df, "close_date") or _find_first_column(
        df, ["due", "end", "date", "timeline"]
    )
    amount_column = get_canonical_column(df, "amount") or _find_first_column(
        df, ["amount", "value", "revenue", "budget"]
    )

    total_orders = int(len(df))
    completed_orders = (
        int(_series_contains(df[status_column], "done|complete|closed").sum())
        if status_column
        else 0
    )
    open_orders = (
        int(_series_contains(df[status_column], "open|progress|started|working").sum())
        if status_column
        else total_orders - completed_orders
    )
    overdue_orders = 0
    if due_column:
        overdue_orders = int(
            (
                (df[due_column].notna())
                & (df[due_column] < pd.Timestamp(datetime.now()))
                & ~_series_contains(df[status_column], "done|complete|closed")
            ).sum()
        )
    total_order_value = float(df[amount_column].fillna(0).sum()) if amount_column else 0.0

    return {
        "metric": "operations",
        "total_work_orders": total_orders,
        "completed_work_orders": completed_orders,
        "open_work_orders": open_orders,
        "overdue_work_orders": overdue_orders,
        "total_order_value": total_order_value,
        "status_column": status_column,
        "due_column": due_column,
        "amount_column": amount_column,
    }


def get_operational_metrics(df: pd.DataFrame) -> Dict:
    return get_work_order_metrics(df)


def _apply_common_filters(
    df: pd.DataFrame, sector: Optional[str] = None, date_range: Optional[str] = None
) -> pd.DataFrame:
    working_df = df.copy()
    if sector:
        sector_column = get_canonical_column(working_df, "sector") or _find_first_column(
            working_df, ["sector", "industry", "vertical", "segment"]
        )
        if sector_column:
            working_df = working_df[
                working_df[sector_column].fillna("").str.contains(sector, case=False, regex=False)
            ]

    if date_range:
        date_column = get_canonical_column(working_df, "close_date") or _find_first_column(
            working_df, ["date", "close", "created", "updated", "timeline"]
        )
        if date_column:
            start, end = _resolve_date_range(date_range)
            if start and end:
                working_df = working_df[
                    (working_df[date_column] >= start) & (working_df[date_column] <= end)
                ]
    return working_df


def _resolve_date_range(time_range: str):
    now = pd.Timestamp.now().normalize()
    if time_range == "this_month":
        start = now.replace(day=1)
        end = now + pd.offsets.MonthEnd(0)
        return start, end
    if time_range == "last_month":
        previous = now - pd.offsets.MonthBegin(1)
        start = previous.replace(day=1)
        end = previous + pd.offsets.MonthEnd(0)
        return start, end
    if time_range == "this_quarter":
        quarter = ((now.month - 1) // 3) * 3 + 1
        start = pd.Timestamp(year=now.year, month=quarter, day=1)
        end = start + pd.offsets.QuarterEnd()
        return start, end
    if time_range == "last_quarter":
        current_quarter = ((now.month - 1) // 3) * 3 + 1
        current_start = pd.Timestamp(year=now.year, month=current_quarter, day=1)
        end = current_start - pd.Timedelta(days=1)
        start = current_start - pd.DateOffset(months=3)
        return start.normalize(), end.normalize()
    if time_range == "this_year":
        return pd.Timestamp(year=now.year, month=1, day=1), pd.Timestamp(
            year=now.year, month=12, day=31
        )
    return None, None


def _find_first_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    for column in df.columns:
        column_l = column.lower()
        if column_l.endswith("_raw") or column_l.endswith("_type"):
            continue
        if any(keyword in column_l for keyword in keywords):
            return column
    return None


def _series_contains(series: pd.Series, pattern: str) -> pd.Series:
    return series.fillna("").astype(str).str.contains(pattern, case=False, regex=True)
