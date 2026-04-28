from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEALS_FILE_CANDIDATES = [
    "Deal_funnel_Data.xlsx",
    "Deal_funnel_Data (1).xlsx",
]
WORK_ORDERS_FILE_CANDIDATES = [
    "Work_Order_Tracker_Data.xlsx",
    "Work_Order_Tracker_Data (1).xlsx",
]


def load_local_data() -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict], List[str]]:
    warnings: List[str] = []

    deals_path = _resolve_candidate_path(DEALS_FILE_CANDIDATES)
    work_orders_path = _resolve_candidate_path(WORK_ORDERS_FILE_CANDIDATES)

    if not deals_path or not work_orders_path:
        missing = []
        if not deals_path:
            missing.append("deals workbook")
        if not work_orders_path:
            missing.append("work orders workbook")
        return _build_demo_data(
            [f"Falling back to demo mode because the following files were not found: {', '.join(missing)}."]
        )

    try:
        deals_df = pd.read_excel(deals_path)
    except Exception as exc:
        return _build_demo_data([f"Could not read deals workbook `{deals_path.name}`: {exc}"])

    try:
        work_orders_df = _read_work_orders_excel(work_orders_path)
    except Exception as exc:
        return _build_demo_data([f"Could not read work orders workbook `{work_orders_path.name}`: {exc}"])

    metadata = {
        "deals": {
            "board_name": "Deals (local file)",
            "row_count": len(deals_df),
            "source": "local_excel",
            "file_name": deals_path.name,
        },
        "work_orders": {
            "board_name": "Work Orders (local file)",
            "row_count": len(work_orders_df),
            "source": "local_excel",
            "file_name": work_orders_path.name,
        },
    }
    warnings.append("Running in local data mode using uploaded Excel files.")
    return {"deals": deals_df, "work_orders": work_orders_df}, metadata, warnings


def _resolve_candidate_path(candidates: List[str]) -> Optional[Path]:
    for candidate in candidates:
        path = PROJECT_ROOT / candidate
        if path.exists():
            return path
    return None


def _read_work_orders_excel(path: Path) -> pd.DataFrame:
    preview = pd.read_excel(path, header=None, nrows=8)
    header_row = _detect_header_row(preview)
    return pd.read_excel(path, header=header_row)


def _detect_header_row(preview_df: pd.DataFrame) -> int:
    best_row = 0
    best_score = -1

    for idx in range(len(preview_df)):
        values = [str(value).strip().lower() for value in preview_df.iloc[idx].tolist() if pd.notna(value)]
        if not values:
            continue

        signal_hits = sum(
            1
            for value in values
            if any(token in value for token in ["status", "deal", "sector", "amount", "billing", "invoice"])
        )
        score = len(values) + signal_hits * 3
        if score > best_score:
            best_score = score
            best_row = idx

    return best_row


def _build_demo_data(extra_warnings: Optional[List[str]] = None):
    deals_df = pd.DataFrame(
        [
            {
                "deal_name": "acme renewal",
                "deal_status": "open",
                "deal_stage": "proposal",
                "masked_deal_value": 250000,
                "sector_service": "healthcare",
                "close_date": "2026-05-20",
                "created_date": "2026-04-01",
            },
            {
                "deal_name": "zenith expansion",
                "deal_status": "won",
                "deal_stage": "closed won",
                "masked_deal_value": 420000,
                "sector_service": "manufacturing",
                "close_date": "2026-04-02",
                "created_date": "2026-03-18",
            },
            {
                "deal_name": "nova pilot",
                "deal_status": "open",
                "deal_stage": "qualification",
                "masked_deal_value": None,
                "sector_service": "healthcare",
                "close_date": "2026-06-10",
                "created_date": "2026-04-06",
            },
        ]
    )
    work_orders_df = pd.DataFrame(
        [
            {
                "deal_name_masked": "install batch a",
                "execution_status": "completed",
                "wo_status_billed": "closed",
                "sector": "healthcare",
                "probable_end_date": "2026-04-09",
                "amount_in_rupees_excl_of_gst_masked": 264398.08,
            },
            {
                "deal_name_masked": "repair queue b",
                "execution_status": "working on it",
                "wo_status_billed": "open",
                "sector": "manufacturing",
                "probable_end_date": "2026-04-15",
                "amount_in_rupees_excl_of_gst_masked": 154150,
            },
            {
                "deal_name_masked": "audit follow up",
                "execution_status": "stuck",
                "wo_status_billed": "open",
                "sector": "healthcare",
                "probable_end_date": "2026-04-14",
                "amount_in_rupees_excl_of_gst_masked": 184980,
            },
        ]
    )
    metadata = {
        "deals": {"board_name": "Deals (demo)", "row_count": len(deals_df), "source": "demo"},
        "work_orders": {
            "board_name": "Work Orders (demo)",
            "row_count": len(work_orders_df),
            "source": "demo",
        },
    }
    warnings = ["Running in demo mode because local Excel files are unavailable."]
    if extra_warnings:
        warnings.extend(extra_warnings)
    return {"deals": deals_df, "work_orders": work_orders_df}, metadata, warnings
