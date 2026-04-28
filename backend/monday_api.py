import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


MONDAY_API_URL = "https://api.monday.com/v2"


class MondayAPIError(Exception):
    pass


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


class MondayClient:
    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.getenv("MONDAY_API_KEY")
        if not self.api_key:
            raise MondayAPIError("MONDAY_API_KEY is not configured.")

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    def run_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        response = requests.post(
            MONDAY_API_URL,
            json={"query": query, "variables": variables or {}},
            headers=self.headers,
            timeout=60,
        )

        if response.status_code >= 400:
            raise MondayAPIError(
                f"Monday API request failed with status {response.status_code}: {response.text}"
            )

        payload = response.json()
        if payload.get("errors"):
            raise MondayAPIError(f"Monday API returned errors: {payload['errors']}")
        return payload["data"]

    def fetch_board_dataframe(self, board_id: int, board_label: str) -> Tuple[pd.DataFrame, Dict]:
        query = """
        query ($board_id: ID!) {
          boards(ids: [$board_id]) {
            id
            name
            columns {
              id
              title
              type
            }
            items_page(limit: 500) {
              items {
                id
                name
                created_at
                updated_at
                group {
                  id
                  title
                }
                column_values {
                  id
                  text
                  type
                  value
                }
              }
            }
          }
        }
        """
        data = self.run_query(query, {"board_id": str(board_id)})
        boards = data.get("boards", [])
        if not boards:
            raise MondayAPIError(f"No board found for board_id={board_id}")

        board = boards[0]
        column_map = {col["id"]: col["title"] for col in board.get("columns", [])}
        records: List[Dict] = []

        for item in board.get("items_page", {}).get("items", []):
            record = {
                "board_id": board["id"],
                "board_name": board.get("name") or board_label,
                "record_name": item.get("name"),
                "item_id": item.get("id"),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "group_title": (item.get("group") or {}).get("title"),
            }

            for column_value in item.get("column_values", []):
                column_id = column_value.get("id")
                column_title = column_map.get(column_id, column_id or "unknown")
                safe_key = _slugify(column_title) or _slugify(column_id or "unknown")
                record[safe_key] = column_value.get("text")
                record[f"{safe_key}_raw"] = column_value.get("value")
                record[f"{safe_key}_type"] = column_value.get("type")

            records.append(record)

        return pd.DataFrame(records), {
            "board_id": board["id"],
            "board_name": board.get("name") or board_label,
            "row_count": len(records),
            "columns": list(column_map.values()),
        }


def load_board_config() -> Dict[str, Dict[str, Optional[str]]]:
    return {
        "deals": {
            "id": os.getenv("MONDAY_DEALS_BOARD_ID"),
            "label": "Deals",
        },
        "work_orders": {
            "id": os.getenv("MONDAY_WORK_ORDERS_BOARD_ID"),
            "label": "Work Orders",
        },
    }


def _build_demo_data() -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict], List[str]]:
    deals_df = pd.DataFrame(
        [
            {
                "board_id": "demo-deals",
                "board_name": "Deals",
                "record_name": "Acme renewal",
                "item_id": "1",
                "created_at": "2026-04-01",
                "updated_at": "2026-04-11",
                "sector": "healthcare",
                "status": "open",
                "amount": "250000",
                "close_date": "2026-05-20",
            },
            {
                "board_id": "demo-deals",
                "board_name": "Deals",
                "record_name": "Zenith expansion",
                "item_id": "2",
                "created_at": "2026-03-18",
                "updated_at": "2026-04-05",
                "sector": "manufacturing",
                "status": "won",
                "amount": "420000",
                "close_date": "2026-04-02",
            },
            {
                "board_id": "demo-deals",
                "board_name": "Deals",
                "record_name": "Nova pilot",
                "item_id": "3",
                "created_at": "2026-04-06",
                "updated_at": "2026-04-17",
                "sector": "healthcare",
                "status": "open",
                "amount": None,
                "close_date": "2026-06-10",
            },
        ]
    )
    work_orders_df = pd.DataFrame(
        [
            {
                "board_id": "demo-work-orders",
                "board_name": "Work Orders",
                "record_name": "Install batch A",
                "item_id": "11",
                "created_at": "2026-04-02",
                "updated_at": "2026-04-10",
                "status": "done",
                "due_date": "2026-04-09",
                "sector": "healthcare",
            },
            {
                "board_id": "demo-work-orders",
                "board_name": "Work Orders",
                "record_name": "Repair queue B",
                "item_id": "12",
                "created_at": "2026-04-08",
                "updated_at": "2026-04-18",
                "status": "working on it",
                "due_date": "2026-04-15",
                "sector": "manufacturing",
            },
            {
                "board_id": "demo-work-orders",
                "board_name": "Work Orders",
                "record_name": "Audit follow-up",
                "item_id": "13",
                "created_at": "2026-04-12",
                "updated_at": "2026-04-20",
                "status": "stuck",
                "due_date": "2026-04-14",
                "sector": "healthcare",
            },
        ]
    )
    metadata = {
        "deals": {
            "board_id": "demo-deals",
            "board_name": "Deals (demo)",
            "row_count": len(deals_df),
            "source": "demo",
        },
        "work_orders": {
            "board_id": "demo-work-orders",
            "board_name": "Work Orders (demo)",
            "row_count": len(work_orders_df),
            "source": "demo",
        },
    }
    warnings = [
        "Running in demo mode because Monday.com credentials or board IDs are not configured.",
    ]
    return {"deals": deals_df, "work_orders": work_orders_df}, metadata, warnings


def fetch_all_boards() -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict], List[str]]:
    config = load_board_config()
    if not os.getenv("MONDAY_API_KEY") or not config["deals"].get("id") or not config["work_orders"].get("id"):
        return _build_demo_data()

    client = MondayClient()
    dataframes: Dict[str, pd.DataFrame] = {}
    metadata: Dict[str, Dict] = {}
    warnings: List[str] = []

    for key, board in config.items():
        board_id = board.get("id")
        if not board_id:
            warnings.append(f"{board['label']} board ID is missing; set the matching environment variable.")
            dataframes[key] = pd.DataFrame()
            metadata[key] = {"board_name": board["label"], "row_count": 0}
            continue

        try:
            df, info = client.fetch_board_dataframe(int(board_id), board["label"])
            info["source"] = "monday"
            dataframes[key] = df
            metadata[key] = info
        except Exception as exc:
            warnings.append(f"Could not load {board['label']} data: {exc}")
            dataframes[key] = pd.DataFrame()
            metadata[key] = {"board_name": board["label"], "row_count": 0, "source": "error"}

    return dataframes, metadata, warnings
