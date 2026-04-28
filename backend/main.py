import os
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.business_logic import (
    get_pipeline_analysis,
    get_revenue_analysis,
    get_sector_performance,
    get_work_order_metrics,
)
from backend.data_cleaning import clean_data, data_quality_warnings
from backend.data_loader import load_local_data
from backend.insights import generate_insight, generate_leadership_summary
from backend.query_parser import parse_query


load_dotenv()

app = FastAPI(title="Monday.com BI AI Agent", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    query: str


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/chat")
def chat(request: ChatRequest) -> Dict[str, Any]:
    try:
        board_data, board_metadata, fetch_warnings = load_local_data()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected backend error: {exc}") from exc

    deals_df = clean_data(board_data.get("deals"))
    work_orders_df = clean_data(board_data.get("work_orders"))
    parsed = parse_query(request.query)

    if parsed["intent"] == "revenue":
        result = get_revenue_analysis(deals_df)
        relevant_df = deals_df
    elif parsed["intent"] == "sector_performance":
        result = get_sector_performance(deals_df)
        relevant_df = deals_df
    elif parsed["intent"] == "operations":
        result = get_work_order_metrics(work_orders_df)
        relevant_df = work_orders_df
    else:
        result = get_pipeline_analysis(
            deals_df,
            sector=parsed.get("sector"),
            date_range=parsed.get("time_range"),
        )
        relevant_df = deals_df

    warnings = fetch_warnings + data_quality_warnings(deals_df, "Deals") + data_quality_warnings(
        work_orders_df, "Work Orders"
    )
    insight = generate_insight(result, relevant_df, warnings)

    leadership_summary = None
    if parsed.get("needs_leadership_summary"):
        leadership_summary = generate_leadership_summary(deals_df, work_orders_df, insight)

    return {
        "query": request.query,
        "parsed_query": parsed,
        "metadata": board_metadata,
        "insight": insight,
        "leadership_summary": leadership_summary,
        "available_env": {
            "openai_enabled": bool(os.getenv("OPENAI_API_KEY")),
            "local_files_mode": True,
        },
    }
