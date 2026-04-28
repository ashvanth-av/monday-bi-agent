import json
import os
import re
from typing import Any, Dict, Optional


DEFAULT_PARSE = {
    "intent": "pipeline",
    "sector": None,
    "status": None,
    "time_range": None,
    "needs_leadership_summary": False,
}


def parse_query(query: str) -> Dict[str, Any]:
    if os.getenv("OPENAI_API_KEY"):
        try:
            llm_result = _parse_with_openai(query)
            if llm_result:
                return {**DEFAULT_PARSE, **llm_result}
        except Exception:
            pass
    return _parse_rule_based(query)


def _parse_with_openai(query: str) -> Optional[Dict[str, Any]]:
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    system_prompt = """
    You convert business intelligence questions into JSON.
    Return JSON only with keys:
    intent, sector, status, time_range, needs_leadership_summary.
    Allowed intents: pipeline, revenue, sector_performance, operations.
    """
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query},
        ],
    )
    content = response.choices[0].message.content
    return json.loads(content)


def _parse_rule_based(query: str) -> Dict[str, Any]:
    query_l = query.lower()
    parsed = dict(DEFAULT_PARSE)

    if any(keyword in query_l for keyword in ["pipeline", "deal", "forecast", "opportunity"]):
        parsed["intent"] = "pipeline"
    elif any(keyword in query_l for keyword in ["revenue", "sales", "bookings", "value"]):
        parsed["intent"] = "revenue"
    elif any(keyword in query_l for keyword in ["operation", "work order", "delivery", "sla", "service", "completion"]):
        parsed["intent"] = "operations"
    elif any(keyword in query_l for keyword in ["sector", "industry", "vertical", "segment"]):
        parsed["intent"] = "sector_performance"

    parsed["needs_leadership_summary"] = "summary" in query_l or "leadership" in query_l
    parsed["time_range"] = _extract_time_range(query_l)
    parsed["status"] = _extract_status(query_l)
    parsed["sector"] = _extract_sector(query_l)
    return parsed


def _extract_time_range(query: str) -> Optional[str]:
    if "this month" in query:
        return "this_month"
    if "last month" in query:
        return "last_month"
    if "this quarter" in query:
        return "this_quarter"
    if "last quarter" in query:
        return "last_quarter"
    if "this year" in query:
        return "this_year"
    return None


def _extract_status(query: str) -> Optional[str]:
    statuses = ["won", "lost", "open", "closed", "done", "stuck", "working on it"]
    for status in statuses:
        if status in query:
            return status
    return None


def _extract_sector(query: str) -> Optional[str]:
    match = re.search(r"(?:sector|industry|vertical)\s+([a-zA-Z0-9&\-\s]+)", query)
    if match:
        return match.group(1).strip()
    reverse_match = re.search(r"(?:for|in|of)\s+([a-zA-Z0-9&\-\s]+?)\s+(?:sector|industry|vertical)", query)
    if reverse_match:
        return reverse_match.group(1).strip() or None
    short_match = re.search(r"\b([a-zA-Z0-9&\-]+)\s+(?:sector|industry|vertical)\b", query)
    if short_match:
        return short_match.group(1).strip()
    return None
