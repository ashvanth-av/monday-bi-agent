from typing import Dict, List

import pandas as pd


def generate_insight(result: Dict, df: pd.DataFrame, warnings: List[str]) -> Dict:
    metric = result.get("metric", "analysis")
    missing_ratio = float(df.attrs.get("missing_ratio", 0.0)) if df is not None and not df.empty else 1.0

    if result.get("summary"):
        summary = result["summary"]
        trends = "Limited data was available, so the response is based on partial board coverage."
        recommendations = "Check board credentials, IDs, and required columns to improve result quality."
    elif metric == "pipeline":
        summary = (
            f"Pipeline appears {'strong' if result.get('pipeline_value', 0) > 0 else 'soft'} with total value "
            f"of INR {result.get('pipeline_value', 0):,.0f} across {result.get('deal_count', 0)} deals. "
            f"Of these, {result.get('open_deals', 0)} opportunities are still open, which means a meaningful "
            "share of near-term revenue depends on execution quality, follow-up discipline, and deal progression."
        )
        trends = (
            f"{result.get('open_deals', 0)} deals are still active in the funnel, so conversion discipline "
            f"will have an outsized impact on near-term outcomes."
        )
        recommendations = (
            "Prioritize high-value open deals and review incomplete records before using this pipeline in forecasts."
        )
    elif metric == "revenue":
        summary = (
            f"Revenue currently stands at INR {result.get('total_revenue', 0):,.0f} from "
            f"{result.get('closed_won_count', 0)} closed or won deals. This suggests that realized business is "
            "coming from a meaningful base of converted opportunities, and it provides a clearer picture of actual "
            "commercial performance than pipeline alone."
        )
        trends = (
            f"Average realized value per converted deal is INR {result.get('average_revenue', 0):,.0f}, "
            "which helps indicate deal quality as well as volume."
        )
        recommendations = "Review deal-stage hygiene and conversion mix to improve the reliability of revenue reporting."
    elif metric == "sector_performance":
        top_sector = (result.get("top_sectors") or [{}])[0]
        summary = (
            f"Sector performance is led by {top_sector.get('_sector', 'unknown')} with "
            f"{top_sector.get('deals', 0)} deals and INR {top_sector.get('value', 0):,.0f} in value. "
            "This indicates that sector concentration is shaping a large share of the commercial outcome, so the "
            "current leader should be treated as both a growth signal and a portfolio concentration watchpoint."
        )
        trends = "Performance is concentrated in a few sectors, so both concentration risk and whitespace opportunity should be tracked."
        recommendations = "Double down on winning sectors while reviewing whether lagging segments need new focus or cleaner tagging."
    else:
        completion_rate = 0
        if result.get("total_work_orders"):
            completion_rate = result.get("completed_work_orders", 0) / result["total_work_orders"]
        summary = (
            f"Operations show {result.get('completed_work_orders', 0)} completed work orders out of "
            f"{result.get('total_work_orders', 0)} total, with INR {result.get('total_order_value', 0):,.0f} linked value. "
            f"There are also {result.get('open_work_orders', 0)} active items still moving through execution, so "
            "delivery consistency and closure speed will directly influence backlog health and billing readiness."
        )
        trends = (
            f"Current completion rate is {completion_rate:.1%}, with {result.get('open_work_orders', 0)} open "
            f"items and {result.get('overdue_work_orders', 0)} overdue orders needing attention."
        )
        recommendations = "Focus on overdue work orders and tighten operational status updates so backlog risk stays visible."

    data_warning = None
    if df is not None and not df.empty and missing_ratio > 0:
        data_warning = (
            f"{missing_ratio:.1%} of the source data is incomplete. Some insights may be directionally correct "
            "but not fully precise."
        )

    combined_warnings = warnings.copy()
    if data_warning:
        combined_warnings.append(data_warning)

    return {
        "summary": summary,
        "trends": trends,
        "warnings": combined_warnings,
        "recommendations": recommendations,
        "raw_result": result,
    }


def generate_leadership_summary(deals_df: pd.DataFrame, work_orders_df: pd.DataFrame, insights: Dict) -> Dict:
    kpis = []
    risks = list(insights.get("warnings", []))

    if deals_df is not None and not deals_df.empty:
        kpis.append(f"Deals rows loaded: {len(deals_df)}")
    if work_orders_df is not None and not work_orders_df.empty:
        kpis.append(f"Work order rows loaded: {len(work_orders_df)}")

    recommendations = [
        insights.get("recommendations", ""),
        "Confirm that key board columns such as status, sector, and revenue are consistently maintained.",
    ]

    return {
        "kpis": kpis,
        "risks": risks,
        "recommendations": [item for item in recommendations if item],
    }
