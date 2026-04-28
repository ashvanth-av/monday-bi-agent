import os

import requests
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

st.set_page_config(page_title="Monday.com BI AI Agent", page_icon=":bar_chart:", layout="centered")
st.title("Monday.com BI AI Agent")
st.caption("Ask questions about deals, revenue, sectors, and work orders.")
st.info("Running in local data mode using uploaded Excel files.")
st.caption(
    "Sample queries: `How is pipeline for energy sector?`, `Revenue summary`, `Leadership update`"
)

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

prompt = st.chat_input("Ask about pipeline, revenue, sector performance, or operations...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            response = requests.post(
                f"{BACKEND_URL}/chat",
                json={"query": prompt},
                timeout=90,
            )
            if response.status_code >= 400:
                try:
                    detail = response.json().get("detail", response.text)
                except Exception:
                    detail = response.text
                raise RuntimeError(detail)
            payload = response.json()
            insight = payload["insight"]

            lines = [
                f"**Summary:** {insight['summary']}",
                f"**Trends:** {insight['trends']}",
            ]
            if insight.get("warnings"):
                lines.append("**Warnings:**")
                lines.extend([f"- {warning}" for warning in insight["warnings"]])
            lines.append(f"**Recommendations:** {insight['recommendations']}")

            if payload.get("leadership_summary"):
                summary = payload["leadership_summary"]
                lines.append("**Leadership Summary**")
                if summary.get("kpis"):
                    lines.append("KPIs:")
                    lines.extend([f"- {kpi}" for kpi in summary["kpis"]])
                if summary.get("risks"):
                    lines.append("Risks:")
                    lines.extend([f"- {risk}" for risk in summary["risks"]])
                if summary.get("recommendations"):
                    lines.append("Leadership recommendations:")
                    lines.extend([f"- {item}" for item in summary["recommendations"]])

            answer = "\n".join(lines)
        except Exception as exc:
            answer = f"Backend request failed: {exc}"

        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
