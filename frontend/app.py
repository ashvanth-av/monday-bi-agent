import streamlit as st
import pandas as pd

# Load data
deals = pd.read_excel("Deal_funnel_Data.xlsx")
work_orders = pd.read_excel("Work_Order_Tracker_Data.xlsx")

st.set_page_config(page_title="Monday.com BI AI Agent", page_icon=":bar_chart:", layout="centered")

st.title("Monday.com BI AI Agent")
st.caption("Ask questions about deals, revenue, sectors, and work orders.")

st.info("Running in local data mode using uploaded Excel files.")
st.caption("Sample queries: How is pipeline for energy sector? | Revenue summary | Leadership update")

# Chat memory
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# -------- PROCESS FUNCTION --------
def process_query(query):
    query = query.lower()

    try:
        total_value = deals.select_dtypes(include='number').sum().sum()
    except:
        total_value = 0

    if "pipeline" in query:
        return f"""
**Summary:** Pipeline looks active with {len(deals)} deals.

**Trends:** Total pipeline value is approximately ₹{int(total_value)}.

**Warnings:** Some records may contain missing or inconsistent values.

**Recommendations:** Focus on closing high-value deals and improving data consistency.
"""

    elif "revenue" in query:
        return f"""
**Summary:** Revenue appears stable.

**Trends:** Estimated total revenue is ₹{int(total_value)}.

**Warnings:** Some financial entries may be incomplete.

**Recommendations:** Diversify deal sources and ensure accurate data entry.
"""

    elif "sector" in query:
        if "sector" in deals.columns:
            top_sector = deals["sector"].astype(str).value_counts().idxmax()
        else:
            top_sector = "Unknown"

        return f"""
**Summary:** Top performing sector is {top_sector}.

**Trends:** Sector performance varies across deals.

**Warnings:** Sector data may not be consistent across all records.

**Recommendations:** Focus on high-performing sectors while improving weaker ones.
"""

    elif "work" in query:
        return f"""
**Summary:** There are {len(work_orders)} work orders.

**Trends:** Execution is ongoing across multiple tasks.

**Warnings:** Some work order statuses may be incomplete.

**Recommendations:** Improve tracking and timely updates for operations.
"""

    elif "leadership" in query:
        return """
**Leadership Summary**

KPIs:
- Stable pipeline
- Consistent deal flow

Risks:
- Data inconsistencies
- Execution gaps

Recommendations:
- Improve data quality
- Strengthen sales-to-execution conversion
"""

    else:
        return "Please ask about pipeline, revenue, sector, or operations."

# -------- CHAT INPUT --------
prompt = st.chat_input("Ask about pipeline, revenue, sector performance, or operations...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    answer = process_query(prompt)

    with st.chat_message("assistant"):
        st.markdown(answer)

    st.session_state.messages.append({"role": "assistant", "content": answer})