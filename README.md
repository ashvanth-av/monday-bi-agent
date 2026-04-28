# Monday.com Business Intelligence AI Agent

A simple working prototype that loads local Excel extracts, cleans them into Pandas DataFrames, interprets natural language BI questions, and returns conversational insights through a FastAPI backend plus Streamlit chat UI.

## Project structure

```text
project/
├── backend/
│   ├── main.py
│   ├── monday_api.py
│   ├── data_cleaning.py
│   ├── query_parser.py
│   ├── business_logic.py
│   └── insights.py
├── frontend/
│   └── app.py
├── requirements.txt
└── README.md
```

## Features

- Loads local Excel data from `Deal_funnel_Data.xlsx` and `Work_Order_Tracker_Data.xlsx`
- Falls back to demo data if the files are unavailable
- Converts board items into Pandas DataFrames
- Cleans missing values, dates, text, and numeric fields
- Parses user queries with OpenAI when available, or a rule-based fallback
- Supports pipeline, revenue, sector, and operational analysis
- Generates human-readable summaries, trends, warnings, and recommendations
- Optionally returns a leadership summary when the query asks for one

## Setup

1. Create and activate a Python virtual environment.

```bash
python -m venv .venv
.venv\Scripts\activate
```

2. Install dependencies.

```bash
pip install -r requirements.txt
```

3. Optional: create a `.env` file in the project root if you want OpenAI-powered parsing.

```env
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4o-mini
BACKEND_URL=http://localhost:8000
```

## How to run

Start the FastAPI backend from the project root:

```bash
uvicorn backend.main:app --reload
```

Start the Streamlit frontend in a second terminal:

```bash
streamlit run frontend/app.py
```

Open the Streamlit URL shown in the terminal, usually `http://localhost:8501`.

If the Excel files are missing, the app falls back to built-in demo data so you can still validate the full flow locally.

## Example questions

- `What does our pipeline look like this quarter?`
- `Show revenue performance`
- `How is the healthcare sector doing?`
- `Give me an operations summary for work orders`
- `Create a leadership summary for this month`

## Local data assumptions

- The app looks for `Deal_funnel_Data.xlsx` and `Work_Order_Tracker_Data.xlsx` in the project root.
- It also accepts the common downloaded names with ` (1)` suffixes.
- Schemas can vary. The prototype uses keyword matching to detect likely columns such as revenue, sector, status, and dates.
- If a needed column is missing, the app returns a graceful warning instead of failing hard.

## API endpoints

- `GET /health` for a health check
- `POST /chat` with JSON body:

```json
{
  "query": "What is our revenue summary?"
}
```

Example response includes:

- parsed query intent
- metadata about loaded boards
- generated insight
- optional leadership summary

## Notes

- If `OPENAI_API_KEY` is not configured, the app falls back to a rule-based parser.
- If the Excel files are missing or unreadable, the backend falls back to demo mode with clear warnings.
- This is a prototype focused on clean modularity and fast iteration, not a production-hardened analytics platform.
