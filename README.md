# Data Agent

A simple, but powerful coding agent. Utilizes tools and write code to answer user queries. 

## Features

- Converts natural language queries to Spark SQL via tool call.
- Uses external or local LLMs via `smolagents`.
- Supports table schema introspection and SQL generation tools.
- Designed for data engineering tasks (e.g., record counts, unique values, geospatial queries).

## Libraries

- `smolagents`: LLM agent framework
- `pandas`, `numpy`: Data manipulation
- Custom tools in `tools.py`

## Usage

1. Edit `agent/agent.py` to set your model and tools.
2. Run:
   ```bash
   python agent/agent.py "how many tables in test_catalog.dev_kbailey ?"
   ```

