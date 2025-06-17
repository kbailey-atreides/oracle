# Data Agent

A simple code agent for querying AWS Glue catalog tables using natural language and Spark SQL.

## Features

- Converts natural language queries to Spark SQL.
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
   python agent/agent.py
   ```
3. Modify the queries in `__main__` as needed.

## Notes

- Only supports Spark SQL syntax.
- Requires full `catalog.database.table` notation for queries.
- Use the provided tools for schema and SQL validation.
