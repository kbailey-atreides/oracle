from smolagents import LiteLLMModel
from smolagents import CodeAgent
from tools import *

from phoenix.otel import register
from openinference.instrumentation.smolagents import SmolagentsInstrumentor

# telemetry
register()
SmolagentsInstrumentor().instrument()


# External Models
# model_id = "o4-mini"
# model_id = "gpt-4.1"
# model_id = "o3-mini"
# model_id = "gpt-4.1-mini"
# model_id = "gpt-4.1-nano"

# Local Models
model_id = "ollama_chat/gemma3:27b-it-qat"
# model_id = "ollama_chat/magistral:24b"
# model_id = "ollama_chat/devstral:24b"
# model_id = "ollama_chat/qwen3:8b"
# model_id = "ollama_chat/hf.co/unsloth/GLM-4-32B-0414-GGUF:Q4_K_M"

# 
if any(substring in model_id for substring in ["o3", "o4"]):
    # reasoning openai models
    model = LiteLLMModel(
        model_id=model_id,
    )
elif any(substring in model_id for substring in ["4.1"]):
    # non-reasoning openai models 
    model = LiteLLMModel(
        model_id=model_id,
        temperature=0.3,
        top_p=0.9,
    )
else:
    # local models
    model = LiteLLMModel(
        model_id=model_id,
        temperature=0.3,
        top_p=0.9,
        api_base="http://192.168.8.116:11434",
    )

agent = CodeAgent(
    tools=[
        get_list_of_tables_in_database,
        get_list_of_databases_in_catalog,
        get_table_columns_and_types_as_list,
        # get_table_description_as_str,
        # get_column_sample_as_list,
        # sql_query_to_pandas_df,
        # get_table_ddl_as_str, 
        sql_query_to_str,
        # sample_table_data,
        # get_table_history,
    ],
    model=model,
    # additional_authorized_imports=["pandas", "numpy"],
    max_steps=5,
    # verbosity_level=2,
    planning_interval=3,
    use_structured_outputs_internally=True,
    
)

def query_agent(query: str):
    result = agent.run(query)
    print(result)

if __name__ == "__main__":
    
    # example queries
    query1 = "does EntityId='2E1EF0B6328770FA94ABED6B63FA273A' exist in prod_catalog.adtech_db.base on Jan 31st 2025 to Feb 1st 2025, for isocode = 'RU' ??"
    query2 = "How many records for EntityId='2E1EF0B6328770FA94ABED6B63FA273A' exist in prod_catalog.adtech_db.base on Jan 31st 2025 to Feb 1st 2025, for isocode = 'RU' ??"
    query3 = """
    I would like to know how many unique EntityId were within 10km of the following latitude and longitude 52.22862088327653, 104.23769255915738 on Jan 31st 2025 in isocode RU.
    use prod_catalog.adtech_db.base table.
    """
    query4 = "what unique values do we have for the 'provider' field in prod_catalog.adtech_db.base table on April 13th 2025 in isocode RU.?"
    query5 = "Explain the data drop in the 3rd week of Jan 2025 for pickwell provider in prod_catalog.adtech_db.base table."

    # run query 
    if len(sys.argv) > 1:
        query = sys.argv[1]
    else:
        query = query1


    # turn off thinking for Qwen3 local models
    no_think = "/no_think" if "qwen3" in model_id else ""

    prompt = f"""
    {no_think}

    Persona
    ------------------
    You are an expert data engineer with expertise in SQL and data analysis.
    You are working with a Spark SQL environment querying an AWS Glue catalog.
    Use the available tools to answer the query.
    
    DB Environment
    ------------------
    - typical notation is '<catalog_name>.<database_name>.<table_name>'
    - default catalog is 'prod_catalog'
    - default database is 'adtech_db'
    - default table is 'base'

    SQL Query Rules
    ------------------
    - User MUST provide full catalog.database.table in the query if asking questions about a specific table.
        - If table is not provided and query requires a table, return ERROR :: Brief description of error!
        - If only table name provided, check for table in:
            - catalog_name == 'prod_catalog' or 'test_catalog'
            - database_name == 'adtech_db' or 'orbat_db'
    - Use the provided tools to get table schema and validate before running queries.
    - Use Haversine formula or Spherical Law of Cosines to calculate distance.
    - Use Spark SQL syntax for all queries.
    - When doing string comparisons, use case-insensitive matching.

    ------------------
    ------------------
    
    QUERY :: {query}
    """
    
    query_agent(prompt)

    ## TODO: query refinement! Make sure all pieces are in query and give a second pass at making it more clear for the coding agent. 
    # TODO: add verifier function for the tool 
    # TODO: a bot that can write and read files from disk 
    # TODO: a bot that can write spark sql  (dangerous, but useful)
    # TODO: move agent persona + rules + env to the system_prompt 