from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from smolagents import tool
import pandas as pd

# Spark
session_name = f"Oracle"
spark = SparkSession.builder.remote(
    "sc://spark-connect-test.tail7cdba.ts.net"
).appName(session_name).getOrCreate()

@tool
def get_list_of_tables_in_database(catalog_name: str, database_name: str) -> list[str]:
    """
    Get all tables in the catalog and database.
    Returns a list[string] representation of available tables.

    Args:
        catalog_name: data catalog to use for query
        database_name: database within catalog to use for query
    """
    query = f"show tables in {catalog_name}.{database_name}"
    result = spark.sql(query).collect()
    tables = [table[1] for table in result]
    return tables

@tool
def get_list_of_databases_in_catalog(catalog_name: str) -> list[str]:
    """
    Get all databases in the catalog.
    Returns a list[string] representation of available databases.

    Args:
        catalog_name: Glue catalog to use for query
    """
    query = f"show databases in {catalog_name}"
    result = spark.sql(query).collect()
    databases = [db[0] for db in result]
    return databases

@tool 
def get_table_description_as_str(catalog_name: str, database_name: str, table_name: str) -> str:
    """
    Get the description of a given 'catalog.db.table'.
    Returns a string representation of table description 
    including column listing w/ types, partitioning and 
    iceberg tables properties.
    
    Args:
        catalog_name: Name of the catalog to use for query
        database_name: Name of the database to use for query
        table_name: Name of the table to use for query
    """
    query = f"DESCRIBE TABLE EXTENDED {catalog_name}.{database_name}.{table_name}"
    result = spark.sql(query).toPandas()
    return result.to_string()

@tool
def get_table_columns_and_types_as_list(catalog_name: str, database_name: str, table_name: str) -> list[tuple[str, str]]:
    """
    Get the columns and their data types for a given 'catalog.db.table'.
    Returns a list of tuples with column names and their data types.
    
    Args:
        catalog_name: Name of the catalog to use for query
        database_name: Name of the database to use for query
        table_name: Name of the table to use for query
    """
    query = f"DESCRIBE {catalog_name}.{database_name}.{table_name}"
    result = spark.sql(query).collect()
    return [(row.col_name, row.data_type) for row in result]

@tool
def get_table_ddl_as_str(table_name: str) -> str:
    """
    Get CREATE TABLE statement for a given 'catalog.db.table'.
    Returns a string representation of the table's DDL.
    
    Args:
        table_name: Name of the table to inspect; format 'catalog.db.table'
    """
    query = f"show create table {table_name}"
    tbl_schema = spark.sql(query).collect()
    return tbl_schema[0][0]

@tool
def sample_table_data(table_name: str, where_clause: str = "", limit: int = 10) -> str:
    """
    Get sample data from a table to understand the data patterns. 
    Table must be provided in the format 'catalog.db.table'.
    Where clause IS ABSOLUTELY NECESSARY!!!
    Always use partitioning columns when sampling data to avoid large scans.

    Note:
        - Always provide partitioning for 'base' table 
    
    Args:
        table_name: Name of the table to sample
        limit: Number of rows to return (default: 5, max: 10)
        where_clause: Optional SQL WHERE clause to filter results
    """
    limit = 20 if limit > 20 else limit
    if len(where_clause) > 0:
        where_clause = f"where {where_clause}"
    query = f"""
        select * 
        from {table_name} 
        {where_clause} 
        limit {limit}
    """

    result = spark.sql(query).toPandas()
    return result.to_string()

@tool
def sql_query_to_str(query: str, max_rows: int = 20) -> str:
    """
    Execute a SQL SELECT query and return results as pd.Datafram.to_string() string value
    Only SELECT statements are allowed for security.
    LIMIT is applied to the query if not specified.

    Rules:
        - When querying ANY .base table you must provide a where for eventtimeunix and isocode
            - A good practice it to limit to one day in early 2025 and to 'RU' isocode.
                - eg. `where date(eventtimeunix) = '2025-01-01' and isocode = 'RU'`
    
    Args:
        query: The SQL SELECT query to execute
        max_rows: Maximum number of rows to return. Not required. Default is 20.
    """
    
    query_upper = query.strip().upper()

    if not query_upper.startswith('SELECT'):
        return "Error: Only SELECT queries are allowed for security reasons."
    
    if "LIMIT" not in query_upper:
        max_rows = 20 if max_rows > 20 else max_rows
        query = query.replace(";", "") + f" LIMIT {max_rows}"

    result = spark.sql(query).toPandas()
    return result.to_string()

@tool
def sql_query_to_pandas_df(query: str, max_rows: int = 20) -> pd.DataFrame | str:
    """
    Execute a SQL query and return the result as a Pandas DataFrame.
    Returns a Pandas DataFrame representation of the query result. 
    
    Args:
        query: The SQL query to execute
        max_rows: Maximum number of rows to return. Not required. Default is 20.
    """
    query_upper = query.strip().upper()
    
    if not query_upper.startswith('SELECT'):
        return "Error: Only SELECT queries are allowed for security reasons."
    
    if "LIMIT" not in query_upper:
        max_rows = 20 if max_rows > 20 else max_rows
        query = query.replace(";", "") + f" LIMIT {max_rows}"

    result = spark.sql(query).toPandas()
    return result

@tool
def get_column_sample_as_list(table_name: str, column_name: str, limit: int = 20) -> list[str]:
    """
    Get a sample of records from a specific column to help with query building.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column
        limit: Maximum number of unique values to return. Not required. Default is 20.
    """
    limit = 20 if limit > 20 else limit
    query = f"""
        select {column_name} 
        from {table_name} 
        where {column_name} is not null 
        limit {limit}
    """
    result = spark.sql(query).collect()
    column_sample = [row[column_name] for row in result]
    return column_sample
    
@tool
def get_table_row_count(table_name: str) -> int:
    """
    Get the total number of rows in a table.
    
    Args:
        table_name: Name of the table to count rows
    """
    query = f"select count(*) from {table_name}"
    result = spark.sql(query).collect()
    return result[0][0]

@tool
def get_table_history(table_name: str, limit: int = 10) -> str:
    """
    Get the history of changes made to a table.
    
    Args:
        table_name: Name of the table to inspect
        limit: Maximum number of history records to return. Not required. Default is 10.
    """
    query = f"""
        select
            h.made_current_at,
            s.operation,
            h.snapshot_id,
            h.is_current_ancestor,
            s.summary['spark.app.id']
        from {table_name}.history h
        join {table_name}.snapshots s
        on h.snapshot_id = s.snapshot_id
        order by made_current_at
        limit 10
    """
    result = spark.sql(query).toPandas()
    return result.to_string()
    