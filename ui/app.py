import streamlit as st
import openai
from typing import List, Dict, Any
import os

# Configure page
st.set_page_config(
    page_title="Data Investigation UI",
    page_icon="🔍",
    layout="wide"
)

# Initialize OpenAI client
@st.cache_resource
def get_openai_client():
    # Make sure to set your OPENAI_API_KEY environment variable
    return openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Fetch available OpenAI models
@st.cache_data
def get_available_models():
    try:
        client = get_openai_client()
        models = client.models.list()
        # Filter for chat models (GPT models)
        chat_models = [
            model.id for model in models.data 
            if any(x in model.id.lower() for x in ['gpt-4', 'gpt-3.5', 'gpt-4o'])
        ]
        return sorted(chat_models)
    except Exception as e:
        # Fallback models if API call fails
        return ["gpt-4", "gpt-4-turbo", "gpt-3.5-turbo", "gpt-4o"]

# Mock functions for database queries (replace with actual Spark queries)
@st.cache_data
def get_databases_for_catalog(catalog: str) -> List[str]:
    # Replace with actual Spark query
    mock_databases = {
        "test_catalog": ["test_db1", "test_db2", "analytics_test"],
        "prod_catalog": ["prod_db1", "prod_db2", "analytics_prod", "sales_db"],
        "legacy_catalog": ["legacy_db1", "legacy_db2", "old_analytics"]
    }
    return mock_databases.get(catalog, [])

@st.cache_data
def get_tables_for_database(catalog: str, database: str) -> List[str]:
    # Replace with actual Spark query
    mock_tables = [
        "users", "orders", "products", "transactions", 
        "customer_data", "sales_metrics", "inventory"
    ]
    return mock_tables

# Initialize session state for chat history and model settings
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Initialize model settings in session state
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "gpt-3.5-turbo"
if "system_message" not in st.session_state:
    st.session_state.system_message = "You are a helpful data analyst assistant. Help users analyze and understand their data with clear, accurate insights."
if "temperature" not in st.session_state:
    st.session_state.temperature = 0.7
if "max_tokens" not in st.session_state:
    st.session_state.max_tokens = 500
if "top_p" not in st.session_state:
    st.session_state.top_p = 1.0
if "frequency_penalty" not in st.session_state:
    st.session_state.frequency_penalty = 0.0
if "presence_penalty" not in st.session_state:
    st.session_state.presence_penalty = 0.0

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        font-size: 2rem;
        font-weight: bold;
        border-bottom: 2px solid #f0f0f0;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">Data Investigation UI</div>', unsafe_allow_html=True)

# Sidebar for model settings
with st.sidebar:
    st.markdown("## 🤖 Model Settings")
    
    # Model selection
    available_models = get_available_models()
    st.session_state.selected_model = st.selectbox(
        "Select Model",
        options=available_models,
        index=available_models.index(st.session_state.selected_model) if st.session_state.selected_model in available_models else 0,
        key="model_selector"
    )
    
    # System message
    with st.expander("System Message", expanded=False):
        st.session_state.system_message = st.text_area(
            "Custom System Message",
            value=st.session_state.system_message,
            height=100,
            help="Define the AI's role and behavior",
            key="system_message_input"
        )
    
    st.markdown("### Model Parameters")
    
    # Temperature
    st.session_state.temperature = st.slider(
        "Temperature",
        min_value=0.0,
        max_value=2.0,
        value=st.session_state.temperature,
        step=0.1,
        help="Controls randomness: 0.0 = deterministic, 2.0 = very creative",
        key="temperature_slider"
    )
    
    # Max tokens
    st.session_state.max_tokens = st.slider(
        "Max Tokens",
        min_value=50,
        max_value=4000,
        value=st.session_state.max_tokens,
        step=50,
        help="Maximum length of the response",
        key="max_tokens_slider"
    )
    
    # Top P
    st.session_state.top_p = st.slider(
        "Top P",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.top_p,
        step=0.05,
        help="Nucleus sampling: consider top P% of tokens",
        key="top_p_slider"
    )
    
    # Frequency penalty
    st.session_state.frequency_penalty = st.slider(
        "Frequency Penalty",
        min_value=-2.0,
        max_value=2.0,
        value=st.session_state.frequency_penalty,
        step=0.1,
        help="Reduces repetition of tokens",
        key="frequency_penalty_slider"
    )
    
    # Presence penalty
    st.session_state.presence_penalty = st.slider(
        "Presence Penalty",
        min_value=-2.0,
        max_value=2.0,
        value=st.session_state.presence_penalty,
        step=0.1,
        help="Encourages talking about new topics",
        key="presence_penalty_slider"
    )
    
    # Show current settings
    with st.expander("Current Settings", expanded=False):
        st.json({
            "model": st.session_state.selected_model,
            "temperature": st.session_state.temperature,
            "max_tokens": st.session_state.max_tokens,
            "top_p": st.session_state.top_p,
            "frequency_penalty": st.session_state.frequency_penalty,
            "presence_penalty": st.session_state.presence_penalty
        })

# Control section
# Top row with dropdowns and toggle
col1, col2, col3, col4 = st.columns(4)

with col1:
    selected_table = st.selectbox(
        "Table",
        options=["Select table..."] + get_tables_for_database("", ""),
        key="table_select"
    )

with col2:
    selected_catalog = st.selectbox(
        "Catalog",
        options=["test_catalog", "prod_catalog", "legacy_catalog"],
        key="catalog_select"
    )

with col3:
    # Get databases based on selected catalog
    databases = get_databases_for_catalog(selected_catalog)
    selected_database = st.selectbox(
        "Database",
        options=["Select database..."] + databases,
        key="database_select"
    )

with col4:
    metadata_enabled = st.toggle("Metadata", value=False)

# Update table options when database changes
if selected_database and selected_database != "Select database...":
    tables = get_tables_for_database(selected_catalog, selected_database)
    # Force refresh of table selectbox
    st.rerun()

# Horizontal rule separator
st.markdown("<hr>", unsafe_allow_html=True)

# Conversation section with ChatGPT-like interface
st.markdown("### Conversation")

# Create main chat container
chat_container = st.container(height=500)

# Display chat history using native Streamlit chat components
with chat_container:
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.write(message["content"])

# Chat input and clear button on same row
input_col, clear_col = st.columns([0.85, 0.15])

with input_col:
    query_input = st.chat_input("Ask a question about your data...")

with clear_col:
    if st.session_state.chat_history:
        if st.button("Clear", type="secondary", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()

# Handle new query submission
if query_input:
    # Add user message to chat history
    st.session_state.chat_history.append({
        "role": "user",
        "content": query_input
    })
    
    # Display user message immediately
    with chat_container:
        with st.chat_message("user"):
            st.write(query_input)
    
    # Get AI response
    try:
        client = get_openai_client()
        
        # Create context message with selected database info
        database_context = f"You are helping analyze data from {selected_catalog}.{selected_database}.{selected_table if selected_table != 'Select table...' else 'unspecified table'}. "
        database_context += f"Metadata access is {'enabled' if metadata_enabled else 'disabled'}."
        
        # Combine custom system message with database context
        full_system_message = f"{st.session_state.system_message}\n\nDatabase Context: {database_context}"
        
        # Build messages with proper typing
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": full_system_message}
        ]
        messages.extend(st.session_state.chat_history)
        
        response = client.chat.completions.create(
            model=st.session_state.selected_model,
            messages=messages,  # type: ignore
            max_tokens=st.session_state.max_tokens,
            temperature=st.session_state.temperature,
            top_p=st.session_state.top_p,
            frequency_penalty=st.session_state.frequency_penalty,
            presence_penalty=st.session_state.presence_penalty
        )
        
        assistant_response = response.choices[0].message.content
        
    except Exception as e:
        assistant_response = f"Mock Response: I understand you're asking about '{query_input}'. In a real implementation, I would query the {selected_catalog}.{selected_database} database and provide insights about your data. (Using {st.session_state.selected_model} with temp={st.session_state.temperature})"
    
    # Add assistant response to chat history
    st.session_state.chat_history.append({
        "role": "assistant", 
        "content": assistant_response
    })
    
    # Display assistant message immediately
    with chat_container:
        with st.chat_message("assistant"):
            st.write(assistant_response)
    
    st.rerun()
