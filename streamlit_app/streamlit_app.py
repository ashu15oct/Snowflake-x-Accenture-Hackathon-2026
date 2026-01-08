import streamlit as st
import _snowflake
import json

DB = "RETAIL_DB"
SCHEMA = "ABT_BUY"

TIMEOUT_MS = 30000

def get_agent_data():
    """
    Get Agent Data
    """
    path = f"/api/v2/databases/{DB}/schemas/{SCHEMA}/agents"
    resp = _snowflake.send_snow_api_request("GET", path, {}, {}, None, None, TIMEOUT_MS)
    
    # Parse JSON
    data = json.loads(resp["content"])
    return data.get("data", [])

def select_agent(agents):
    """
    Select Agent
    """
    st.header("Agent List")
    st.subheader("Select Agent")
    
    labels = [agent['profile']['display_name'] for agent in agents]
    selected_display = st.radio("", labels, label_visibility = "collapsed")
    selected_agent = next(agent for agent in agents if agent['profile']['display_name'] == selected_display)
    return selected_agent
    
def main():
    st.title("Retail Intelligence Platform")

    agents = get_agent_data()
    agent_object = select_agent(agents)
    agent_object_name = agent_object['name']
    
    api = f"/api/v2/databases/{DB}/schemas/{SCHEMA}/agents/{agent_object_name}:run"

    st.subheader(f"Ask the {agent_object['profile']['display_name']}")
    prompt = st.text_input("", "Show price comparisons", label_visibility="collapsed")

if __name__ == "__main__":
    main()
