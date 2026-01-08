import streamlit as st
import _snowflake
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

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

def ask_agent(api_endpoint, prompt):
    body = {
      "messages": [
        {
            "role": "user",
            "content": [
            {
              "type": "text",
              "text": prompt
            }
          ]
        }
      ],
    }
    headers = {"Accept": "text/event-stream"}
    resp = _snowflake.send_snow_api_request(
        "POST", api_endpoint, headers, {}, body, None, TIMEOUT_MS
    )
    st.write(resp)
    
def main():
    st.title("Retail Intelligence Platform")

    agents = get_agent_data()
    agent_object = select_agent(agents)
    agent_object_name = agent_object['name']
    
    api_endpoint = f"/api/v2/databases/{DB}/schemas/{SCHEMA}/agents/{agent_object_name}:run"

    st.subheader(f"Ask the {agent_object['profile']['display_name']}")
    prompt = st.text_input("", "Show price comparisons", label_visibility="collapsed")
    
    if st.button("Run"):
        with st.spinner("Thinking", show_time=True):
            ask_agent(api_endpoint, prompt)

if __name__ == "__main__":
    main()
