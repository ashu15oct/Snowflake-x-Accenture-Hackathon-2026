import streamlit as st
import _snowflake
import json
st.title("List Cortex Agents")

DB = "RETAIL_DB"
SCHEMA = "ABT_BUY"
TIMEOUT_MS = 30000

# Call the API
path = f"/api/v2/databases/{DB}/schemas/{SCHEMA}/agents"
resp = _snowflake.send_snow_api_request("GET", path, {}, {}, None, None, TIMEOUT_MS)

# Parse JSON
data = json.loads(resp["content"])
agents = data.get("data", [])

# Show each agent vertically
for agent in agents:
    st.markdown("---")
    st.write(f"**Display Name:** {agent.get('profile', {}).get('display_name')}")
    st.write(f"**Object Name:** {agent.get('name')}")
    st.write(f"**Comment:** {agent.get('comment')}")
    st.write(f"**Database:** {agent.get('database_name')}")
    st.write(f"**Schema:** {agent.get('schema_name')}")
    st.write(f"**Owner:** {agent.get('owner')}")
    st.write(f"**Created On:** {agent.get('created_on')}")