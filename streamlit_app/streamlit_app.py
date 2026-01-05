RETAIL_DB.ABT_BUY.ABT_CANONICALimport streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Retail Intelligence Platform", layout="wide")
st.title("Retail Intelligence Platform")
