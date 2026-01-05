import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="AI Product Matching & Retail Intelligence", layout="wide")
st.title("🧠 AI Product Matching & Retail Intelligence")

session = get_active_session()

# -----------------------
# Sidebar controls
# -----------------------
st.sidebar.header("Filters")
min_score = st.sidebar.slider("Min similarity score", 0.0, 1.0, 0.80, 0.01)
retailer_filter = st.sidebar.multiselect(
    "Retailers (from canonical views)",
    options=[*session.sql("SELECT DISTINCT RETAILER FROM ABT_BUY_SCHEMA.ABT_CANONICAL").to_pandas()["RETAILER"].dropna().unique(),
             *session.sql("SELECT DISTINCT RETAILER FROM ABT_BUY_SCHEMA.BUY_CANONICAL").to_pandas()["RETAILER"].dropna().unique()],
    default=[],
)
limit_rows = st.sidebar.number_input("Result limit", min_value=50, max_value=5000, value=500, step=50)

# Agent triggers (hook these up to your procedures/agents)
c1, c2, c3 = st.sidebar.columns(3)
run_match = c1.button("Run Matching Agent")
run_price = c2.button("Run Price Optimization")
run_trend = c3.button("Refresh Trends")

# -----------------------
# Call agents (stored procs / Cortex Agents wrappers)
# -----------------------
if run_match:
    try:
        session.sql("CALL ABT_BUY_SCHEMA.RUN_PRODUCT_MATCHING_AGENT()").collect()
        st.success("Matching agent executed. Refresh sections to see updates.")
    except Exception as e:
        st.error(f"Matching agent failed: {e}")

if run_price:
    try:
        session.sql("CALL ABT_BUY_SCHEMA.RUN_PRICE_OPTIMIZATION()").collect()
        st.success("Price optimization executed. Check Competitive Pricing section.")
    except Exception as e:
        st.error(f"Price optimization failed: {e}")

if run_trend:
    try:
        session.sql("CALL ABT_BUY_SCHEMA.REFRESH_MARKET_INTELLIGENCE()").collect()
        st.success("Trend data refreshed. See Market Trends section.")
    except Exception as e:
        st.error(f"Trend refresh failed: {e}")

# -----------------------
# Matching Accuracy Metrics
# -----------------------
st.subheader("📊 Matching Accuracy Metrics")
metrics_sql = """
SELECT RETAILER, MATCHED_PAIRS, AVG_ACCURACY, LAST_UPDATED
FROM ABT_BUY_SCHEMA.MATCHING_METRICS
ORDER BY LAST_UPDATED DESC
"""
df_metrics = session.sql(metrics_sql).to_pandas()

if df_metrics.empty:
    st.info("No metrics yet. Run the matching agent.")
else:
    left, right = st.columns([2,3])
    with left:
        total_pairs = int(df_metrics["MATCHED_PAIRS"].sum())
        avg_acc = float(df_metrics["AVG_ACCURACY"].mean())
        st.metric("Total matched pairs", f"{total_pairs:,}")
        st.metric("Average accuracy", f"{avg_acc:.2%}")
    with right:
        st.bar_chart(df_metrics.rename(columns={"AVG_ACCURACY":"ACCURACY"}), x="RETAILER", y="ACCURACY")
    st.expander("View metrics data").dataframe(df_metrics, use_container_width=True)

# -----------------------
# Candidate Matches & Rationale
# -----------------------
st.subheader("🔗 Candidate Matches (Similarity Scores)")
where_parts = [f"SIMILARITY_SCORE >= {min_score}"]
if retailer_filter:
    rlist = ", ".join([f"'{r}'" for r in retailer_filter])
    where_parts.append(f"(a.RETAILER IN ({rlist}) OR b.RETAILER IN ({rlist}))")
where_clause = " AND ".join(where_parts)

cand_sql = f"""
SELECT
  s.PRODUCT_ID_ABT,
  s.PRODUCT_ID_BUY,
  s.SIMILARITY_SCORE,
  a.RETAILER AS RETAILER_ABT,
  a.TITLE AS TITLE_ABT,
  b.RETAILER AS RETAILER_BUY,
  b.TITLE AS TITLE_BUY,
  s.MATCH_REASON
FROM ABT_BUY_SCHEMA.SIMILARITY_SCORES s
JOIN ABT_BUY_SCHEMA.ABT_CANONICAL a ON s.PRODUCT_ID_ABT = a.PRODUCT_ID
JOIN ABT_BUY_SCHEMA.BUY_CANONICAL b ON s.PRODUCT_ID_BUY = b.PRODUCT_ID
WHERE {where_clause}
ORDER BY s.SIMILARITY_SCORE DESC
LIMIT {limit_rows}
"""
df_cand = session.sql(cand_sql).to_pandas()

if df_cand.empty:
    st.info("No candidates for current filters.")
else:
    st.dataframe(df_cand, use_container_width=True)

# Confirmed/Final matches
st.subheader("✅ Final Product Matches")
final_sql = f"""
SELECT
  f.PRODUCT_ID_ABT,
  f.PRODUCT_ID_BUY,
  f.MATCH_CONFIDENCE,
  f.LAST_UPDATED,
  a.TITLE AS TITLE_ABT,
  b.TITLE AS TITLE_BUY
FROM ABT_BUY_SCHEMA.FINAL_PRODUCT_MATCHES f
JOIN ABT_BUY_SCHEMA.ABT_CANONICAL a ON f.PRODUCT_ID_ABT = a.PRODUCT_ID
JOIN ABT_BUY_SCHEMA.BUY_CANONICAL b ON f.PRODUCT_ID_BUY = b.PRODUCT_ID
WHERE f.MATCH_CONFIDENCE >= {min_score}
ORDER BY f.LAST_UPDATED DESC
LIMIT {limit_rows}
"""
df_final = session.sql(final_sql).to_pandas()
st.dataframe(df_final, use_container_width=True)

# -----------------------
# Competitive Pricing
# -----------------------
st.subheader("💹 Competitive Pricing Comparison")
price_sql = """
SELECT
  RETAILER_ABT,
  RETAILER_BUY,
  PRODUCT_ID_ABT,
  PRODUCT_ID_BUY,
  PRICE_ABT,
  PRICE_BUY,
  PRICE_DELTA,
  LAST_UPDATED
FROM ABT_BUY_SCHEMA.PRICE_COMPARISON
ORDER BY LAST_UPDATED DESC
LIMIT 1000
"""
df_price = session.sql(price_sql).to_pandas()
if df_price.empty:
    st.info("No pricing comparison available.")
else:
    # Aggregate price deltas by retailer pair
    agg = (df_price.groupby(["RETAILER_ABT", "RETAILER_BUY"], as_index=False)["PRICE_DELTA"]
           .mean().rename(columns={"PRICE_DELTA":"AVG_PRICE_DELTA"}))
    st.bar_chart(agg, x="RETAILER_ABT", y="AVG_PRICE_DELTA")
    st.expander("Price comparison").dataframe(df_price, use_container_width=True)

# -----------------------
# Market Trends (Semantic view or metrics)
# -----------------------
st.subheader("📈 Market Trends (Semantic View)")
trend_sql = """
SELECT *
FROM ABT_BUY_SCHEMA.PRODUCTMATCHINGSEMANTICVIEW
LIMIT 1000
"""
df_trend = session.sql(trend_sql).to_pandas()
if df_trend.empty:
    st.info("No trend rows yet. Refresh trends.")
else:
    # Example: If the semantic view exposes weekly share by category
    # Try to auto-detect columns
    cols = df_trend.columns.str.upper()
    if {"WEEK", "CATEGORY", "MARKET_SHARE"}.issubset(set(cols)):
        df_viz = df_trend.rename(columns=str.upper)
        st.line_chart(df_viz.sort_values("WEEK"), x="WEEK", y="MARKET_SHARE", color="CATEGORY")
    st.expander("Semantic view sample").dataframe(df_trend.head(200), use_container_width=True)

st.caption("Built on Snowflake (Snowpark + Snowsight). Use the sidebar to run agents, adjust thresholds, and explore outputs.")
``