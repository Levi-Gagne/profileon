# profileon/src/pages/rules_dashboard.py

import streamlit as st
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

from config.cla_color_config import CLAColor


rules_table = "dq_dev.expectations.dqx_expectations"  # Change here for now

spark = SparkSession.builder.getOrCreate()

# Load rules from Delta table
try:
    rules_df = spark.table(rules_table).toPandas()
except Exception as e:
    rules_df = None
    error_msg = str(e)

st.set_page_config(page_title="profileon | rules dashboard", layout="wide")

primary = CLAColor.ansi_to_hex(CLAColor.cla['riptide'])
navy = CLAColor.ansi_to_hex(CLAColor.cla['navy'])
cloud = CLAColor.ansi_to_hex(CLAColor.cla['cloud'])
scarlett = CLAColor.ansi_to_hex(CLAColor.cla['scarlett'])
saffron = CLAColor.ansi_to_hex(CLAColor.cla['saffron'])
smoke = CLAColor.ansi_to_hex(CLAColor.cla['smoke'])

st.markdown(
    f"""
    <div style="background:{cloud}; padding:30px 0 20px 0; border-radius:18px; border:2px solid {primary};">
      <h1 style="color:{primary}; font-size:2.5rem; font-family: 'Montserrat', 'Segoe UI', 'Arial', sans-serif; text-align:center; font-weight:800; letter-spacing:0.2px; margin-bottom:4px; text-transform: lowercase;">
        rules dashboard
      </h1>
      <div style="color:{navy}; font-size:1.22rem; font-weight:500; text-align:center; margin-bottom:8px;">
        overview & live snapshot of all generated DQX rules
      </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown("<br>", unsafe_allow_html=True)

# --- Summary Dashboard ---
if rules_df is not None and not rules_df.empty:
    total_rules = len(rules_df)
    tables_with_rules = rules_df['table'].nunique()
    avg_rules_per_table = (
        rules_df.groupby('table').size().mean() if tables_with_rules else 0
    )

    st.markdown(
        f"""
        <div style="display:flex; gap:2.7vw; justify-content:center; margin-bottom:25px;">
          <div style="background:{saffron}; color:{navy}; border-radius:12px; padding:22px 32px; min-width:220px; text-align:center; box-shadow:0 2px 8px #E2E86844;">
            <div style="font-size:2.0rem; font-weight:700;">{total_rules:,}</div>
            <div style="font-size:1.03rem;">total rules</div>
          </div>
          <div style="background:{primary}; color:{navy}; border-radius:12px; padding:22px 32px; min-width:220px; text-align:center; box-shadow:0 2px 8px #7DD2D355;">
            <div style="font-size:2.0rem; font-weight:700;">{tables_with_rules:,}</div>
            <div style="font-size:1.03rem;">tables with rules</div>
          </div>
          <div style="background:{scarlett}; color:{cloud}; border-radius:12px; padding:22px 32px; min-width:220px; text-align:center; box-shadow:0 2px 8px #EE534044;">
            <div style="font-size:2.0rem; font-weight:700;">{avg_rules_per_table:,.2f}</div>
            <div style="font-size:1.03rem;">avg rules per table</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )
    # Placeholder/future metrics (can be filled in later)
    st.markdown(
        f"""
        <div style="background:{smoke}; color:{navy}; border-radius:10px; padding:13px 24px; margin-bottom:14px; text-align:center;">
            <i>More dashboard features coming soon—filter by owner, show recent changes, rule coverage % …</i>
        </div>
        """,
        unsafe_allow_html=True
    )

    # --- Rule Table ---
    st.markdown(
        f"""
        <div style="margin-bottom:7px; color:{navy}; font-size:1.19rem; font-weight:700;">Current DQX Rules</div>
        """,
        unsafe_allow_html=True
    )
    st.dataframe(rules_df, use_container_width=True)
else:
    st.markdown(
        f"""
        <div style="background:{scarlett}; color:{cloud}; border-radius:10px; padding:21px 26px; margin-bottom:16px;">
            <b>No rules found or failed to load table:</b><br>{error_msg if rules_df is None else "No rules in the selected table yet."}
        </div>
        """,
        unsafe_allow_html=True
    )
