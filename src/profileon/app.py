import streamlit as st
from profileon.config.cla_color_config import CLAColor

st.set_page_config(page_title="profileon", layout="centered")

# --- Define HEX colors ---
navy = CLAColor.ansi_to_hex(CLAColor.cla['navy'])
cloud = CLAColor.ansi_to_hex(CLAColor.cla['cloud'])
primary = CLAColor.ansi_to_hex(CLAColor.cla['riptide'])
saffron = CLAColor.ansi_to_hex(CLAColor.cla['saffron'])
scarlett = CLAColor.ansi_to_hex(CLAColor.cla['scarlett'])

# --- Custom hover-based info tooltip ---
st.markdown(
    f"""
    <style>
    .info-hover {{
        position: relative;
        display: inline-block;
        cursor: pointer;
    }}
    .info-hover .tooltip {{
        visibility: hidden;
        width: 360px;
        background-color: {cloud};
        color: {navy};
        text-align: left;
        border-radius: 8px;
        padding: 16px;
        border: 2px solid {saffron};
        font-size: 0.92rem;
        font-family: 'Segoe UI', sans-serif;
        position: absolute;
        z-index: 1;
        top: 130%;
        left: 50%;
        margin-left: -180px;
    }}
    .info-hover:hover .tooltip {{
        visibility: visible;
    }}
    </style>
    <div class="info-hover">
        <span style="font-size:0.98rem; color:{primary}; font-weight:600;">ⓘ What is profileon?</span>
        <div class="tooltip">
            <b style="color:{navy};">profileon</b> helps you profile Databricks tables, pipelines, and schemas, and automatically generates DQX data quality rules.<br><br>
            <ul style="margin-left:18px;">
                <li>Supports <b>table, pipeline, schema, and catalog</b> modes</li>
                <li>Column-level profiling and rule customization</li>
                <li>Exports directly to Databricks Lakehouse</li>
            </ul>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Main App Title ---
st.markdown(
    f"""
    <div style="background:{navy}; padding:36px 0 26px 0; border-radius:18px; border:2.5px solid {primary};">
        <h1 style="color:{primary}; font-size:3.1rem; font-family: 'Segoe UI', sans-serif; font-weight: 800; text-align:center; margin-bottom:6px; text-transform: lowercase; letter-spacing: 0.5px;">
            profileon
        </h1>
        <div style="color:{cloud}; font-size:1.45rem; font-weight: 600; text-align: center; margin-top: 0px; letter-spacing:0.3px;">
            turn on insight, turn on trust.
        </div>
        <hr style="margin-top:22px; border:0; border-top:2px solid {primary}; width:58%;">
    </div>
    """,
    unsafe_allow_html=True
)

# --- Usage Summary ---
st.markdown(
    f"""
    <div style="background:{cloud}; color:{navy}; border-radius:12px; padding:27px 32px; margin-top:30px; font-size:1.13rem; border:1.6px solid {saffron};">
        <b>how it works:</b>
        <ol style="margin: 12px 0 0 22px;">
            <li>Select a profiling mode: <b>table</b>, <b>pipeline</b>, <b>schema</b>, or <b>catalog</b></li>
            <li>Choose targets and optionally narrow columns</li>
            <li>Configure profiling options for DQX rule generation</li>
            <li>Generate, review, and export your rules</li>
        </ol>
        <div style="margin-top:13px;">
            <b>Platform support:</b> Databricks with Unity Catalog + DQX (PySpark required).
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Import and Run Wizard Page ---
from profileon.pages.generate_dqx_rules import main as main_wizard_page
main_wizard_page()

# --- Contact Section ---
st.markdown(
    f"""
    <hr style="margin:36px 0 16px 0; border:0; border-top:2px solid {scarlett};">
    <div style="color:{cloud}; background:{navy}; font-size:1.07rem; border-radius:10px; padding:20px 30px; border:1.7px solid {scarlett};">
        <b>Questions or ideas?</b><br>
        Email <a href="mailto:levigagne0@gmail.com" style="color:{primary}; font-weight:600;">levigagne0@gmail.com</a>
        or contact <span style="color:{saffron}; font-weight:600;">Levi Gagne</span> directly.<br>
        You’re welcome to open a pull request or just DM me. I want this app to work for real people—let me know what you need.
    </div>
    """,
    unsafe_allow_html=True
)