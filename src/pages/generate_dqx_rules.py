# profileon/src/pages/generate_dqx_rules.py

import streamlit as st
import os
import sys

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.profile_options_docs import profile_option_docs

class DQRuleWizard:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.ws = WorkspaceClient()
        self.mode = st.session_state.get("dq_mode")
        self.name_param = st.session_state.get("dq_name_param")
        self.selected_tables = st.session_state.get("dq_selected_tables", [])
        self.profile_options = st.session_state.get("dq_profile_options", {})
        self.exclude_pattern = st.session_state.get("dq_exclude_pattern")
        self.valid = False

    def render_mode(self):
        modes = ["table", "pipeline", "schema", "catalog"]
        mode = st.selectbox("Profiling Mode", modes, key="dq_mode")
        self.mode = mode

    def render_params(self):
        if self.mode == "table":
            fqtn = st.text_input("Table Name (catalog.schema.table)", key="dq_name_param")
            if fqtn and self._validate_fqtn(fqtn):
                self.name_param = fqtn
                tables = [fqtn]
                st.session_state["dq_selected_tables"] = tables
                self.valid = True
                cols = self._get_columns(fqtn)
                selected_columns = st.multiselect("Columns to Profile", cols, default=cols)
                st.session_state["dq_selected_columns"] = selected_columns
            else:
                st.info("Enter a valid table name (catalog.schema.table).")
                self.valid = False

        elif self.mode == "pipeline":
            pipelines = [p.name for p in self.ws.pipelines.list_pipelines()]
            pipeline_name = st.selectbox("Pipeline", pipelines, key="dq_name_param")
            if pipeline_name:
                self.name_param = pipeline_name
                tables = self._get_pipeline_tables(pipeline_name)
                selected_tables = st.multiselect("Tables in Pipeline", tables, default=tables)
                st.session_state["dq_selected_tables"] = selected_tables
                self.exclude_pattern = st.text_input("Exclude Table Pattern (optional, e.g. .tamarack_*)", key="dq_exclude_pattern")
                self.valid = bool(selected_tables)
            else:
                self.valid = False

        elif self.mode == "schema":
            catalogs = [r.catalog for r in self.spark.sql("SHOW CATALOGS").collect()]
            catalog = st.selectbox("Catalog", catalogs, key="dq_schema_catalog")
            schemas = [r.namespace for r in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
            schema = st.selectbox("Schema", schemas, key="dq_schema_schema")
            if catalog and schema:
                self.name_param = f"{catalog}.{schema}"
                tables = self._get_tables_for_schema(catalog, schema)
                selected_tables = st.multiselect("Tables in Schema", tables, default=tables)
                st.session_state["dq_selected_tables"] = selected_tables
                self.exclude_pattern = st.text_input("Exclude Table Pattern (optional, e.g. .tamarack_*)", key="dq_exclude_pattern")
                self.valid = bool(selected_tables)
            else:
                self.valid = False

        elif self.mode == "catalog":
            catalogs = [r.catalog for r in self.spark.sql("SHOW CATALOGS").collect()]
            catalog = st.selectbox("Catalog", catalogs, key="dq_catalog_catalog")
            if catalog:
                self.name_param = catalog
                tables = self._get_tables_for_catalog(catalog)
                selected_tables = st.multiselect("Tables in Catalog", tables, default=tables)
                st.session_state["dq_selected_tables"] = selected_tables
                self.exclude_pattern = st.text_input("Exclude Table Pattern (optional, e.g. .tamarack_*)", key="dq_exclude_pattern")
                self.valid = bool(selected_tables)
            else:
                self.valid = False

    def render_profile_options(self):
        st.subheader("Profile Options")
        opts = {}
        for opt, doc in profile_option_docs.items():
            default = doc["default"]
            short = doc["short"]
            if isinstance(default, bool):
                opts[opt] = st.checkbox(opt, value=default, help=short)
            elif isinstance(default, int):
                opts[opt] = st.number_input(opt, value=default, step=1, help=short)
            elif isinstance(default, float):
                opts[opt] = st.number_input(opt, value=default, min_value=0.0, max_value=1.0, step=0.01, help=short)
            elif isinstance(default, list):
                # Only enable for table mode
                if self.mode == "table" and "dq_selected_tables" in st.session_state and st.session_state["dq_selected_tables"]:
                    cols = self._get_columns(st.session_state["dq_selected_tables"][0])
                    opts[opt] = st.multiselect(opt, cols, default=[], help=short)
                else:
                    opts[opt] = st.text_area(opt + " (comma-separated)", value="", help=short)
            else:
                opts[opt] = st.text_input(opt, value=str(default), help=short)
        st.session_state["dq_profile_options"] = opts
        self.profile_options = opts

    def render_run(self):
        if self.valid:
            if st.button("Run Rules", type="primary"):
                st.success("Ready to run rules!")
                # Call your Spark/DQX logic here

    # --- Data/validation helpers ---
    def _validate_fqtn(self, fqtn):
        parts = fqtn.split('.')
        return len(parts) == 3 and all(parts)

    def _get_columns(self, fqtn):
        try:
            return [c for c, t in self.spark.table(fqtn).dtypes]
        except Exception:
            return []

    def _get_pipeline_tables(self, pipeline_name):
        pls = list(self.ws.pipelines.list_pipelines())
        pl = next((p for p in pls if p.name == pipeline_name), None)
        if not pl:
            return []
        latest_update = pl.latest_updates[0].update_id
        events = self.ws.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=250)
        return sorted({getattr(ev.origin, "flow_name", None)
                       for ev in events
                       if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)} - {None})

    def _get_tables_for_schema(self, catalog, schema):
        try:
            return [row.tableName for row in self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()]
        except Exception:
            return []

    def _get_tables_for_catalog(self, catalog):
        try:
            schemas = [row.namespace for row in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]
            all_tables = []
            for s in schemas:
                tbls = self.spark.sql(f"SHOW TABLES IN {catalog}.{s}").collect()
                all_tables += [f"{catalog}.{s}.{row.tableName}" for row in tbls]
            return all_tables
        except Exception:
            return []

def main():
    wizard = DQRuleWizard()
    wizard.render_mode()
    wizard.render_params()
    if wizard.valid:
        wizard.render_profile_options()
        wizard.render_run()
    else:
        st.warning("Fill in all required fields to proceed.")

if __name__ == "__main__":
    main()
