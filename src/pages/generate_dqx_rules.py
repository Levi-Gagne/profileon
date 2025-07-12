import streamlit as st

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from profileon.config.profile_options_docs import profile_option_docs
from profileon.databricks.pipeline_discovery import discover_output_tables
from profileon.databricks.uc_utils import UCUtils
# Import TableProfiler, RuleGenerator as needed

class DQRuleWizard:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.ws = WorkspaceClient()
        self.uc = UCUtils(self.spark)
        self._load_session_state()

    def _load_session_state(self):
        self.mode = st.session_state.get("dq_mode")
        self.name_param = st.session_state.get("dq_name_param")
        self.selected_tables = st.session_state.get("dq_selected_tables", [])
        self.selected_columns = st.session_state.get("dq_selected_columns", [])
        self.profile_options = st.session_state.get("dq_profile_options", {})
        self.exclude_pattern = st.session_state.get("dq_exclude_pattern")
        self.valid = False

    def render_mode(self):
        modes = ["table", "pipeline", "schema", "catalog"]
        mode = st.selectbox("Profiling Mode", modes, key="dq_mode")
        self.mode = mode
        st.session_state["dq_mode"] = mode

    def render_params(self):
        if self.mode == "table":
            fqtn = st.text_input("Table Name (catalog.schema.table)", key="dq_name_param")
            if fqtn and self._validate_fqtn(fqtn):
                self.name_param = fqtn
                tables = [fqtn]
                st.session_state["dq_selected_tables"] = tables
                self.valid = True
                cols = self.uc.get_table_columns(fqtn)
                selected_columns = st.multiselect("Columns to Profile", [c["name"] for c in cols], default=[c["name"] for c in cols])
                st.session_state["dq_selected_columns"] = selected_columns
            else:
                st.info("Enter a valid table name (catalog.schema.table).")
                self.valid = False

        elif self.mode == "pipeline":
            pipelines = [p.name for p in self.ws.pipelines.list_pipelines()]
            pipeline_name = st.selectbox("Pipeline", pipelines, key="dq_name_param")
            if pipeline_name:
                self.name_param = pipeline_name
                tables = discover_output_tables(pipeline_name, sdk_client=self.ws)
                selected_tables = st.multiselect("Tables in Pipeline", tables, default=tables)
                st.session_state["dq_selected_tables"] = selected_tables
                self.exclude_pattern = st.text_input("Exclude Table Pattern (optional, e.g. .tamarack_*)", key="dq_exclude_pattern")
                self.valid = bool(selected_tables)
            else:
                self.valid = False

        elif self.mode == "schema":
            catalogs = self.uc.list_catalogs()
            catalog = st.selectbox("Catalog", catalogs, key="dq_schema_catalog")
            schemas = self.uc.list_schemas(catalog)
            schema = st.selectbox("Schema", schemas, key="dq_schema_schema")
            if catalog and schema:
                self.name_param = f"{catalog}.{schema}"
                tables = [f"{catalog}.{schema}.{row['name']}" for row in self.uc.list_tables(catalog, schema)]
                selected_tables = st.multiselect("Tables in Schema", tables, default=tables)
                st.session_state["dq_selected_tables"] = selected_tables
                self.exclude_pattern = st.text_input("Exclude Table Pattern (optional, e.g. .tamarack_*)", key="dq_exclude_pattern")
                self.valid = bool(selected_tables)
            else:
                self.valid = False

        elif self.mode == "catalog":
            catalogs = self.uc.list_catalogs()
            catalog = st.selectbox("Catalog", catalogs, key="dq_catalog_catalog")
            if catalog:
                self.name_param = catalog
                schemas = self.uc.list_schemas(catalog)
                all_tables = []
                for schema in schemas:
                    all_tables += [f"{catalog}.{schema}.{row['name']}" for row in self.uc.list_tables(catalog, schema)]
                selected_tables = st.multiselect("Tables in Catalog", all_tables, default=all_tables)
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
                    cols = self.uc.get_table_columns(st.session_state["dq_selected_tables"][0])
                    opts[opt] = st.multiselect(opt, [c["name"] for c in cols], default=[], help=short)
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
                # Insert DQX logic, profiling, and rule generation here
                # For now, this is where you’d call TableProfiler, RuleGenerator, etc.

    def _validate_fqtn(self, fqtn):
        parts = fqtn.split('.')
        return len(parts) == 3 and all(parts)

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