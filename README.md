# Profileon

## Overview

**Profileon** is a Databricks-native Streamlit application for profiling Unity Catalog tables and generating [DQX](https://docs.databricks.com/en/delta-live-tables/dq-expectations.html)-compatible data quality rules. It provides a guided interface for data engineers and analysts to select tables, configure profiling, review results, and export YAML/JSON rules for automated pipeline enforcement.

- **Runs natively on any Databricks cluster.**
- **Full Spark compute:** All profiling leverages the live Spark session—nothing runs locally or in memory.
- **Deep Unity Catalog and workspace integration.**
- **CLA-compliant UI:** Full branding, CLA color palette, and UX.

---

## Features

- **Table/Pipeline/Schema Discovery:**  
  Auto-discovers tables and DLT pipelines from Unity Catalog, Databricks SDK, or Spark SQL.

- **Flexible Profiling Options:**  
  All options (thresholds, checks, types) are driven by config files and rendered as interactive widgets.

- **Column Selection:**  
  Pick which columns to profile or apply rules to.

- **DQX Rule Generation:**  
  Produces DQX rules as YAML/JSON—download directly or export to workspace/volume.

- **Workspace Integration:**  
  Export generated rules to Volumes or external workspace paths for automated downstream use.

- **CLA Branding:**  
  Uses [cla_color_config.py](src/profileon/config/cla_color_config.py) for colors, fonts, and panel styling.

---

## Architecture

```plaintext
src/profileon/
├── app.py               # Entrypoint. Handles routing and state.
├── config/              # Color and profiling docs/configs.
├── profiling/           # Spark logic and rule generator.
├── databricks/          # SDK + UC access/discovery.
├── ui/                  # Widgets, info panels, help, branding.
├── utils/               # File export, session state, helpers.
└── pages/               # (Optional) Streamlit multi-page wizard.