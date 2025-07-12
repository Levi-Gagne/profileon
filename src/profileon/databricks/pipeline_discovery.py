# src/profileon/databricks/pipeline_discovery.py

from typing import List, Optional

try:
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    WorkspaceClient = None
    print(f"[ERROR] databricks.sdk import failed: {e}")


def discover_output_tables(
    pipeline_name: str,
    sdk_client: Optional[object] = None
) -> List[str]:
    """
    Uses Databricks SDK to discover all fully-qualified tables written by a given DLT pipeline.
    Relies on event logs, and always runs live. Suitable for production.

    Args:
        pipeline_name (str): Name of the pipeline as it appears in Databricks.
        sdk_client (WorkspaceClient, optional): Allow injection of WorkspaceClient for testing or reuse.

    Returns:
        List[str]: Sorted list of unique output table names in catalog.schema.table format.

    Raises:
        RuntimeError: If SDK is missing, or no matching pipeline/tables are found.
    """
    if WorkspaceClient is None and sdk_client is None:
        print("[ERROR] Databricks SDK not installed. Cannot run discovery.")
        raise RuntimeError("Databricks SDK is required for pipeline discovery.")

    w = sdk_client or WorkspaceClient()
    print(f"[INFO] Using WorkspaceClient: {type(w).__name__}")

    # Find pipeline by name
    pipelines = list(w.pipelines.list_pipelines())
    pl = next((p for p in pipelines if p.name == pipeline_name), None)
    if not pl:
        print(f"[ERROR] Pipeline '{pipeline_name}' not found.")
        raise RuntimeError(f"Pipeline '{pipeline_name}' not found via SDK.")

    if not pl.latest_updates:
        print(f"[ERROR] No updates found for pipeline '{pipeline_name}'.")
        raise RuntimeError(f"Pipeline '{pipeline_name}' has no update history.")

    latest_update_id = pl.latest_updates[0].update_id
    print(f"[INFO] Using latest update ID: {latest_update_id}")

    # Pull event logs
    try:
        events = w.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=250)
    except Exception as e:
        print(f"[ERROR] Failed to list pipeline events: {e}")
        raise RuntimeError(f"Failed to list events for pipeline '{pipeline_name}': {e}")

    tables = set()
    empty_pages = 0
    buffer = []
    it = iter(events)

    while True:
        buffer.clear()
        try:
            for _ in range(250):
                buffer.append(next(it))
        except StopIteration:
            pass

        page_tables = {
            getattr(ev.origin, "flow_name", None)
            for ev in buffer
            if getattr(ev.origin, "update_id", None) == latest_update_id
               and getattr(ev.origin, "flow_name", None)
        }
        page_tables.discard(None)

        if page_tables:
            tables |= page_tables
            empty_pages = 0
            print(f"[DEBUG] Found tables in this page: {sorted(page_tables)}")
        else:
            empty_pages += 1
            print(f"[DEBUG] No tables found. Empty pages so far: {empty_pages}")

        if empty_pages >= 2 or not buffer:
            break

    result = sorted(tables)
    if not result:
        print(f"[ERROR] No output tables found for pipeline '{pipeline_name}'.")
        raise RuntimeError(
            f"No output tables discovered for pipeline '{pipeline_name}' using event logs."
        )

    print(f"[INFO] Found {len(result)} tables: {result}")
    return result