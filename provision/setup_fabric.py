"""
provision/setup_fabric.py
Create a Fabric workspace, Lakehouse and Warehouse via Azure CLI (az rest).
Prints the SQL endpoint connection strings when done.

Usage:
    py provision/setup_fabric.py [--workspace NAME] [--lh NAME] [--wh NAME]

Environment variables (used as defaults if args not provided):
    FABRIC_WORKSPACE_NAME   default: FabLab_SQL_Endpoint
    FABRIC_LAKEHOUSE_NAME   default: LH_01
    FABRIC_WAREHOUSE_NAME   default: WH_01
    FABRIC_CAPACITY_ID      optional: attach workspace to a specific capacity
"""

import argparse
import json
import logging
import os
import subprocess
import sys

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

FABRIC_API = "https://api.fabric.microsoft.com/v1"


# ---------------------------------------------------------------------------
# Azure CLI helpers
# ---------------------------------------------------------------------------

def _az_rest(method: str, url: str, body: dict | None = None) -> dict:
    cmd = [
        "az", "rest", "--method", method, "--url", url,
        "--resource", "https://api.fabric.microsoft.com",
    ]
    if body:
        cmd += ["--body", json.dumps(body)]
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"az rest {method.upper()} {url} failed:\n{result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _ensure_az_login() -> None:
    result = subprocess.run(["az", "account", "show"], capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        logger.info("Not logged in to Azure CLI. Running az login...")
        subprocess.run(["az", "login"], check=True, shell=True)


# ---------------------------------------------------------------------------
# Fabric resource helpers
# ---------------------------------------------------------------------------

def _find_workspace(name: str) -> dict | None:
    """Return the workspace dict if it exists, else None."""
    data = _az_rest("get", f"{FABRIC_API}/workspaces")
    for ws in data.get("value", []):
        if ws.get("displayName") == name:
            return ws
    return None


def _create_workspace(name: str, capacity_id: str | None) -> dict:
    body: dict = {"displayName": name}
    if capacity_id:
        body["capacityId"] = capacity_id
    return _az_rest("post", f"{FABRIC_API}/workspaces", body)


def _find_item(workspace_id: str, item_type: str, name: str) -> dict | None:
    data = _az_rest("get", f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}")
    for item in data.get("value", []):
        if item.get("displayName") == name:
            return item
    return None


def _create_item(workspace_id: str, item_type: str, name: str) -> dict:
    body = {"displayName": name, "type": item_type}
    return _az_rest("post", f"{FABRIC_API}/workspaces/{workspace_id}/items", body)


def _get_sql_endpoint(workspace_id: str, item_id: str, item_type: str) -> dict:
    """Retrieve SQL endpoint connection details for a Lakehouse or Warehouse."""
    type_path = "lakehouses" if item_type == "Lakehouse" else "warehouses"
    data = _az_rest("get", f"{FABRIC_API}/workspaces/{workspace_id}/{type_path}/{item_id}")
    props = data.get("properties", {})
    if item_type == "Lakehouse":
        return props.get("sqlEndpointProperties", {})
    # Warehouse: connectionString is at top-level of properties
    return {
        "connectionString": props.get("connectionString", ""),
        "id": data.get("id", ""),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Provision Fabric workspace, Lakehouse and Warehouse."
    )
    parser.add_argument(
        "--workspace",
        default=os.getenv("FABRIC_WORKSPACE_NAME", "FabLab_SQL_Endpoint"),
        help="Fabric workspace display name (default: FabLab_SQL_Endpoint)",
    )
    parser.add_argument(
        "--lh",
        default=os.getenv("FABRIC_LAKEHOUSE_NAME", "LH_01"),
        help="Lakehouse display name (default: LH_01)",
    )
    parser.add_argument(
        "--wh",
        default=os.getenv("FABRIC_WAREHOUSE_NAME", "WH_01"),
        help="Warehouse display name (default: WH_01)",
    )
    parser.add_argument(
        "--capacity-id",
        default=os.getenv("FABRIC_CAPACITY_ID"),
        help="Fabric capacity ID to attach to the workspace (optional)",
    )
    args = parser.parse_args()

    _ensure_az_login()

    # --- Workspace ---
    ws = _find_workspace(args.workspace)
    if ws:
        logger.info("Workspace '%s' already exists (id: %s).", args.workspace, ws["id"])
    else:
        logger.info("Creating workspace '%s'...", args.workspace)
        ws = _create_workspace(args.workspace, args.capacity_id)
        logger.info("Workspace created (id: %s).", ws["id"])
    workspace_id = ws["id"]

    # --- Lakehouse ---
    lh = _find_item(workspace_id, "Lakehouse", args.lh)
    if lh:
        logger.info("Lakehouse '%s' already exists (id: %s).", args.lh, lh["id"])
    else:
        logger.info("Creating Lakehouse '%s'...", args.lh)
        lh = _create_item(workspace_id, "Lakehouse", args.lh)
        logger.info("Lakehouse created (id: %s).", lh["id"])

    # --- Warehouse ---
    wh = _find_item(workspace_id, "Warehouse", args.wh)
    if wh:
        logger.info("Warehouse '%s' already exists (id: %s).", args.wh, wh["id"])
    else:
        logger.info("Creating Warehouse '%s'...", args.wh)
        wh = _create_item(workspace_id, "Warehouse", args.wh)
        logger.info("Warehouse created (id: %s).", wh["id"])

    # --- SQL endpoint info ---
    lh_ep = _get_sql_endpoint(workspace_id, lh["id"], "Lakehouse")
    wh_ep = _get_sql_endpoint(workspace_id, wh["id"], "Warehouse")

    print("\n" + "=" * 60)
    print("PROVISIONING COMPLETE")
    print("=" * 60)
    print(f"Workspace     : {args.workspace}  (id: {workspace_id})")
    print()
    print(f"Lakehouse     : {args.lh}  (id: {lh['id']})")
    if lh_ep:
        print(f"  Server      : {lh_ep.get('connectionString', 'n/a')}")
        print(f"  Database    : {lh_ep.get('id', 'n/a')}")
    print()
    print(f"Warehouse     : {args.wh}  (id: {wh['id']})")
    if wh_ep:
        print(f"  Server      : {wh_ep.get('connectionString', 'n/a')}")
        print(f"  Database    : {wh_ep.get('id', 'n/a')}")
    print("=" * 60)
    print("Copy the values above into your .env file.")


if __name__ == "__main__":
    main()
