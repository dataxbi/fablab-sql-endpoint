"""
provision/capacity_manager.py
Fabric capacity lifecycle management: pause and resume via Azure CLI (az rest).
Polls the Fabric REST API until the capacity reaches the expected state.
Used by setup_fabric.py and benchmark/runner.py.
"""

import subprocess
import json
import sys
import time
import logging
from enum import Enum

logger = logging.getLogger(__name__)

FABRIC_API = "https://api.fabric.microsoft.com/v1"
AZURE_MGMT_API = "https://management.azure.com"
POLL_INTERVAL_SEC = 15
DEFAULT_TIMEOUT_SEC = 600  # 10 minutes


class CapacityState(str, Enum):
    ACTIVE = "Active"
    PAUSED = "Paused"
    PAUSING = "Pausing"
    RESUMING = "Resuming"
    SCALING = "Scaling"
    FAILED = "Failed"

# On Windows, az is a batch script (.cmd) and must be invoked explicitly
_AZ_CMD = "az.cmd" if sys.platform == "win32" else "az"


def _az_rest(method: str, url: str, body: dict | None = None) -> dict:
    """Execute an az rest call and return the parsed JSON response."""
    cmd = [_AZ_CMD, "rest", "--method", method, "--url", url]
    if body:
        cmd += ["--body", json.dumps(body)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"az rest {method.upper()} {url} failed:\n{result.stderr.strip()}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def get_capacity_state(subscription_id: str, resource_group: str, capacity_name: str) -> CapacityState:
    """Return the current state of a Fabric capacity via Azure Resource Manager."""
    url = (
        f"{AZURE_MGMT_API}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}"
        f"?api-version=2023-11-01"
    )
    data = _az_rest("get", url)
    state = data.get("properties", {}).get("state", "Unknown")
    return CapacityState(state)


def _wait_for_state(
    subscription_id: str,
    resource_group: str,
    capacity_name: str,
    target_state: CapacityState,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> None:
    """Poll until the capacity reaches target_state or timeout is exceeded."""
    elapsed = 0
    logger.info(
        "Waiting for capacity '%s' to reach state '%s' (timeout: %ds)...",
        capacity_name, target_state.value, timeout_sec,
    )
    while elapsed < timeout_sec:
        state = get_capacity_state(subscription_id, resource_group, capacity_name)
        logger.info("  Current state: %s (%ds elapsed)", state.value, elapsed)
        if state == target_state:
            logger.info("Capacity reached state '%s'.", target_state.value)
            return
        if state == CapacityState.FAILED:
            raise RuntimeError(
                f"Capacity '{capacity_name}' entered FAILED state while waiting for {target_state.value}."
            )
        time.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC

    raise TimeoutError(
        f"Capacity '{capacity_name}' did not reach '{target_state.value}' within {timeout_sec}s. "
        f"Last known state: {get_capacity_state(subscription_id, resource_group, capacity_name).value}"
    )


def pause_capacity(
    subscription_id: str,
    resource_group: str,
    capacity_name: str,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> None:
    """Pause a Fabric capacity and wait until it reaches the Paused state."""
    state = get_capacity_state(subscription_id, resource_group, capacity_name)
    if state == CapacityState.PAUSED:
        logger.info("Capacity '%s' is already Paused.", capacity_name)
        return

    logger.info("Pausing capacity '%s'...", capacity_name)
    url = (
        f"{AZURE_MGMT_API}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend"
        f"?api-version=2023-11-01"
    )
    _az_rest("post", url)
    _wait_for_state(subscription_id, resource_group, capacity_name, CapacityState.PAUSED, timeout_sec)
    logger.info("Capacity '%s' paused successfully.", capacity_name)


def resume_capacity(
    subscription_id: str,
    resource_group: str,
    capacity_name: str,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> None:
    """Resume a Fabric capacity and wait until it reaches the Active state."""
    state = get_capacity_state(subscription_id, resource_group, capacity_name)
    if state == CapacityState.ACTIVE:
        logger.info("Capacity '%s' is already Active.", capacity_name)
        return

    logger.info("Resuming capacity '%s'...", capacity_name)
    url = (
        f"{AZURE_MGMT_API}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}/resume"
        f"?api-version=2023-11-01"
    )
    _az_rest("post", url)
    _wait_for_state(subscription_id, resource_group, capacity_name, CapacityState.ACTIVE, timeout_sec)
    logger.info("Capacity '%s' resumed successfully.", capacity_name)
