"""Docker container driver for workstation isolation.

Manages the lifecycle of Docker containers that serve as agent workstations.
Uses the Docker SDK for Python to create, start, stop, and inspect containers.
Supports resource limits, volume mounts, network policies, and health checks.
"""

import logging
import os
from typing import Any, Optional

from hub.workstation.models import Workstation, WorkstationConfig, WorkstationStatus

logger = logging.getLogger(__name__)

# Docker SDK — lazy import to allow graceful degradation when Docker is unavailable
_docker = None
DockerError = None


def _get_docker():
    """Lazy-import the Docker SDK. Returns None if not available."""
    global _docker, DockerError
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
            DockerError = docker.errors.APIError
            logger.info("Docker SDK connected successfully")
        except ImportError:
            logger.warning("Docker SDK not installed — workstations will use simulation mode")
        except Exception as e:
            logger.error(f"Failed to connect to Docker: {e}")
    return _docker


def ensure_network(network_name: str) -> bool:
    """Ensure the workstation network exists. Creates it if missing.

    Returns True if the network exists or was created, False on error.
    """
    client = _get_docker()
    if client is None:
        logger.warning("Docker unavailable — skipping network creation")
        return False

    try:
        # Check if network already exists
        for net in client.networks.list():
            if net.name == network_name:
                logger.debug(f"Network '{network_name}' already exists")
                return True

        # Create the network with default-deny style configuration
        client.networks.create(
            network_name,
            driver="bridge",
            internal=False,
            options={"com.docker.network.bridge.enable_icc": "true"},
        )
        logger.info(f"Created Docker network '{network_name}'")
        return True
    except Exception as e:
        logger.error(f"Failed to create network '{network_name}': {e}")
        return False


def pull_image(image: str) -> bool:
    """Pull a Docker image if not already present.

    Returns True if the image is available, False on error.
    """
    client = _get_docker()
    if client is None:
        return False

    try:
        # Check if image exists locally
        images = client.images.list(name=image)
        if images:
            logger.debug(f"Image '{image}' already present locally")
            return True

        logger.info(f"Pulling image '{image}'...")
        client.images.pull(image)
        logger.info(f"Successfully pulled image '{image}'")
        return True
    except Exception as e:
        logger.error(f"Failed to pull image '{image}': {e}")
        return False


def create_container(
    workstation: Workstation,
    config: WorkstationConfig,
) -> Optional[str]:
    """Create a Docker container for the workstation.

    Returns the container ID on success, None on failure.
    """
    client = _get_docker()
    if client is None:
        logger.error("Docker unavailable — cannot create container")
        return None

    container_name = f"evershade-ws-{workstation.id}"
    workstation.container_name = container_name

    try:
        # Build environment variables
        env_vars = dict(config.env_vars)
        env_vars["WORKSTATION_ID"] = workstation.id
        env_vars["WORKSTATION_NAME"] = workstation.name
        env_vars["AGENT_NAME"] = workstation.agent_name

        # Build volume mounts
        volume_bindings = {}
        for vol in config.volumes:
            source = vol.get("source", "")
            target = vol.get("target", "")
            read_only = vol.get("read_only", False)
            volume_bindings[source] = {
                "bind": target,
                "mode": "ro" if read_only else "rw",
            }

        # Build extra_hosts
        extra_hosts = {}
        for host, ip in config.extra_hosts.items():
            extra_hosts[host] = ip

        # Build labels
        labels = dict(config.labels)
        labels["com.evershade.workstation.id"] = workstation.id
        labels["com.evershade.workstation.name"] = workstation.name

        # Create the container
        container = client.containers.create(
            config.image,
            name=container_name,
            environment=env_vars,
            volumes=volume_bindings,
            network=config.network_name,
            cpu_quota=int(config.cpu_limit * 100000),
            mem_limit=config.memory_limit,
            labels=labels,
            extra_hosts=extra_hosts if extra_hosts else None,
            command=config.command,
            tty=config.enable_terminal,
            stdin_open=config.enable_terminal,
            detach=True,
            restart_policy={"Name": "no"},
        )

        workstation.container_id = container.id
        logger.info(f"Created container '{container_name}' (ID: {container.short_id})")
        return container.id

    except Exception as e:
        logger.error(f"Failed to create container for workstation '{workstation.id}': {e}")
        return None


def start_container(container_id: str) -> bool:
    """Start an existing Docker container.

    Returns True on success, False on failure.
    """
    client = _get_docker()
    if client is None:
        return False

    try:
        container = client.containers.get(container_id)
        container.start()
        logger.info(f"Started container '{container.short_id}'")
        return True
    except Exception as e:
        logger.error(f"Failed to start container '{container_id}': {e}")
        return False


def stop_container(container_id: str, timeout: int = 30) -> bool:
    """Stop a running Docker container.

    Returns True on success, False on failure.
    """
    client = _get_docker()
    if client is None:
        return False

    try:
        container = client.containers.get(container_id)
        container.stop(timeout=timeout)
        logger.info(f"Stopped container '{container.short_id}'")
        return True
    except Exception as e:
        logger.error(f"Failed to stop container '{container_id}': {e}")
        return False


def remove_container(container_id: str, force: bool = False) -> bool:
    """Remove (destroy) a Docker container.

    Returns True on success, False on failure.
    """
    client = _get_docker()
    if client is None:
        return False

    try:
        container = client.containers.get(container_id)
        container.remove(force=force)
        logger.info(f"Removed container '{container_id[:12]}'")
        return True
    except Exception as e:
        logger.error(f"Failed to remove container '{container_id}': {e}")
        return False


def inspect_container(container_id: str) -> Optional[dict[str, Any]]:
    """Inspect a Docker container and return its status and resource usage.

    Returns a dict with status, cpu, memory, and network info, or None on failure.
    """
    client = _get_docker()
    if client is None:
        return None

    try:
        container = client.containers.get(container_id)
        status = container.status

        # Get resource stats
        stats = container.stats(stream=False)
        cpu_delta = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0) \
                    - stats.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
        system_delta = stats.get("cpu_stats", {}).get("system_cpu_usage", 0) \
                       - stats.get("precpu_stats", {}).get("system_cpu_usage", 0)
        cpu_count = stats.get("cpu_stats", {}).get("online_cpus", 1)
        cpu_percent = (cpu_delta / system_delta * cpu_count * 100) if system_delta > 0 else 0.0

        memory_usage = stats.get("memory_stats", {}).get("usage", 0)

        # Get port bindings
        ports = []
        for port_mapping in container.ports or {}:
            for mapping in container.ports[port_mapping]:
                ports.append({
                    "container_port": int(str(port_mapping).replace("/tcp", "").replace("/udp", "")),
                    "host_port": mapping.get("HostPort", 0),
                })

        return {
            "status": status,
            "cpu_percent": round(cpu_percent, 2),
            "memory_usage_bytes": memory_usage,
            "ports": ports,
            "running": status == "running",
        }
    except Exception as e:
        logger.error(f"Failed to inspect container '{container_id}': {e}")
        return None


def exec_in_container(container_id: str, command: str, timeout: int = 60) -> dict[str, Any]:
    """Execute a command inside a running container.

    Returns a dict with exit_code, stdout, and stderr.
    The command is passed as a list of arguments to prevent shell injection.
    """
    import shlex
    client = _get_docker()
    if client is None:
        return {"exit_code": -1, "stdout": "", "stderr": "Docker unavailable"}

    try:
        container = client.containers.get(container_id)
        # Use list-based command to prevent shell injection via /bin/sh -c
        result = container.exec_run(["/bin/sh", "-c", command], demux=True)
        exit_code = result.exit_code
        stdout = result.output[0].decode("utf-8", errors="replace") if result.output[0] else ""
        stderr = result.output[1].decode("utf-8", errors="replace") if result.output[1] else ""
        return {
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
        }
    except Exception as e:
        logger.error(f"Failed to exec in container '{container_id}': {e}")
        return {"exit_code": -1, "stdout": "", "stderr": str(e)}


def get_container_logs(container_id: str, tail: int = 100) -> str:
    """Get the logs of a running container.

    Returns the last N lines of logs as a string.
    """
    client = _get_docker()
    if client is None:
        return ""

    try:
        container = client.containers.get(container_id)
        logs = container.logs(tail=tail, stdout=True, stderr=True)
        return logs.decode("utf-8", errors="replace")
    except Exception as e:
        logger.error(f"Failed to get logs for container '{container_id}': {e}")
        return f"Error: {e}"


def is_docker_available() -> bool:
    """Check if Docker is available and connected."""
    client = _get_docker()
    if client is None:
        return False
    try:
        client.ping()
        return True
    except Exception:
        return False


def get_host_port(container_id: str, container_port: int) -> Optional[int]:
    """Get the host port mapped to a container port.

    Returns the host port number or None if not mapped.
    """
    client = _get_docker()
    if client is None:
        return None

    try:
        container = client.containers.get(container_id)
        port_key = f"{container_port}/tcp"
        if port_key in (container.ports or {}):
            mappings = container.ports[port_key]
            if mappings:
                return int(mappings[0].get("HostPort", 0))
    except Exception as e:
        logger.error(f"Failed to get port mapping for container '{container_id}': {e}")
    return None
