"""Remote execution support for running agents on remote machines via SSH.

Provides SSH connection management, project synchronization, and remote
command execution with timeout and output streaming.
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class RemoteConfig:
    """Configuration for a remote execution target."""
    host: str
    user: str = "root"
    port: int = 22
    key_path: Optional[str] = None
    remote_project_dir: Optional[str] = None
    remote_junie_path: Optional[str] = None
    sync_before_run: bool = True
    connection_timeout: int = 30
    command_timeout: int = 1800
    strict_host_key_checking: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["RemoteConfig"]:
        """Create RemoteConfig from a dict. Returns None if not configured."""
        if not data or not data.get("host"):
            return None
        return cls(
            host=data["host"],
            user=data.get("user", "root"),
            port=data.get("port", 22),
            key_path=data.get("key_path"),
            remote_project_dir=data.get("remote_project_dir"),
            remote_junie_path=data.get("remote_junie_path"),
            sync_before_run=data.get("sync_before_run", True),
            connection_timeout=data.get("connection_timeout", 30),
            command_timeout=data.get("command_timeout", 1800),
            strict_host_key_checking=data.get("strict_host_key_checking", True),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict."""
        return {
            "host": self.host,
            "user": self.user,
            "port": self.port,
            "key_path": self.key_path,
            "remote_project_dir": self.remote_project_dir,
            "remote_junie_path": self.remote_junie_path,
            "sync_before_run": self.sync_before_run,
            "connection_timeout": self.connection_timeout,
            "command_timeout": self.command_timeout,
            "strict_host_key_checking": self.strict_host_key_checking,
        }

    @property
    def remote_project(self) -> str:
        """Get the remote project directory, defaulting to ~/agent-project."""
        return self.remote_project_dir or "~/agent-project"


class RemoteError(Exception):
    """Raised when remote operations fail."""
    pass


class RemoteTarget:
    """Manages SSH connections and remote command execution.

    Uses SSH commands (ssh, scp) for portability — no paramiko dependency required.
    Falls back gracefully when SSH is not available.
    """

    def __init__(self, config: RemoteConfig):
        self.config = config
        self._ssh_available = self._check_ssh_available()

    def _check_ssh_available(self) -> bool:
        """Check if SSH client is available."""
        return shutil.which("ssh") is not None

    def is_available(self) -> bool:
        """Check if remote target is reachable."""
        if not self._ssh_available:
            return False
        try:
            result = self._run_ssh(
                "echo 'connected'",
                timeout=10,
            )
            return result.exit_code == 0
        except (RemoteError, subprocess.TimeoutExpired):
            return False

    def _ssh_args(self) -> List[str]:
        """Build base SSH arguments."""
        args = [
            "ssh",
            "-o", f"StrictHostKeyChecking={'yes' if self.config.strict_host_key_checking else 'no'}",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={self.config.connection_timeout}",
            "-p", str(self.config.port),
        ]
        if not self.config.strict_host_key_checking:
            args.extend(["-o", "UserKnownHostsFile=/dev/null"])
        if self.config.key_path:
            key = str(Path(self.config.key_path).expanduser())
            args.extend(["-i", key])
        return args

    def _run_ssh(
        self,
        command: str,
        timeout: Optional[int] = None,
    ) -> subprocess.CompletedProcess:
        """Run a command on the remote host via SSH."""
        if not self._ssh_available:
            raise RemoteError("SSH client not found on this system")

        remote_addr = f"{self.config.user}@{self.config.host}"
        args = self._ssh_args() + [remote_addr, command]

        try:
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout or self.config.command_timeout,
            )
            return result
        except FileNotFoundError:
            raise RemoteError("SSH binary not found — install OpenSSH client")
        except subprocess.TimeoutExpired:
            raise RemoteError(f"SSH command timed out after {timeout}s")

    def execute(
        self,
        command: str,
        timeout: Optional[int] = None,
    ) -> Tuple[int, str, str]:
        """Execute a command on the remote host.

        Args:
            command: The shell command to run remotely.
            timeout: Maximum execution time in seconds.

        Returns:
            Tuple of (exit_code, stdout, stderr).

        Raises:
            RemoteError: If SSH is unavailable or connection fails.
        """
        result = self._run_ssh(command, timeout=timeout)
        return result.returncode, result.stdout, result.stderr

    def sync_project(self, local_project_dir: str) -> bool:
        """Synchronize a local project directory to the remote host.

        Creates a tarball of the local project and extracts it remotely,
        excluding common unnecessary files (.git, __pycache__, node_modules, etc.).

        Args:
            local_project_dir: Path to the local project directory.

        Returns:
            True if sync succeeded, False otherwise.
        """
        if not self._ssh_available:
            logger.error("Cannot sync: SSH not available")
            return False

        local_path = Path(local_project_dir).expanduser().resolve()
        if not local_path.exists():
            logger.error(f"Local project directory does not exist: {local_path}")
            return False

        remote_dir = self.config.remote_project

        # Create tarball in memory, excluding unnecessary files
        exclude_patterns = {
            ".git", "__pycache__", "node_modules", ".venv", "venv",
            "*.pyc", "*.pyo", ".DS_Store", "Thumbs.db", "*.log",
            ".idea", ".vscode", "*.swp", "*.swo",
        }

        tar_buffer = BytesIO()
        try:
            with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
                for item in local_path.iterdir():
                    if item.name in exclude_patterns:
                        continue
                    if any(item.name.endswith(pat.strip("*")) for pat in exclude_patterns if pat.startswith("*")):
                        continue
                    tar.add(item, arcname=item.name)
            tar_buffer.seek(0)
        except (OSError, tarfile.TarError) as e:
            logger.error(f"Failed to create project archive: {e}")
            return False

        # Transfer and extract via SSH
        try:
            import shlex
            # Escape remote_dir to prevent shell injection
            safe_remote_dir = shlex.quote(remote_dir)
            # Create remote directory
            self._run_ssh(f"mkdir -p {safe_remote_dir}", timeout=30)

            # Transfer tarball and extract
            remote_addr = f"{self.config.user}@{self.config.host}"
            ssh_args = self._ssh_args() + [remote_addr, f"cd {safe_remote_dir} && tar xzf -"]

            proc = subprocess.run(
                ssh_args,
                stdin=tar_buffer,
                capture_output=True,
                timeout=120,
            )
            if proc.returncode != 0:
                logger.error(f"Remote extract failed: {proc.stderr.decode('utf-8', errors='replace')}")
                return False

            logger.info(f"Project synced to {remote_addr}:{remote_dir}")
            return True

        except (subprocess.TimeoutExpired, RemoteError) as e:
            logger.error(f"Project sync failed: {e}")
            return False

    def build_remote_command(
        self,
        local_command: List[str],
        local_project_dir: str,
    ) -> str:
        """Convert a local command to a remote shell command.

        Replaces local project paths with the remote project path.

        Args:
            local_command: The local command list.
            local_project_dir: The local project directory.

        Returns:
            A shell command string for remote execution.
        """
        remote_dir = self.config.remote_project
        local_resolved = str(Path(local_project_dir).expanduser().resolve())
        local_original = str(Path(local_project_dir).expanduser())

        parts = []
        for part in local_command:
            # Replace project directory references (try both resolved and original paths)
            replaced = part.replace(local_resolved, remote_dir)
            if local_original != local_resolved:
                replaced = replaced.replace(local_original, remote_dir)
            # Escape single quotes
            replaced = replaced.replace("'", "'\\''")
            parts.append(f"'{replaced}'" if any(c in replaced for c in " $&|;(){}[]<>!#") else replaced)

        return " ".join(parts)


def get_remote_config(config: Dict[str, Any]) -> Optional[RemoteConfig]:
    """Get remote config from hub.yaml config dict.

    Checks both top-level 'remote' section and agent-specific 'remote' section.
    """
    # Top-level remote config
    remote_data = config.get("remote", {})
    if remote_data:
        return RemoteConfig.from_dict(remote_data)
    return None


def get_agent_remote_config(config: Dict[str, Any], agent_name: str) -> Optional[RemoteConfig]:
    """Get remote config for a specific agent.

    Checks agent-specific 'remote' section first, then falls back to top-level.
    """
    agent_cfg = config.get("agents", {}).get(agent_name, {})
    remote_data = agent_cfg.get("remote", {})
    if remote_data:
        return RemoteConfig.from_dict(remote_data)
    # Fall back to top-level
    return get_remote_config(config)
