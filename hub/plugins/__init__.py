"""Plugin registry for Evershade.

Manages installation, configuration, and lifecycle of plugins that extend
Evershade capabilities (tools, resources, prompts, integrations).
"""

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Plugin:
    """Represents an installed plugin."""
    id: str
    name: str
    description: str
    version: str
    plugin_type: str  # tool, resource, prompt, integration
    enabled: bool
    source: str
    installed_at: str
    config: Dict[str, Any] = field(default_factory=dict)
    entry_point: str = ""
    dependencies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PluginRegistry:
    """Manages the plugin registry."""

    def __init__(self):
        self.plugins: Dict[str, Plugin] = {}
        self._config_file = os.path.join(
            os.getenv("GREENIE_DATA_DIR", "data"), "plugins.json"
        )
        self._load_config()

    def _load_config(self):
        """Load plugin registry from file."""
        try:
            if os.path.exists(self._config_file):
                with open(self._config_file) as f:
                    config = json.load(f)
                for plugin_data in config.get("plugins", []):
                    plugin = Plugin(
                        id=plugin_data["id"],
                        name=plugin_data["name"],
                        description=plugin_data.get("description", ""),
                        version=plugin_data.get("version", "1.0.0"),
                        plugin_type=plugin_data.get("type", "tool"),
                        enabled=plugin_data.get("enabled", True),
                        source=plugin_data.get("source", ""),
                        installed_at=plugin_data.get("installed_at", datetime.now(timezone.utc).isoformat()),
                        config=plugin_data.get("config", {}),
                        entry_point=plugin_data.get("entry_point", ""),
                        dependencies=plugin_data.get("dependencies", []),
                    )
                    self.plugins[plugin.id] = plugin
        except Exception as e:
            logger.error(f"Failed to load plugin registry: {e}")

    def _save_config(self):
        """Save plugin registry to file."""
        try:
            os.makedirs(os.path.dirname(self._config_file), exist_ok=True)
            config = {
                "plugins": [
                    {
                        "id": p.id,
                        "name": p.name,
                        "description": p.description,
                        "version": p.version,
                        "type": p.plugin_type,
                        "enabled": p.enabled,
                        "source": p.source,
                        "installed_at": p.installed_at,
                        "config": p.config,
                        "entry_point": p.entry_point,
                        "dependencies": p.dependencies,
                    }
                    for p in self.plugins.values()
                ]
            }
            with open(self._config_file, "w") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save plugin registry: {e}")

    def install(self, name: str, source: str, plugin_type: str = "tool",
                description: str = "", version: str = "1.0.0",
                config: Dict[str, Any] = None) -> Plugin:
        """Install a new plugin."""
        plugin = Plugin(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            version=version,
            plugin_type=plugin_type,
            enabled=True,
            source=source,
            installed_at=datetime.now(timezone.utc).isoformat(),
            config=config or {},
        )
        self.plugins[plugin.id] = plugin
        self._save_config()
        logger.info(f"Installed plugin: {name} ({plugin.id})")
        return plugin

    def uninstall(self, plugin_id: str) -> bool:
        """Uninstall a plugin."""
        if plugin_id in self.plugins:
            name = self.plugins[plugin_id].name
            del self.plugins[plugin_id]
            self._save_config()
            logger.info(f"Uninstalled plugin: {name}")
            return True
        return False

    def enable(self, plugin_id: str) -> bool:
        """Enable a plugin."""
        if plugin_id in self.plugins:
            self.plugins[plugin_id].enabled = True
            self._save_config()
            return True
        return False

    def disable(self, plugin_id: str) -> bool:
        """Disable a plugin."""
        if plugin_id in self.plugins:
            self.plugins[plugin_id].enabled = False
            self._save_config()
            return True
        return False

    def update_config(self, plugin_id: str, config: Dict[str, Any]) -> bool:
        """Update plugin configuration."""
        if plugin_id in self.plugins:
            self.plugins[plugin_id].config.update(config)
            self._save_config()
            return True
        return False

    def get(self, plugin_id: str) -> Optional[Plugin]:
        """Get a plugin by ID."""
        return self.plugins.get(plugin_id)

    def list_all(self, enabled_only: bool = False) -> List[Plugin]:
        """List all plugins."""
        plugins = list(self.plugins.values())
        if enabled_only:
            plugins = [p for p in plugins if p.enabled]
        return plugins

    def list_by_type(self, plugin_type: str, enabled_only: bool = True) -> List[Plugin]:
        """List plugins by type."""
        plugins = [p for p in self.plugins.values() if p.plugin_type == plugin_type]
        if enabled_only:
            plugins = [p for p in plugins if p.enabled]
        return plugins


# Global plugin registry instance
plugin_registry = PluginRegistry()


def create_plugin_routes(app):
    """Create plugin-related routes (no-op; routes registered via hub.api.plugins)."""
    pass
