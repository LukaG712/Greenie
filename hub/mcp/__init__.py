"""MCP (Model Context Protocol) server implementation for Evershade.

Provides a bridge between Evershade services and external MCP-compatible tools.
Supports stdio, SSE, and HTTP transports.
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class TransportType(str, Enum):
    STDIO = "stdio"
    SSE = "sse"
    HTTP = "http"


@dataclass
class MCPTool:
    """A tool available via an MCP server."""
    name: str
    description: str
    inputSchema: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPResource:
    """A resource available via an MCP server."""
    uri: str
    name: str
    description: str = ""
    mimeType: str = "text/plain"


@dataclass
class MCPServer:
    """Configuration for an MCP server connection."""
    name: str
    url: str
    transport: TransportType = TransportType.SSE
    tools: List[MCPTool] = field(default_factory=list)
    resources: List[MCPResource] = field(default_factory=list)
    connected: bool = False
    connectedAt: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["transport"] = self.transport.value
        return result


class MCPClient:
    """Client for communicating with MCP servers."""

    def __init__(self, server: MCPServer):
        self.server = server
        self.client: Optional[httpx.AsyncClient] = None
        self._request_id = 0

    async def connect(self) -> bool:
        """Establish connection to the MCP server."""
        try:
            if self.server.transport == TransportType.HTTP:
                self.client = httpx.AsyncClient(
                    base_url=self.server.url,
                    timeout=30.0,
                    headers={"Content-Type": "application/json"},
                )
                # Test connection
                resp = await self.client.get("/health", timeout=5)
                self.server.connected = resp.status_code == 200
            elif self.server.transport == TransportType.SSE:
                self.client = httpx.AsyncClient(
                    base_url=self.server.url,
                    timeout=30.0,
                    headers={"Content-Type": "application/json"},
                )
                # Initialize MCP session
                await self._send_request("initialize", {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "evershade", "version": "1.0.0"},
                })
                self.server.connected = True
            else:
                # stdio — not supported via HTTP client
                logger.warning(f"stdio transport not supported for server {self.server.name}")
                return False

            self.server.connectedAt = datetime.now(timezone.utc).isoformat()
            logger.info(f"Connected to MCP server: {self.server.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MCP server {self.server.name}: {e}")
            self.server.connected = False
            return False

    async def disconnect(self):
        """Close connection to the MCP server."""
        if self.client:
            await self.client.aclose()
            self.client = None
        self.server.connected = False
        logger.info(f"Disconnected from MCP server: {self.server.name}")

    async def _send_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Send an MCP JSON-RPC request."""
        self._request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params,
        }

        if not self.client:
            raise RuntimeError("Not connected")

        resp = await self.client.post("/", json=request, timeout=15)
        resp.raise_for_status()
        return resp.json()

    async def list_tools(self) -> List[MCPTool]:
        """List available tools from the MCP server."""
        try:
            result = await self._send_request("tools/list", {})
            tools = []
            for tool in result.get("tools", []):
                tools.append(MCPTool(
                    name=tool["name"],
                    description=tool.get("description", ""),
                    inputSchema=tool.get("inputSchema", {}),
                ))
            self.server.tools = tools
            return tools
        except Exception as e:
            logger.error(f"Failed to list tools from {self.server.name}: {e}")
            return []

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call a tool on the MCP server."""
        try:
            result = await self._send_request("tools/call", {
                "name": name,
                "arguments": arguments,
            })
            return result
        except Exception as e:
            logger.error(f"Failed to call tool {name} on {self.server.name}: {e}")
            return {"error": str(e)}

    async def list_resources(self) -> List[MCPResource]:
        """List available resources from the MCP server."""
        try:
            result = await self._send_request("resources/list", {})
            resources = []
            for resource in result.get("resources", []):
                resources.append(MCPResource(
                    uri=resource["uri"],
                    name=resource.get("name", ""),
                    description=resource.get("description", ""),
                    mimeType=resource.get("mimeType", "text/plain"),
                ))
            self.server.resources = resources
            return resources
        except Exception as e:
            logger.error(f"Failed to list resources from {self.server.name}: {e}")
            return []

    async def read_resource(self, uri: str) -> str:
        """Read a resource from the MCP server."""
        try:
            result = await self._send_request("resources/read", {"uri": uri})
            contents = result.get("contents", [])
            return "\n".join(c.get("text", "") for c in contents)
        except Exception as e:
            logger.error(f"Failed to read resource {uri} from {self.server.name}: {e}")
            return ""


class MCPManager:
    """Manages multiple MCP server connections."""

    def __init__(self):
        self.servers: Dict[str, MCPServer] = {}
        self.clients: Dict[str, MCPClient] = {}
        self._config_file = os.path.join(
            os.getenv("GREENIE_DATA_DIR", "data"), "mcp_servers.json"
        )
        self._load_config()

    def _load_config(self):
        """Load MCP server configuration from file."""
        try:
            if os.path.exists(self._config_file):
                with open(self._config_file) as f:
                    config = json.load(f)
                for srv in config.get("servers", []):
                    server = MCPServer(
                        name=srv["name"],
                        url=srv["url"],
                        transport=TransportType(srv.get("transport", "sse")),
                        config=srv.get("config", {}),
                    )
                    self.servers[server.name] = server
                    self.clients[server.name] = MCPClient(server)
        except Exception as e:
            logger.error(f"Failed to load MCP config: {e}")

    def _save_config(self):
        """Save MCP server configuration to file."""
        try:
            os.makedirs(os.path.dirname(self._config_file), exist_ok=True)
            config = {
                "servers": [
                    {
                        "name": srv.name,
                        "url": srv.url,
                        "transport": srv.transport.value,
                        "config": srv.config,
                    }
                    for srv in self.servers.values()
                ]
            }
            with open(self._config_file, "w") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save MCP config: {e}")

    def add_server(self, name: str, url: str, transport: str = "sse", config: Dict[str, Any] = None) -> MCPServer:
        """Add a new MCP server."""
        server = MCPServer(
            name=name,
            url=url,
            transport=TransportType(transport),
            config=config or {},
        )
        self.servers[name] = server
        self.clients[name] = MCPClient(server)
        self._save_config()
        return server

    def remove_server(self, name: str) -> bool:
        """Remove an MCP server."""
        if name in self.servers:
            client = self.clients.pop(name)
            asyncio.create_task(client.disconnect())
            del self.servers[name]
            self._save_config()
            return True
        return False

    async def connect_all(self):
        """Connect to all configured MCP servers."""
        tasks = []
        for name, client in self.clients.items():
            tasks.append(client.connect())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def disconnect_all(self):
        """Disconnect from all MCP servers."""
        tasks = []
        for client in self.clients.values():
            tasks.append(client.disconnect())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_servers(self) -> List[Dict[str, Any]]:
        """Get all server configurations."""
        return [srv.to_dict() for srv in self.servers.values()]

    async def call_tool(self, server_name: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call a tool on a specific server."""
        client = self.clients.get(server_name)
        if not client:
            return {"error": f"Server {server_name} not found"}
        if not client.server.connected:
            await client.connect()
        return await client.call_tool(tool_name, arguments)

    async def list_all_tools(self) -> Dict[str, List[MCPTool]]:
        """List tools from all connected servers."""
        result = {}
        for name, client in self.clients.items():
            if client.server.connected:
                tools = await client.list_tools()
                result[name] = tools
        return result


# Global MCP manager instance
mcp_manager = MCPManager()
