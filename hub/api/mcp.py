"""MCP server API routes for Greenie."""

import asyncio
import logging
from dataclasses import asdict
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from hub.mcp import mcp_manager

logger = logging.getLogger(__name__)

# Background MCP connection tasks — store references to prevent silent failures
_mcp_background_tasks: set = set()

router = APIRouter(prefix="/api/mcp", tags=["MCP"])


@router.get("/servers")
async def list_mcp_servers():
    """List all configured MCP servers."""
    servers = mcp_manager.get_servers()
    return {"servers": servers}


@router.post("/servers")
async def add_mcp_server(data: Dict[str, Any]):
    """Add a new MCP server."""
    name = data.get("name")
    url = data.get("url")
    if not name or not url:
        raise HTTPException(status_code=400, detail="name and url are required")
    transport = data.get("transport", "sse")
    config = data.get("config", {})
    server = mcp_manager.add_server(name, url, transport, config)
    # Try to connect — store task reference to prevent silent failures
    task = asyncio.create_task(mcp_manager.clients[name].connect())
    _mcp_background_tasks.add(task)
    task.add_done_callback(_mcp_background_tasks.discard)
    return {"server": server.to_dict()}


@router.delete("/servers/{name}")
async def remove_mcp_server(name: str):
    """Remove an MCP server."""
    if not mcp_manager.remove_server(name):
        raise HTTPException(status_code=404, detail=f"Server {name} not found")
    return {"message": f"Server {name} removed"}


@router.post("/servers/{name}/connect")
async def connect_server(name: str):
    """Connect to a specific MCP server."""
    client = mcp_manager.clients.get(name)
    if not client:
        raise HTTPException(status_code=404, detail=f"Server {name} not found")
    success = await client.connect()
    return {"connected": success, "server": client.server.to_dict()}


@router.post("/servers/{name}/disconnect")
async def disconnect_server(name: str):
    """Disconnect from a specific MCP server."""
    client = mcp_manager.clients.get(name)
    if not client:
        raise HTTPException(status_code=404, detail=f"Server {name} not found")
    await client.disconnect()
    return {"message": f"Disconnected from {name}"}


@router.get("/servers/{name}/tools")
async def list_server_tools(name: str):
    """List tools available on a specific MCP server."""
    client = mcp_manager.clients.get(name)
    if not client:
        raise HTTPException(status_code=404, detail=f"Server {name} not found")
    if not client.server.connected:
        await client.connect()
    tools = await client.list_tools()
    return {"tools": [asdict(t) if hasattr(t, 'to_dict') else t.__dict__ for t in tools]}


@router.post("/servers/{name}/tools/{tool_name}/call")
async def call_server_tool(name: str, tool_name: str, data: Dict[str, Any]):
    """Call a tool on a specific MCP server."""
    arguments = data.get("arguments", {})
    result = await mcp_manager.call_tool(name, tool_name, arguments)
    return result


@router.get("/tools")
async def list_all_tools():
    """List all tools from all connected MCP servers."""
    tools = await mcp_manager.list_all_tools()
    result = {}
    for server_name, tool_list in tools.items():
        result[server_name] = [t.__dict__ for t in tool_list]
    return {"tools": result}


def register(app):
    """Register MCP routes with the FastAPI app."""
    app.include_router(router)
